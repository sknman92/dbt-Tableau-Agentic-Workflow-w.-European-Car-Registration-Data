"""
LangChain structured extraction chain for ACEA PDF data.

Instead of regex, we pass raw PDF text to Claude and ask it to return
structured rows that match our Pydantic schema. The existing regex logic
in pdf_scrape.py remains as the fallback if LLM extraction fails.
"""

from __future__ import annotations

from typing import List, Literal, Optional

import dotenv
from langchain_anthropic import ChatAnthropic
from langchain_core.prompts import ChatPromptTemplate
from pydantic import BaseModel, Field

dotenv.load_dotenv()


# ---------------------------------------------------------------------------
# Step 1 — Pydantic schema
#
# BaseModel is the Pydantic base class. Each field has:
#   - a Python type (str, int, Literal["M","YTD"])
#   - a Field(description=...) that the LLM reads to know what to extract
#
# ACEARow: one data row (manufacturer + metric for one period)
# ACEAExtraction: wrapper that holds a list of rows — this is what we ask
#                 the LLM to return as a single structured object
# ---------------------------------------------------------------------------

class ACEARow(BaseModel):
    manufacturer: str = Field(
        description="Car manufacturer name, e.g. 'BMW', 'Volkswagen', 'Stellantis'"
    )
    frequency: Literal["M", "YTD"] = Field(
        description="M for a single monthly figure, YTD for year-to-date cumulative"
    )
    month: str = Field(
        description="Month-year label in Mon-YY format, e.g. 'Jan-25', 'Mar-24'"
    )
    units: int = Field(
        description="Number of vehicle registrations as a plain integer (no commas)"
    )
    region: str = Field(
        description=(
            "Geographic region the row belongs to. One of: "
            "'European Union (EU)', 'EFTA', 'EU + EFTA + UK'"
        )
    )
    pdf_month: str = Field(
        description="Source PDF identifier — same Mon-YY label as the current month"
    )


class ACEAExtraction(BaseModel):
    """Container returned by the LLM — a list of all extracted ACEARow objects."""

    rows: List[ACEARow] = Field(
        description="Every manufacturer registration row extracted from the table"
    )


# ---------------------------------------------------------------------------
# Step 2 — Extraction chain
#
# ChatAnthropic is the LLM. .with_structured_output(ACEAExtraction) tells
# LangChain to force the model to return JSON that matches our schema —
# no manual parsing needed.
#
# ChatPromptTemplate.from_messages builds the prompt. The system message
# explains the task; the user message is the raw PDF text.
#
# prompt | structured_llm is a LangChain "pipe" — it chains the two steps:
#   1. format the prompt with the input variables
#   2. pass it to the LLM and parse the structured output
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """\
You are extracting car registration data from ACEA press release PDF tables.

Find the section titled "NEW CAR REGISTRATIONS BY MANUFACTURER" and extract \
every manufacturer row from it. Skip rows labelled "Others".

The table repeats for three regions in this order:
  1. EU + EFTA + UK
  2. European Union (EU)
  3. EFTA

For EACH manufacturer in EACH region, produce FOUR rows:
  - frequency=M,   month={current_month}  (monthly registrations, current year)
  - frequency=M,   month={prior_month}    (monthly registrations, prior year)
  - frequency=YTD, month={current_month}  (year-to-date, current year)
  - frequency=YTD, month={prior_month}    (year-to-date, prior year)

Set pdf_month="{pdf_month}" on every row.
Return units as plain integers — strip commas and any footnote markers (1,2,3...).
"""


def get_extraction_chain():
    """
    Returns a stateless, reusable runnable chain:  dict  →  ACEAExtraction

    The chain has no baked-in values — all substitution happens at invoke time.
    Required keys when calling .invoke():
        text          (str)  raw PDF text (or the manufacturer table section)
        current_month (str)  Mon-YY label for the current reporting month, e.g. 'Jan-25'
        prior_month   (str)  Mon-YY label for the prior year month, e.g. 'Jan-24'
        pdf_month     (str)  source PDF identifier (same value as current_month)
    """
    # Haiku is fast and cheap for structured extraction tasks like this.
    # Swap to claude-sonnet-4-6 if you need higher accuracy on messy PDFs.
    llm = ChatAnthropic(model="claude-haiku-4-5-20251001", temperature=0)
    structured_llm = llm.with_structured_output(ACEAExtraction)

    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", _SYSTEM_PROMPT),
            ("user", "{text}"),
        ]
    )

    return prompt | structured_llm


# ---------------------------------------------------------------------------
# Step 3 — Section extractor
#
# ACEA PDFs are long. We don't need to send every page to the LLM —
# we only need the "NEW CAR REGISTRATIONS BY MANUFACTURER" section.
# This helper slices out just those lines, cutting token usage significantly.
# ---------------------------------------------------------------------------

def _extract_table_section(lines: List[str]) -> str:
    """
    Return only the lines from 'NEW CAR REGISTRATIONS BY MANUFACTURER'
    onwards, discarding headers, press release text, and footnotes.
    Falls back to returning all lines if the marker isn't found.
    """
    start = None
    for i, line in enumerate(lines):
        if "NEW CAR REGISTRATIONS BY MANUFACTURER" in line.upper():
            start = i
            break

    if start is None:
        return "\n".join(lines)  # fallback: send everything

    return "\n".join(lines[start:])


# ---------------------------------------------------------------------------
# Step 4 — Public entry point
#
# Called from parse_acea_pdf() in pdf_scrape.py as the primary extractor.
# Returns a list of plain dicts (matching the existing records format) so
# the rest of pdf_scrape.py needs no changes.
# ---------------------------------------------------------------------------

def extract_rows_with_llm(
    lines: List[str],
    current_month: str,
    prior_month: str,
    pdf_month: str,
) -> Optional[List[dict]]:
    """
    Extract ACEA rows from raw PDF text lines using the LLM extraction chain.

    Args:
        lines:         All text lines extracted from the PDF.
        current_month: Mon-YY label for the current reporting month (e.g. 'Jan-25').
        prior_month:   Mon-YY label for the prior year month (e.g. 'Jan-24').
        pdf_month:     Source PDF identifier (same as current_month).

    Returns:
        List of record dicts with keys matching parse_acea_pdf's expected_columns,
        or None if extraction fails (so the caller can fall back to regex).
    """
    section_text = _extract_table_section(lines)
    if not section_text.strip():
        return None

    chain = get_extraction_chain()
    result: ACEAExtraction = chain.invoke(
        {
            "text": section_text,
            "current_month": current_month,
            "prior_month": prior_month,
            "pdf_month": pdf_month,
        }
    )

    if not result.rows:
        return None

    # Convert from snake_case Pydantic fields → Title case dict keys
    # that match the existing DataFrame columns in pdf_scrape.py
    return [
        {
            "Manufacturer": row.manufacturer,
            "Frequency": row.frequency,
            "Month": row.month,
            "Units": row.units,
            "Region": row.region,
            "PDF": row.pdf_month,
        }
        for row in result.rows
    ]
