"""
VizQL Data Service chain for ACEA Tableau queries.

Uses the Tableau VizQL Data Service REST API to:
  1. Resolve the published datasource LUID from Tableau Server
  2. Read field metadata so we know available field captions
  3. Translate a natural-language query into a structured VizQL query (via Claude Haiku)
  4. Execute the query and return formatted results

Usage:
    python -m execution.vizql_chain "Top 5 manufacturers by total registrations"
"""

from __future__ import annotations

import json
import os
import sys

import dotenv
import requests
import tableauserverclient as TSC
from langchain_anthropic import ChatAnthropic
from langchain_core.prompts import ChatPromptTemplate

dotenv.load_dotenv()

DATASOURCE_NAME = "marts_acea_metrics"

# ---------------------------------------------------------------------------
# Step 1 — Auth
# ---------------------------------------------------------------------------

def _sign_in() -> tuple[TSC.Server, str, str]:
    """
    Sign in to Tableau Server using username/password from env.

    Returns:
        (TSC.Server instance, auth_token, server_url)
    """
    server_url = os.getenv("tableau_server")
    auth = TSC.TableauAuth(
        os.getenv("tableau_user"),
        os.getenv("tableau_password"),
        site_id=os.getenv("tableau_site", ""),
    )
    server = TSC.Server(server_url, use_server_version=True)
    server.auth.sign_in(auth)
    return server, server.auth_token, server_url


# ---------------------------------------------------------------------------
# Step 2 — Resolve datasource LUID
# ---------------------------------------------------------------------------

def _get_datasource_luid(server: TSC.Server) -> str:
    """Find the published MARTS_ACEA_METRICS datasource LUID by name."""
    req = TSC.RequestOptions()
    req.filter.add(
        TSC.Filter(
            TSC.RequestOptions.Field.Name,
            TSC.RequestOptions.Operator.Equals,
            DATASOURCE_NAME,
        )
    )
    datasources, _ = server.datasources.get(req)
    if not datasources:
        raise ValueError(
            f"Datasource '{DATASOURCE_NAME}' not found on Tableau Server. "
            "Run the pipeline to publish it first."
        )
    return datasources[0].id


# ---------------------------------------------------------------------------
# Step 3 — Read field metadata from VizQL Data Service
# ---------------------------------------------------------------------------

def _read_metadata(server_url: str, token: str, luid: str) -> list[dict]:
    """
    Call POST /api/v1/vizql-data-service/read-metadata.

    Returns the list of field metadata dicts (fieldCaption, dataType,
    defaultAggregation, columnClass, ...).
    """
    url = f"{server_url}/api/v1/vizql-data-service/read-metadata"
    resp = requests.post(
        url,
        json={"datasource": {"datasourceLuid": luid}},
        headers={"X-Tableau-Auth": token},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json().get("data", [])


# ---------------------------------------------------------------------------
# Step 4 — NL → VizQL query (Claude Haiku)
# ---------------------------------------------------------------------------

_NL_TO_VIZQL_PROMPT = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            """You are a VizQL query builder for the ACEA automotive registration dataset.

Given a natural-language question and the list of available datasource fields below,
output a JSON object with the following structure:

{{
  "fields": [
    {{"fieldCaption": "<exact caption>"}},                          // dimension
    {{"fieldCaption": "<exact caption>", "function": "<AGG>"}}      // measure
  ],
  "filters": []   // optional — include only if the question requires filtering
}}

Aggregation functions: SUM, AVG, COUNT, COUNTD, MIN, MAX, MEDIAN.
Dimension fields (strings, dates) must NOT have a "function" key.
Measure fields (numbers) MUST have a "function" key.
Field captions must match EXACTLY one of the values listed below.

Available fields:
{field_list}

Output ONLY the raw JSON object. No markdown, no explanation.""",
        ),
        ("user", "{question}"),
    ]
)


def _build_vizql_query(nl_query: str, fields_metadata: list[dict]) -> dict:
    """Use Claude Haiku to translate a natural-language query into a VizQL query dict."""
    field_list = "\n".join(
        f"  fieldCaption={f.get('fieldCaption', '?')!r:40s}  "
        f"dataType={f.get('dataType', '?'):10s}  "
        f"defaultAggregation={f.get('defaultAggregation', 'NONE')}"
        for f in fields_metadata
        if f.get("fieldCaption")
    )

    llm = ChatAnthropic(model="claude-haiku-4-5-20251001", temperature=0)
    chain = _NL_TO_VIZQL_PROMPT | llm
    response = chain.invoke({"question": nl_query, "field_list": field_list})
    text = response.content if hasattr(response, "content") else str(response)

    # Strip markdown fences if the model wraps the JSON
    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()

    return json.loads(text)


# ---------------------------------------------------------------------------
# Step 5 — Execute the query
# ---------------------------------------------------------------------------

def _query_datasource(server_url: str, token: str, luid: str, query: dict) -> list[dict]:
    """
    Call POST /api/v1/vizql-data-service/query-datasource.

    Returns the list of row dicts from the response.
    """
    url = f"{server_url}/api/v1/vizql-data-service/query-datasource"
    payload = {
        "datasource": {"datasourceLuid": luid},
        "query": query,
        "options": {
            "returnFormat": "OBJECTS",
            "disaggregate": False,
        },
    }
    resp = requests.post(
        url,
        json=payload,
        headers={"X-Tableau-Auth": token},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json().get("data", [])


# ---------------------------------------------------------------------------
# Step 6 — Format results
# ---------------------------------------------------------------------------

def _format_rows(rows: list[dict], max_rows: int = 50) -> str:
    if not rows:
        return "Query returned no data."
    headers = list(rows[0].keys())
    col_widths = [max(len(h), max(len(str(r.get(h, ""))) for r in rows[:max_rows])) for h in headers]
    sep = " | "

    def fmt_row(row):
        return sep.join(str(row.get(h, "")).ljust(w) for h, w in zip(headers, col_widths))

    lines = [sep.join(h.ljust(w) for h, w in zip(headers, col_widths))]
    lines.append("-" * len(lines[0]))
    lines.extend(fmt_row(r) for r in rows[:max_rows])
    if len(rows) > max_rows:
        lines.append(f"... ({len(rows) - max_rows} more rows not shown)")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def query_vizql(nl_query: str) -> str:
    """
    Translate *nl_query* into a VizQL structured query, execute it against the
    published Tableau datasource, and return a formatted table string.
    """
    server, token, server_url = _sign_in()
    try:
        luid = _get_datasource_luid(server)
        fields_metadata = _read_metadata(server_url, token, luid)
        vizql_query = _build_vizql_query(nl_query, fields_metadata)
        rows = _query_datasource(server_url, token, luid, vizql_query)
        return _format_rows(rows)
    finally:
        server.auth.sign_out()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    question = (
        " ".join(sys.argv[1:])
        if len(sys.argv) > 1
        else "Top 5 manufacturers by total registrations"
    )
    print(f"\nQ: {question}\n{'-' * 60}")
    print(query_vizql(question))
    print("-" * 60)
