#!/usr/bin/env python
"""Extract data from PDFs into schema-aligned CSVs."""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd
import pdfplumber
import pypdfium2 as pdfium


MONTHS = {
    "january": "Jan",
    "february": "Feb",
    "march": "Mar",
    "april": "Apr",
    "may": "May",
    "june": "Jun",
    "july": "Jul",
    "august": "Aug",
    "september": "Sep",
    "october": "Oct",
    "november": "Nov",
    "december": "Dec",
}


def iter_pdfs(input_dir: Path) -> Iterable[Path]:
    return sorted(input_dir.rglob("*.pdf"))


def find_schema(schema_dir: Path, pdf_path: Path) -> Optional[Path]:
    try:
        folder = pdf_path.relative_to(schema_dir.parent / "PDFs").parts[0]
    except Exception:
        return None
    schema_path = schema_dir / f"{folder}.csv"
    return schema_path if schema_path.exists() else None


def load_schema_columns(schema_path: Path) -> List[str]:
    df = pd.read_csv(schema_path)
    return list(df.columns)


def parse_month_from_filename(pdf_path: Path) -> Optional[tuple[str, str]]:
    name = pdf_path.stem.replace("_", " ")
    for month_name, abbr in MONTHS.items():
        if month_name in name.lower():
            year = None
            for token in name.split():
                if token.isdigit() and len(token) == 4:
                    year = token
                    break
            if year:
                return abbr, year
    return None


def clean_manufacturer(name: str) -> str:
    name = name.strip()
    name = name.replace("  ", " ")
    name = name.rstrip(" ")
    name = name.rstrip("1").rstrip("2").rstrip("3").rstrip("4").rstrip("5")
    name = name.rstrip()
    return name


def is_footnote_line(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return False

    lower = stripped.lower()
    if lower.startswith("www.acea.auto") or "page" in lower and "of" in lower:
        return True

    footnote_prefixes = (
        "1 ",
        "2 ",
        "3 ",
        "4 ",
        "5 ",
        "6 ",
        "7 ",
        "8 ",
        "9 ",
    )
    if stripped.startswith(footnote_prefixes):
        footnote_markers = (
            "acea estimation",
            "includes",
            "dodge",
            "maserati",
            "ram",
            "bentley",
            "bugatti",
            "lamborghini",
            "man",
        )
        if any(marker in lower for marker in footnote_markers):
            return True

    return False


def _parse_acea_lines(lines: List[str], month_2025: str, month_2024: str, pdf_month: str) -> List[dict]:
    records: List[dict] = []
    region: Optional[str] = None
    in_section = False
    number_pattern = r"[+-]?\d+(?:,\d{3})*(?:\.\d+)?"
    month_prefixes = tuple(month.upper() for month in MONTHS)

    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue
        if is_footnote_line(line):
            continue

        upper = line.upper()
        if "EU + EFTA + UK" in upper:
            region = "EU + EFTA + UK"
            continue
        if "EUROPEAN UNION" in upper:
            region = "European Union (EU)"
            continue
        if upper.startswith("EFTA"):
            region = "EFTA"
            continue

        if "NEW CAR REGISTRATIONS BY MANUFACTURER" in upper:
            in_section = True
            continue
        if not in_section:
            continue

        if upper.startswith("% SHARE") or upper.startswith("% CHANGE") or upper.startswith("2025 2024"):
            continue
        if upper.startswith(month_prefixes):
            continue
        if upper in {"(EU)", "EU", "M"}:
            continue

        first_match = re.search(number_pattern, line)
        if not first_match:
            continue
        name = line[: first_match.start()].strip()
        if not name:
            continue

        remainder = line[first_match.start() :]
        if first_match.start() > 0 and line[first_match.start() - 1].isalpha():
            remainder = line[first_match.end() :]
        tokens = re.findall(number_pattern, remainder)
        if len(tokens) < 9:
            continue

        manufacturer = clean_manufacturer(name)
        if manufacturer == "Others":
            continue

        monthly_2025 = tokens[2]
        monthly_2024 = tokens[3]
        ytd_2025 = tokens[7]
        ytd_2024 = tokens[8]

        current_region = region or "European Union (EU)"
        records.extend(
            [
                {
                    "Manufacturer": manufacturer,
                    "Frequency": "M",
                    "Month": month_2025,
                    "Units": monthly_2025,
                    "Region": current_region,
                    "PDF": pdf_month,
                },
                {
                    "Manufacturer": manufacturer,
                    "Frequency": "M",
                    "Month": month_2024,
                    "Units": monthly_2024,
                    "Region": current_region,
                    "PDF": pdf_month,
                },
                {
                    "Manufacturer": manufacturer,
                    "Frequency": "YTD",
                    "Month": month_2025,
                    "Units": ytd_2025,
                    "Region": current_region,
                    "PDF": pdf_month,
                },
                {
                    "Manufacturer": manufacturer,
                    "Frequency": "YTD",
                    "Month": month_2024,
                    "Units": ytd_2024,
                    "Region": current_region,
                    "PDF": pdf_month,
                },
            ]
        )

    return records


def parse_acea_pdf(pdf_path: Path) -> pd.DataFrame:
    expected_columns = ["Manufacturer", "Frequency", "Month", "Units", "Region", "PDF"]
    month_info = parse_month_from_filename(pdf_path)
    if not month_info:
        raise ValueError(f"Unable to parse month/year from filename: {pdf_path.name}")
    month_abbr, year_str = month_info
    year = int(year_str)
    month_2025 = f"{month_abbr}-{str(year)[-2:]}"
    month_2024 = f"{month_abbr}-{str(year - 1)[-2:]}"
    pdf_month = month_2025

    records: List[dict] = []

    # Primary parse: pdfplumber
    lines: List[str] = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            lines.extend(text.splitlines())

    records = _parse_acea_lines(lines, month_2025, month_2024, pdf_month)

    # Fallback parse: pypdfium2 text engine for fragmented PDFs
    if not records:
        pdfium_lines: List[str] = []
        doc = pdfium.PdfDocument(str(pdf_path))
        for page_index in range(len(doc)):
            page = doc.get_page(page_index)
            textpage = page.get_textpage()
            text = textpage.get_text_range() or ""
            pdfium_lines.extend(text.splitlines())
            page.close()
        records = _parse_acea_lines(pdfium_lines, month_2025, month_2024, pdf_month)

    return pd.DataFrame(records, columns=expected_columns)


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract data from PDFs into schema-aligned CSV files.")
    parser.add_argument(
        "--input",
        default=str(Path("PDFs")),
        help="Directory containing PDFs (default: PDFs)",
    )
    parser.add_argument(
        "--output",
        default=str(Path("data")),
        help="Output directory for CSVs (default: data)",
    )
    args = parser.parse_args()

    input_dir = Path(args.input).resolve()
    output_dir = Path(args.output).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    pdfs = list(iter_pdfs(input_dir))
    if not pdfs:
        raise SystemExit(f"No PDFs found in {input_dir}")

    all_records: List[dict] = []
    for pdf_path in pdfs:
        schema_path = find_schema(schema_dir=Path("schema").resolve(), pdf_path=pdf_path)
        if not schema_path:
            raise SystemExit(f"No schema found for {pdf_path}")
        columns = load_schema_columns(schema_path)

        if schema_path.stem.upper() == "ACEA":
            df = parse_acea_pdf(pdf_path)
        else:
            raise SystemExit(f"No parser available for schema {schema_path.stem}")

        df = df.reindex(columns=columns)
        out_name = f"{pdf_path.stem}_schema.csv"
        out_path = output_dir / out_name
        df.to_csv(out_path, index=False)

        all_records.append(
            {
                "pdf_file": pdf_path.name,
                "output_csv": out_path.name,
                "rows": int(df.shape[0]),
                "cols": int(df.shape[1]),
            }
        )

    index_path = output_dir / "_index.csv"
    pd.DataFrame(all_records).to_csv(index_path, index=False)

    print(f"Processed {len(pdfs)} PDFs. Index written to {index_path}")

if __name__ == "__main__":
    main()