"""
LangChain agentic orchestrator for the ACEA pipeline.

The agent wraps each execution script as a structured @tool and uses
Claude (via langchain-anthropic) + LangGraph's ReAct loop to decide
which tools to call and in what order, based on natural-language prompts.

Usage:
    python -m execution.agent "Run the full ACEA pipeline end to end."
    python -m execution.agent "Upload to Snowflake and rebuild dbt."
"""

from __future__ import annotations

import asyncio
import subprocess
import sys
from typing import Optional

import dotenv
from langchain_anthropic import ChatAnthropic
from langchain_core.tools import tool
from langgraph.prebuilt import create_react_agent

dotenv.load_dotenv()


# ---------------------------------------------------------------------------
# Tools — each wraps one execution script / pipeline step
# ---------------------------------------------------------------------------

@tool
def run_webscrape(date_range_start: str = "2025-01-01", date_range_end: str = "2025-12-31") -> str:
    """
    Download ACEA car-registration PDFs from the ACEA website.

    Args:
        date_range_start: Start of date range in YYYY-MM-DD format (default 2024-01-01).
        date_range_end:   End of date range in YYYY-MM-DD format (default 2025-12-31).

    Returns:
        A status message indicating how many PDFs were downloaded.
    """
    from execution.webscrape import web_scrape_acea
    try:
        web_scrape_acea(date_range=[date_range_start, date_range_end])
        return f"webscrape complete for {date_range_start} → {date_range_end}."
    except Exception as e:
        return f"webscrape FAILED: {e}"


@tool
def run_pdf_scrape(input_dir: str = "PDFs", output_dir: str = "data") -> str:
    """
    Parse downloaded PDFs and write schema-aligned CSVs to the output directory.

    Args:
        input_dir:  Directory containing raw PDFs (default: PDFs).
        output_dir: Directory to write parsed CSVs into (default: data).

    Returns:
        stdout/stderr from the pdf_scrape script.
    """
    result = subprocess.run(
        [sys.executable, "-m", "execution.pdf_scrape", "--input", input_dir, "--output", output_dir],
        capture_output=True,
        text=True,
    )
    output = result.stdout + result.stderr
    if result.returncode != 0:
        return f"pdf_scrape FAILED (exit {result.returncode}):\n{output}"
    return f"pdf_scrape complete.\n{output}"


@tool
def run_upload_snowflake() -> str:
    """
    Upload all parsed CSVs from the data/ directory to Snowflake.

    Returns:
        A status message confirming the upload or describing the error.
    """
    from execution.upload_snowflake import upload_to_snowflake
    try:
        upload_to_snowflake()
        return "Snowflake upload complete."
    except Exception as e:
        return f"Snowflake upload FAILED: {e}"


@tool
def run_dbt_build(select: Optional[str] = None) -> str:
    """
    Run `dbt build` inside the dbt/ project directory.

    Args:
        select: Optional dbt node selector (e.g. 'marts_acea_metrics').
                If omitted, builds all models.

    Returns:
        stdout/stderr from dbt.
    """
    cmd = [".venv/Scripts/dbt.exe", "build"]
    if select:
        cmd += ["-s", select]

    result = subprocess.run(cmd, capture_output=True, text=True, cwd="dbt")
    output = result.stdout + result.stderr
    if result.returncode != 0:
        return f"dbt build FAILED (exit {result.returncode}):\n{output}"
    return f"dbt build complete.\n{output}"


@tool
def run_publish_tableau(project_name: str = "Charles") -> str:
    """
    Build a Tableau Hyper extract from Snowflake and publish it to Tableau Server.

    Args:
        project_name: Name of the Tableau project to publish the datasource into.

    Returns:
        A status message confirming publish or describing the error.
    """
    from execution.publish_tableau_datasource import create_hyper, upload_to_tableau
    try:
        create_hyper()
        upload_to_tableau(project_name)
        return f"Tableau datasource published to project '{project_name}'."
    except Exception as e:
        return f"Tableau publish FAILED: {e}"


@tool
def search_docs(query: str) -> str:
    """
    Search the ACEA vector store for relevant data using a natural language query.
    Use this to answer questions about car registrations, manufacturers, or regions
    without running the full pipeline.

    Args:
        query: Natural language question, e.g. 'BMW registrations EU Jan 2025'

    Returns:
        Claude's answer grounded in the retrieved PDF chunks.
    """
    try:
        from execution.rag_chain import ask
        return ask(query)
    except Exception as e:
        return f"search_docs FAILED: {e}"


@tool
def verify_csvs(data_dir: str = "data") -> str:
    """
    Preview all CSVs in the data directory so the user can verify them before upload.
    Shows the first 5 rows of each file.

    Args:
        data_dir: Directory containing intermediate CSVs (default: data).

    Returns:
        A formatted preview of each CSV file found.
    """
    import os
    import pandas as pd

    files = [f for f in os.listdir(data_dir) if f.endswith(".csv")]
    if not files:
        return f"No CSV files found in {data_dir}/"

    parts = []
    for f in sorted(files):
        path = os.path.join(data_dir, f)
        df = pd.read_csv(path)
        parts.append(
            f"--- {f} ({len(df)} rows x {len(df.columns)} cols) ---\n"
            f"{df.head(5).to_string(index=False)}"
        )
    return "\n\n".join(parts)


@tool
def create_analysis_folder(title: str) -> str:
    """
    Create a uniquely named folder under analyses/<today>/<title>/ for a triage run.
    Adds a numeric suffix (_2, _3, ...) if the folder already exists.

    Args:
        title: Short descriptive title for the triage request (snake_case preferred).

    Returns:
        The path of the created folder.
    """
    import os
    from datetime import date

    base = os.path.join("analyses", str(date.today()), title)
    path = base
    i = 2
    while os.path.exists(path):
        path = f"{base}_{i}"
        i += 1
    os.makedirs(path)
    return path


@tool
def save_query_csv(rows_json: str, folder: str, filename: str = "query_result.csv") -> str:
    """
    Save query result rows (JSON string of a list of dicts) as a CSV file.

    Args:
        rows_json: JSON string — the 'data' array returned by query-datasource.
        folder:    Destination folder path (from create_analysis_folder).
        filename:  Output filename (default: query_result.csv).

    Returns:
        Confirmation with row count and file path.
    """
    import json
    import os
    import pandas as pd

    rows = json.loads(rows_json)
    df = pd.DataFrame(rows)
    path = os.path.join(folder, filename)
    df.to_csv(path, index=False)
    return f"Saved {len(df)} rows to {path}"


@tool
def generate_plot(
    csv_path: str,
    plot_type: str,
    x_col: str,
    y_col: str,
    color_col: str = "",
    title: str = "",
    output_path: str = "",
) -> str:
    """
    Generate a matplotlib/seaborn plot from a CSV and save as PNG.
    Follows the data-viz-plots skill conventions (300 DPI, tight layout).

    Args:
        csv_path:    Path to the source CSV file.
        plot_type:   One of 'bar', 'line', 'scatter'.
        x_col:       Column name for the x-axis.
        y_col:       Column name for the y-axis.
        color_col:   Optional column to split series by color.
        title:       Chart title (auto-generated if omitted).
        output_path: PNG save path (defaults to same folder as CSV).

    Returns:
        Path of the saved PNG file.
    """
    import os
    import pandas as pd
    import matplotlib.pyplot as plt
    import seaborn as sns

    df = pd.read_csv(csv_path)
    sns.set_style("whitegrid")
    plt.rcParams["figure.dpi"] = 150
    plt.rcParams["savefig.dpi"] = 300
    plt.rcParams["font.size"] = 10

    fig, ax = plt.subplots(figsize=(10, 6))
    groups = df[color_col].unique() if color_col and color_col in df.columns else [None]

    for g in groups:
        subset = df[df[color_col] == g] if g is not None else df
        label = str(g) if g is not None else None
        if plot_type == "bar":
            ax.bar(subset[x_col], subset[y_col], label=label, alpha=0.8)
        elif plot_type == "line":
            ax.plot(subset[x_col], subset[y_col], marker="o", label=label, linewidth=2)
        elif plot_type == "scatter":
            ax.scatter(subset[x_col], subset[y_col], label=label, alpha=0.7)

    if color_col:
        ax.legend(frameon=True, loc="best")
    ax.set_xlabel(x_col, fontsize=12)
    ax.set_ylabel(y_col, fontsize=12)
    ax.set_title(title or f"{y_col} by {x_col}", fontsize=14, fontweight="bold")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()

    if not output_path:
        stem = os.path.splitext(csv_path)[0]
        output_path = f"{stem}_{plot_type}.png"

    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    return output_path


@tool
def save_run_metadata(
    folder: str,
    datasource_luid: str,
    query_summary: str,
    filters: str = "",
    notes: str = "",
) -> str:
    """
    Save a JSON metadata file describing the triage run.

    Args:
        folder:           Analysis folder path (from create_analysis_folder).
        datasource_luid:  LUID of the Tableau datasource queried.
        query_summary:    Short description of what was queried.
        filters:          Any filters applied (optional).
        notes:            Additional notes (optional).

    Returns:
        Path of the saved metadata file.
    """
    import json
    import os
    from datetime import datetime

    metadata = {
        "timestamp": datetime.now().isoformat(),
        "datasource_luid": datasource_luid,
        "query_summary": query_summary,
        "filters": filters,
        "notes": notes,
    }
    path = os.path.join(folder, "run_metadata.json")
    with open(path, "w") as f:
        json.dump(metadata, f, indent=2)
    return path


# ---------------------------------------------------------------------------
# Agent
# ---------------------------------------------------------------------------

PIPELINE_TOOLS = [
    run_webscrape,
    run_pdf_scrape,
    run_upload_snowflake,
    run_dbt_build,
    run_publish_tableau,
    search_docs,
    verify_csvs,
    create_analysis_folder,
    save_query_csv,
    generate_plot,
    save_run_metadata,
]

SYSTEM_PROMPT = """You are the orchestration layer of the ACEA automotive data pipeline.

## Pipeline steps (run in order unless the user specifies otherwise)

  1. run_webscrape        — download PDFs from the ACEA website
  2. run_pdf_scrape       — parse PDFs into schema-aligned CSVs (output → data/)
  3. verify_csvs          — preview data/ CSVs; STOP and wait for user confirmation before continuing
  4. run_upload_snowflake — upload verified CSVs to Snowflake; delete data/ files after upload
  5. run_dbt_build        — transform data with dbt
  6. run_publish_tableau  — export marts/ models to Tableau

## Data triage (LUID: 72aa7c33-63e2-45cc-aa6d-15ed24e91cb6)

Datasource fields — use these EXACT names in query-datasource:
  MANUFACTURER  — dimension (string)
  REGION        — dimension (string)
  DATE          — dimension (datetime)
  FREQUENCY     — dimension (string)
  Measure       — dimension (string)
  Value         — measure   (SUM / AVG / COUNT / etc.)

For any data question or chart request, follow this sequence:
  a. query-datasource         — run a structured VizQL query using the exact field names above
  c. create_analysis_folder   — create analyses/<today>/<title>/ (snake_case title, no timestamp in date)
  d. save_query_csv           — save query rows as CSV in the run folder
  e. generate_plot            — generate a matplotlib PNG from the CSV (bar/line/scatter)
  f. save_run_metadata        — save datasource, filters, and timestamp as run_metadata.json

All triage output (CSV, PNG, metadata) must be saved in the same run folder. Never save to temp dirs.
If the folder already exists, create_analysis_folder will auto-append a numeric suffix.

## Other tools
  - search_docs  — vector search over ACEA PDFs for questions about raw source data

## Rules
- Only run the steps the user asks for. "Full pipeline" means steps 1–6 in order.
- After verify_csvs (step 3), always stop and wait for explicit user approval before uploading.
- If any step fails, stop and report the error clearly. Do not skip ahead.
- Be concise. Summarise tool output — do not repeat it verbatim.
"""


def build_agent(mcp_tools: list) -> object:
    llm = ChatAnthropic(model="claude-sonnet-4-6", temperature=0)
    return create_react_agent(llm, PIPELINE_TOOLS + mcp_tools, prompt=SYSTEM_PROMPT)


async def _run_async(prompt: str) -> None:
    from execution.mcp_utils import with_tableau_tools
    from langchain_core.tools import BaseTool

    async def _run_with_tools(mcp_tools: list[BaseTool]) -> None:
        agent = build_agent(mcp_tools)
        print(f"\n[agent] prompt: {prompt}\n{'-' * 60}")
        try:
            async for chunk in agent.astream({"messages": [("user", prompt)]}):
                if "agent" in chunk:
                    for msg in chunk["agent"]["messages"]:
                        print(msg.content)
                elif "tools" in chunk:
                    for msg in chunk["tools"]["messages"]:
                        content = msg.content if isinstance(msg.content, str) else str(msg.content)
                        print(f"[tool: {msg.name}] {content[:2000]}")
        except Exception as e:
            print(f"[error] {type(e).__name__}: {e}")
        print("-" * 60)

    await with_tableau_tools(_run_with_tools)


def run(prompt: str) -> None:
    asyncio.run(_run_async(prompt))


if __name__ == "__main__":
    prompt = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "What steps are available in this pipeline?"
    run(prompt)
