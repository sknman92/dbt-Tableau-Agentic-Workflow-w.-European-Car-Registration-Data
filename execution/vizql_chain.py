"""
VizQL Data Service chain for ACEA Tableau queries.

Uses the Tableau VizQL Data Service REST API to:
  1. Resolve the published datasource LUID from Tableau Server
  2. Read field metadata so we know available field captions
  3. Translate a natural-language query into a structured VizQL query (via Claude Haiku)
  4. Execute the query and return formatted results

Usage:
    python -m execution.vizql_chain "Top 5 manufacturers by total registrations"


Before executing VizQL data service via Tableau MCP to answer user prompts about
the data, use this function to return the dimensions and their distinct values.
"""

from __future__ import annotations

import os

import dotenv
import requests
import tableauserverclient as TSC
from langchain_anthropic import ChatAnthropic
from langchain_core.prompts import ChatPromptTemplate

dotenv.load_dotenv()

DATASOURCE_NAME = "marts_acea"

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

def _read_dimension_values(server_url: str, token: str, luid: str, field_caption: str = 'REGION')\
    -> list:
    """Return distinct values for a single dimension field"""

    query = {"fields": [{"fieldCaption": field_caption}]}

    rows = _query_datasource(server_url, token, luid, query)

    distinct_values = [row.get(field_caption) for row in rows]

    return distinct_values

# ---------------------------------------------------------------------------
# Step 4 — Retrieve JSON schema for tool call
# ---------------------------------------------------------------------------

import asyncio
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from langchain_mcp_adapters.tools import load_mcp_tools

_TABLEAU_MCP_PARAMS = StdioServerParameters(
    command="powershell.exe",
    args=[
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        ".vscode/start-tableau-mcp.ps1",
    ],
)

_TOOL_SCHEMA: dict | None = None


async def fetch_tool_json():
    global _TOOL_SCHEMA
    if _TOOL_SCHEMA is None:
        async with stdio_client(_TABLEAU_MCP_PARAMS) as (read,write):
            async with ClientSession(read,write) as session:
                await session.initialize()
                all_tools = await load_mcp_tools(session)
                queryDatasource_tool = next(t for t in all_tools if t.name == 'query-datasource')
                _TOOL_SCHEMA = queryDatasource_tool.args_schema.get("properties", {}).get("query", queryDatasource_tool.args_schema)
    
    return _TOOL_SCHEMA
# ---------------------------------------------------------------------------
# Step 5 — NL → VizQL query (Claude Haiku)
# ---------------------------------------------------------------------------

DIMENSION_FIELDS = ["REGION", "MANUFACTURER", "FREQUENCY", "Measure"]

_NL_TO_VIZQL_PROMPT = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            """You are helping to build a VizQL query for the ACEA automotive registration dataset.

Given a natural-language question, output a JSON object with structure as outlined in tool schema:

{schema}

Aggregation functions: SUM, AVG, COUNT, COUNTD, MIN, MAX, MEDIAN.
Dimension fields must NOT have a "function" key. Measure fields MUST have a "function" key.
Filters MUST use filterType "SET" with "field" and "values" as a list. Never use bare "fieldCaption"/"value" keys at the filter level.
Field captions and filter values must match EXACTLY from the available fields below.

IMPORTANT — data model: All numeric data lives in the single field "Value". The "Measure" dimension
specifies what kind of metric each row represents (e.g. UNITS, YTD, YTD_PoP, TTM_PoP, etc.).
Never use a Measure value (like YTD_PoP, UNITS, TTM) as a fieldCaption — they are always filter
values on the "Measure" field. Always set "context": true on Measure and REGION filters.

Available fields:
{fields_metadata}

Distinct values for each dimension (use these EXACT strings in filters):
{dimension_values}

Output ONLY the raw JSON object. No markdown, no explanation."""
        ),
        ("user", "{question}"),
    ]
)


async def _build_vizql_query(nl_query: str, fields_metadata: list, dimension_values: dict) -> str:
    """Use Claude Haiku to translate a natural-language query into a VizQL query dict."""
    fields_str = "\n".join(
        f"  {f.get('fieldCaption')} — {f.get('dataType', '?')} ({f.get('columnClass', '?')})"
        for f in fields_metadata if f.get("fieldCaption")
    )  
    values_str = "\n".join(
        f"  {field}: {values}" for field, values in dimension_values.items()
    )

    schema = await fetch_tool_json()

    llm = ChatAnthropic(model="claude-haiku-4-5-20251001", temperature=0)
    chain = _NL_TO_VIZQL_PROMPT | llm
    response = await chain.ainvoke({
        "question": nl_query,
        "fields_metadata": fields_str,
        "dimension_values": values_str,
        "schema": schema
    })
    text = response.content if hasattr(response, "content") else str(response)
    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()
    return text




# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

async def query_vizql(nl_query: str = 'Who are the top ten manufacturer in the EU region by units sold?'):
    """
    Translate *nl_query* into a VizQL structured query and return it as a string.
    """
    server, token, server_url = _sign_in()
    try:
        luid = _get_datasource_luid(server)
        fields_metadata = _read_metadata(server_url, token, luid)
        print(f"Fetched fields metadata: {fields_metadata}")
        dimension_values = {
            field: _read_dimension_values(server_url, token, luid, field)
            for field in DIMENSION_FIELDS
        }
        print(f"Fetched dimension_values: {dimension_values}")
        return await _build_vizql_query(nl_query, fields_metadata, dimension_values)
    finally:
        server.auth.sign_out()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    asyncio.run(query_vizql())
    
