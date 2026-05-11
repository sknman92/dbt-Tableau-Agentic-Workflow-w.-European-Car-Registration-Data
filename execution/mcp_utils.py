import asyncio
from collections.abc import Callable, Awaitable
from typing import Any, Optional
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

### langchain imports ###
from langchain_mcp_adapters.tools import load_mcp_tools
from langchain_core.tools import BaseTool

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

_EXPOSED_TOOLS = {"query-datasource"}


async def with_tableau_tools(
    callback: Callable[[list[BaseTool]], Awaitable[Any]],
) -> Any:
    """
    Opens a Tableau MCP session, loads LangChain tools, calls callback(tools),
    then cleanly closes the session. The session stays open for the duration
    of the callback so tools can actually call session.call_tool.
    """
    async with stdio_client(_TABLEAU_MCP_PARAMS) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            all_tools = await load_mcp_tools(session)
            tools = [t for t in all_tools if t.name in _EXPOSED_TOOLS]
            return await callback(tools)


if __name__ == "__main__":
    async def _list(tools: list[BaseTool]) -> None:
        for t in tools:
            print(f"  {t.name}")

    asyncio.run(with_tableau_tools(_list))
