from dataclasses import dataclass
from typing import Literal

VIZQL_KEYWORDS = {"tableau", "filter", "top", "bottom", "sort", "group by", "aggregate", "visualization", "chart", "graph"}
RAG_KEYWORDS = {"acea", "registration", "european", "car", "press release", "pdf"}

Destination = Literal["vizql", "rag", "hybrid", "clarify"]


@dataclass
class RouterDecision:
    query: str
    vizql_score: int
    rag_score: int
    destination: Destination


def route(q: str) -> RouterDecision:
    ql = q.lower()
    viz = sum(1 for k in VIZQL_KEYWORDS if k in ql)
    rag = sum(1 for k in RAG_KEYWORDS if k in ql)

    if viz == 0 and rag == 0:
        destination: Destination = "clarify"
    elif viz > 0 and rag > 0:
        destination = "hybrid"
    elif viz > rag:
        destination = "vizql"
    else:
        destination = "rag"

    return RouterDecision(query=q, vizql_score=viz, rag_score=rag, destination=destination)


async def dispatch(q: str) -> str:
    decision = route(q)

    match decision.destination:
        case "rag":
            from execution.rag_chain import ask
            return ask(q)

        case "vizql":
            from execution.mcp_utils import with_tableau_tools
            async def _query(tools):
                return await tools[0].ainvoke({"query": q})
            return await with_tableau_tools(_query)

        case "hybrid":
            from execution.rag_chain import ask
            from execution.mcp_utils import with_tableau_tools
            async def _query(tools):
                return await tools[0].ainvoke({"query": q})
            rag_answer = ask(q)
            vizql_answer = await with_tableau_tools(_query)
            return f"[RAG]\n{rag_answer}\n\n[VizQL]\n{vizql_answer}"

        case "clarify":
            return "I couldn't determine what you're asking about. Try including keywords like 'registration', 'chart', or 'filter'."