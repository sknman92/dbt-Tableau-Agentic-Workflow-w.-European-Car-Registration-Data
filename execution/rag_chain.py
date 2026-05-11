"""
Phase 3 — RAG (Retrieval-Augmented Generation) chain for ACEA data.

RAG = Retrieval + Generation:
  1. Retrieval: find the most relevant chunks from the vector store
  2. Generation: pass those chunks + the question to Claude to generate an answer

This gives Claude grounded, factual answers instead of hallucinations —
it can only answer based on what's actually in your PDFs.

Usage:
    python -m execution.rag_chain "Which manufacturer had the most EU registrations in Jan 2025?"
"""

from __future__ import annotations

import sys

import dotenv
from langchain_anthropic import ChatAnthropic
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough

from execution.vector_store import load_store

dotenv.load_dotenv()


# ---------------------------------------------------------------------------
# Step 1 — Retriever
#
# A retriever wraps the vector store and handles the similarity search.
# as_retriever(search_kwargs={"k": 6}) means: return the 6 most relevant
# chunks for each query.
#
# We use search_type="mmr" (Maximal Marginal Relevance) instead of plain
# similarity — MMR balances relevance AND diversity, so you don't get
# 6 chunks that all say the same thing.
# ---------------------------------------------------------------------------

def get_retriever(k: int = 6):
    store = load_store()
    return store.as_retriever(
        search_type="mmr",
        search_kwargs={"k": k},
    )


# ---------------------------------------------------------------------------
# Step 2 — Format retrieved docs for the prompt
#
# The retriever returns a list of Document objects. We need to convert
# them to a single string that fits into the prompt context window.
# We include the source filename so Claude (and you) can see where
# each piece of information came from.
# ---------------------------------------------------------------------------

def _format_docs(docs) -> str:
    sections = []
    for doc in docs:
        source = doc.metadata.get("source", "unknown")
        sections.append(f"[Source: {source}]\n{doc.page_content}")
    return "\n\n---\n\n".join(sections)


# ---------------------------------------------------------------------------
# Step 3 — RAG chain
#
# The chain is:
#   question  →  retriever (finds relevant chunks)
#                         ↓
#              prompt (inserts chunks + question into template)
#                         ↓
#                       LLM (Claude generates the answer)
#
# RunnablePassthrough() passes the question through unchanged to the prompt.
# The {"context": ..., "question": ...} dict feeds both slots in the template.
# ---------------------------------------------------------------------------

_RAG_PROMPT = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            """You are an analyst answering questions about European car registration data \
from ACEA press releases.

Answer the question using ONLY the context below. If the answer is not in the \
context, say so clearly — do not make up numbers.

When quoting figures, cite the source PDF in brackets, e.g. [Press_release_..._January_2025.pdf].

Context:
{context}""",
        ),
        ("user", "{question}"),
    ]
)


def build_rag_chain():
    """
    Returns a runnable chain:  question (str)  →  answer (str)

    Retrieves relevant chunks from the vector store, formats them into
    the prompt, and passes everything to Claude to generate a grounded answer.
    """
    retriever = get_retriever()
    llm = ChatAnthropic(model="claude-haiku-4-5-20251001", temperature=0)

    chain = (
        {"context": retriever | _format_docs, "question": RunnablePassthrough()}
        | _RAG_PROMPT
        | llm
    )
    return chain


def ask(question: str) -> str:
    """Ask a question against the ACEA vector store. Returns Claude's answer."""
    chain = build_rag_chain()
    response = chain.invoke(question)
    # ChatAnthropic returns an AIMessage; extract the text content
    return response.content if hasattr(response, "content") else str(response)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    question = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else \
        "Which manufacturers had the highest registrations in the EU in 2025?"
    print(f"\nQ: {question}\n{'-' * 60}")
    print(ask(question))
    print("-" * 60)