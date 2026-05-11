"""
Phase 3 — Vector store builder for ACEA PDF data.

Loads all PDFs from PDFs/ACEA/, extracts the manufacturer table section
from each one, and embeds them into a local Chroma vector store.

The store is persisted to chroma_db/ so it only needs to be built once
(or rebuilt when new PDFs are added).

Usage:
    python -m execution.vector_store          # build / rebuild the store
    python -m execution.vector_store --query "BMW registrations Jan 2025"
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

import pdfplumber
from langchain_chroma import Chroma
from langchain_core.documents import Document
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_text_splitters import RecursiveCharacterTextSplitter

from execution.pdf_extract_chain import _extract_table_section

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PDFS_DIR = Path("PDFs/ACEA")
CHROMA_DIR = "chroma_db"          # where Chroma persists its SQLite + vectors

# all-MiniLM-L6-v2: small (80 MB), fast, good for semantic similarity on
# short-to-medium text. Downloads automatically on first run.
EMBEDDING_MODEL = "all-MiniLM-L6-v2"


# ---------------------------------------------------------------------------
# Step 1 — Embeddings
#
# HuggingFaceEmbeddings wraps a sentence-transformers model. It converts
# a piece of text into a fixed-length numeric vector (an "embedding").
# Texts that are semantically similar end up as vectors that are close
# together in vector space — that's what makes similarity search work.
# ---------------------------------------------------------------------------

def get_embeddings() -> HuggingFaceEmbeddings:
    return HuggingFaceEmbeddings(model_name=EMBEDDING_MODEL)


# ---------------------------------------------------------------------------
# Step 2 — Load PDFs into LangChain Documents
#
# A LangChain Document is a simple container:
#   - page_content (str): the text to embed
#   - metadata (dict):    anything you want to store alongside it
#                         (returned with search results so you know the source)
#
# We extract only the manufacturer table section from each PDF (reusing
# _extract_table_section from pdf_extract_chain.py) rather than the full
# PDF text — this keeps the content focused and reduces noise.
# ---------------------------------------------------------------------------

def _parse_month_year_from_filename(name: str):
    """Return (month_abbr, year_str) from a PDF filename, or (None, None)."""
    months = {
        "january": "Jan", "february": "Feb", "march": "Mar", "april": "Apr",
        "may": "May", "june": "Jun", "july": "Jul", "august": "Aug",
        "september": "Sep", "october": "Oct", "november": "Nov", "december": "Dec",
    }
    name_lower = name.lower()
    month_abbr = next((v for k, v in months.items() if k in name_lower), None)
    tokens = name.replace("_", " ").split()
    year = next((t for t in tokens if t.isdigit() and len(t) == 4), None)
    return month_abbr, year


def load_documents(pdfs_dir: Path = PDFS_DIR) -> List[Document]:
    """
    Load all PDFs from pdfs_dir, extract the manufacturer table section,
    and return as a list of LangChain Documents with metadata.
    """
    docs: List[Document] = []

    for pdf_path in sorted(pdfs_dir.rglob("*.pdf")):
        lines: List[str] = []
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ""
                lines.extend(text.splitlines())

        # Only embed the manufacturer table — not press release boilerplate
        section_text = _extract_table_section(lines)
        if not section_text.strip():
            print(f"[vector_store] skipping {pdf_path.name} — no table section found")
            continue

        month_abbr, year = _parse_month_year_from_filename(pdf_path.stem)

        # Metadata is stored in Chroma alongside the embedding.
        # When a search returns this document, you get the metadata too —
        # so you know exactly which PDF the answer came from.
        docs.append(
            Document(
                page_content=section_text,
                metadata={
                    "source": pdf_path.name,
                    "month": month_abbr or "unknown",
                    "year": year or "unknown",
                    "month_year": f"{month_abbr}-{str(year)[-2:]}" if month_abbr and year else "unknown",
                },
            )
        )
        print(f"[vector_store] loaded {pdf_path.name} ({len(section_text)} chars)")

    return docs


# ---------------------------------------------------------------------------
# Step 3 — Chunking
#
# Each PDF table section is a few thousand characters. We split it into
# smaller overlapping chunks so the embeddings are more focused.
#
# chunk_size=1000:    each chunk is ~1000 characters
# chunk_overlap=200:  chunks overlap by 200 chars so context isn't lost
#                     at boundaries (e.g. a manufacturer row split across chunks)
#
# RecursiveCharacterTextSplitter tries to split on "\n\n", then "\n",
# then " " — so it avoids cutting mid-line where possible.
# ---------------------------------------------------------------------------

def split_documents(docs: List[Document]) -> List[Document]:
    splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=200)
    chunks = splitter.split_documents(docs)
    print(f"[vector_store] split {len(docs)} documents into {len(chunks)} chunks")
    return chunks


# ---------------------------------------------------------------------------
# Step 4 — Build and persist the Chroma store
#
# Chroma.from_documents():
#   - takes your chunks and embeddings model
#   - calls embeddings.embed_documents() on each chunk's text
#   - stores the resulting vectors + metadata in a local SQLite file
#
# persist_directory: Chroma writes to disk here so you don't have to
# rebuild on every run. Subsequent calls use load_store() instead.
# ---------------------------------------------------------------------------

def build_store(pdfs_dir: Path = PDFS_DIR, chroma_dir: str = CHROMA_DIR) -> Chroma:
    """Build the vector store from scratch and persist it to disk."""
    docs = load_documents(pdfs_dir)
    if not docs:
        raise ValueError(f"No documents loaded from {pdfs_dir}")

    chunks = split_documents(docs)
    embeddings = get_embeddings()

    print(f"[vector_store] embedding {len(chunks)} chunks (first run downloads the model)...")
    store = Chroma.from_documents(
        documents=chunks,
        embedding=embeddings,
        persist_directory=chroma_dir,
    )
    print(f"[vector_store] store built and persisted to {chroma_dir}/")
    return store


def load_store(chroma_dir: str = CHROMA_DIR) -> Chroma:
    """Load an already-built store from disk (fast — no re-embedding)."""
    return Chroma(
        persist_directory=chroma_dir,
        embedding_function=get_embeddings(),
    )


# ---------------------------------------------------------------------------
# Step 5 — Similarity search
#
# store.similarity_search(query, k=5) converts the query string into a
# vector using the same embedding model, then finds the k chunks whose
# vectors are closest (most similar) to the query vector.
#
# This is NOT keyword search — it finds semantically similar content
# even if the exact words don't match.
# ---------------------------------------------------------------------------

def search(query: str, k: int = 5, chroma_dir: str = CHROMA_DIR) -> List[Document]:
    """Search the vector store for the most relevant chunks."""
    store = load_store(chroma_dir)
    return store.similarity_search(query, k=k)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", help="Run a test similarity search after building")
    parser.add_argument("--rebuild", action="store_true", default=True, help="Rebuild the store")
    args = parser.parse_args()

    if args.rebuild:
        build_store()

    if args.query:
        results = search(args.query)
        for i, doc in enumerate(results, 1):
            print(f"\n--- Result {i} [{doc.metadata.get('source')}] ---")
            print(doc.page_content[:400])