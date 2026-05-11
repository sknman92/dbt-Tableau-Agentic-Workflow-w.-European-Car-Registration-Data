"""Generate pdf_extract_chain diagram from actual source code."""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch
import os

OUT = os.path.join(os.path.dirname(__file__), "..", "pdf_extract_chain_diagram.png")

C_ENTRY = "#e0f2fe"; C_ENTRY_E = "#0284c7"   # sky    – entry point
C_SEC   = "#fef9c3"; C_SEC_E   = "#ca8a04"   # yellow – section extractor
C_PRM   = "#fef3c7"; C_PRM_E   = "#d97706"   # amber  – prompt
C_LLM   = "#ede9fe"; C_LLM_E   = "#7c3aed"   # purple – LLM
C_MDL   = "#faf5ff"; C_MDL_E   = "#9333ea"   # light purple – pydantic
C_MAP   = "#dcfce7"; C_MAP_E   = "#16a34a"   # green  – field map
C_OUT   = "#f0fdf4"; C_OUT_E   = "#15803d"   # mint   – output
C_ERR   = "#fee2e2"; C_ERR_E   = "#dc2626"   # red    – none / fallback
C_LC    = "#fff7ed"; C_LC_E    = "#ea580c"   # orange – LCEL boundary
C_SIDE  = "#f1f5f9"; C_SIDE_E  = "#94a3b8"   # grey   – side note

fig, ax = plt.subplots(figsize=(11, 15))
ax.set_xlim(0, 11)
ax.set_ylim(0, 15)
ax.axis("off")
fig.patch.set_facecolor("#f8fafc")
ax.set_facecolor("#f8fafc")


def box(ax, x, y, w, h, label, fc, ec, fontsize=8.5, bold=False, ls="-", lw=1.8):
    ax.add_patch(FancyBboxPatch(
        (x - w/2, y - h/2), w, h,
        boxstyle="round,pad=0.14", facecolor=fc, edgecolor=ec,
        linewidth=lw, linestyle=ls, zorder=3))
    ax.text(x, y, label, ha="center", va="center", fontsize=fontsize,
            fontweight="bold" if bold else "normal", color="#1e293b",
            zorder=4, multialignment="center", linespacing=1.5)


def arrow(ax, x1, y1, x2, y2, color="#64748b", lw=1.6, rad=0.0, ls="-"):
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(arrowstyle="-|>", lw=lw, color=color,
                                connectionstyle=f"arc3,rad={rad}",
                                linestyle=ls, mutation_scale=14))


def step_badge(ax, x, y, text):
    ax.add_patch(FancyBboxPatch(
        (x - 0.38, y - 0.17), 0.76, 0.34,
        boxstyle="round,pad=0.05", facecolor="#1e293b", edgecolor="none", zorder=6))
    ax.text(x, y, text, ha="center", va="center",
            fontsize=7, fontweight="bold", color="white", zorder=7)


CX = 5.5

# ── Title ─────────────────────────────────────────────────────────────────────
ax.text(CX, 14.65, "pdf_extract_chain.py", ha="center",
        fontsize=14, fontweight="bold", color="#1e293b")
ax.text(CX, 14.25, "execution/pdf_extract_chain.py  ·  4 code sections",
        ha="center", fontsize=8.5, color="#64748b")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 4  — Public entry point   extract_rows_with_llm()
# ══════════════════════════════════════════════════════════════════════════════
Y4 = 13.35
box(ax, CX, Y4, 8.5, 0.8,
    "Step 4 · extract_rows_with_llm( lines, current_month, prior_month, pdf_month )\n"
    "Public entry point — called by pdf_scrape.parse_acea_pdf()",
    C_ENTRY, C_ENTRY_E, bold=True)
step_badge(ax, CX - 3.95, Y4, "step 4")

# side: inputs
box(ax, 0.75, Y4, 1.0, 0.8,
    "lines\nList[str]", C_SIDE, C_SIDE_E, fontsize=7.5)
arrow(ax, 1.25, Y4, 1.25, Y4, color=C_SIDE_E)  # dummy, real below
arrow(ax, 1.25, Y4, CX - 4.25, Y4, color=C_SIDE_E, lw=1.2)

arrow(ax, CX, 12.95, CX, 12.52, color=C_ENTRY_E, lw=1.6)

# ══════════════════════════════════════════════════════════════════════════════
# STEP 3  — Section extractor   _extract_table_section()
# ══════════════════════════════════════════════════════════════════════════════
Y3 = 12.1
box(ax, CX, Y3, 8.5, 0.72,
    "Step 3 · _extract_table_section( lines )  ->  str\n"
    "Scans for 'NEW CAR REGISTRATIONS BY MANUFACTURER' — returns only those lines\n"
    "(reduces token usage;  falls back to all lines if marker not found)",
    C_SEC, C_SEC_E, fontsize=8.2)
step_badge(ax, CX - 3.95, Y3, "step 3")

arrow(ax, CX, 11.74, CX, 11.27, color="#64748b", lw=1.6)

# section_text label
ax.text(CX + 0.15, 11.5, "section_text  (str)", ha="left", va="center",
        fontsize=7.5, color="#64748b", fontstyle="italic")

# ══════════════════════════════════════════════════════════════════════════════
# LCEL chain boundary
# ══════════════════════════════════════════════════════════════════════════════
ax.add_patch(FancyBboxPatch(
    (1.3, 7.45), 8.4, 3.55,
    boxstyle="round,pad=0.18", facecolor="#fffbf5", edgecolor=C_LC_E,
    linewidth=2, linestyle="--", zorder=1))
ax.text(1.7, 10.83, "Step 2 · get_extraction_chain()  ->  prompt | structured_llm",
        ha="left", va="center", fontsize=8.5, color=C_LC_E,
        fontweight="bold", fontstyle="italic")

# Prompt box
Y_PRM = 10.25
box(ax, CX, Y_PRM, 7.8, 0.78,
    "ChatPromptTemplate.from_messages()\n"
    "  system: _SYSTEM_PROMPT  { current_month, prior_month, pdf_month }\n"
    "  user:   { text }   <- section_text",
    C_PRM, C_PRM_E, fontsize=8.2)

# pipe glyph
ax.add_patch(FancyBboxPatch(
    (5.22, 9.68), 0.56, 0.40,
    boxstyle="round,pad=0.05", facecolor="#1e293b", edgecolor="none", zorder=6))
ax.text(CX, 9.88, "|", ha="center", va="center",
        fontsize=11, fontweight="bold", color="white", zorder=7)
ax.text(CX + 0.45, 9.88, "LCEL pipe", ha="left", va="center",
        fontsize=7.5, color="#64748b", fontstyle="italic")

arrow(ax, CX, 9.86, CX, 9.68, color="#64748b", lw=0)  # spacer
arrow(ax, CX, 9.65, CX, 9.15, color="#64748b", lw=1.4)

# LLM box
Y_LLM = 8.75
box(ax, CX, Y_LLM, 7.8, 0.72,
    "ChatAnthropic( model='claude-haiku-4-5-20251001', temperature=0 )\n"
    ".with_structured_output( ACEAExtraction )   <- forces JSON matching schema",
    C_LLM, C_LLM_E, fontsize=8.2)

arrow(ax, CX, Y3 - 0.36, CX, 10.65, color="#64748b", lw=1.6)
arrow(ax, CX, 8.39, CX, 7.87, color=C_LLM_E, lw=1.8)

# invoke label
ax.text(CX + 0.15, 8.12, "chain.invoke( text, current_month, prior_month, pdf_month )",
        ha="left", va="center", fontsize=7.5, color="#64748b", fontstyle="italic")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 1  — Pydantic schema   ACEARow / ACEAExtraction
# ══════════════════════════════════════════════════════════════════════════════
Y1 = 7.05
box(ax, CX, Y1, 8.5, 0.95,
    "Step 1 · ACEAExtraction  { rows: List[ACEARow] }\n"
    "ACEARow fields:  manufacturer · frequency (M | YTD) · month (Mon-YY)\n"
    "                 units (int)  · region  · pdf_month",
    C_MDL, C_MDL_E, fontsize=8.2)
step_badge(ax, CX - 3.95, Y1, "step 1")
step_badge(ax, CX - 3.95, Y1 - 0.32, "step 2")

arrow(ax, CX, 6.57, CX, 6.1, color="#64748b", lw=1.6)

# check label
ax.text(CX + 0.15, 6.33, "result.rows  (empty? -> return None)",
        ha="left", va="center", fontsize=7.5, color="#64748b", fontstyle="italic")

# ══════════════════════════════════════════════════════════════════════════════
# Field rename
# ══════════════════════════════════════════════════════════════════════════════
Y_MAP = 5.62
box(ax, CX, Y_MAP, 8.5, 0.72,
    "Field rename   snake_case  ->  Title case  (matches DataFrame columns)\n"
    "manufacturer -> Manufacturer  ·  frequency -> Frequency  ·  month -> Month\n"
    "units -> Units  ·  region -> Region  ·  pdf_month -> PDF",
    C_MAP, C_MAP_E, fontsize=8.2)

arrow(ax, CX, 5.26, CX, 4.77, color=C_MAP_E, lw=1.8)

# ══════════════════════════════════════════════════════════════════════════════
# Output
# ══════════════════════════════════════════════════════════════════════════════
Y_OUT = 4.38
box(ax, CX, Y_OUT, 8.5, 0.72,
    "return  List[dict]   ->  pdf_scrape wraps in DataFrame  ->  data/<stem>_schema.csv\n"
    "or  return None  ->  caller falls back to regex  _parse_acea_lines()",
    C_OUT, C_OUT_E, fontsize=8.5, bold=True)

# None path label
ax.text(CX - 3.75, 4.5, "None ->", ha="left", va="center",
        fontsize=7.5, color=C_ERR_E, fontstyle="italic")

# ── Legend ────────────────────────────────────────────────────────────────────
items = [
    (C_ENTRY, C_ENTRY_E, "Entry point (Step 4)"),
    (C_SEC,   C_SEC_E,   "Section extractor (Step 3)"),
    (C_PRM,   C_PRM_E,   "Prompt template"),
    (C_LLM,   C_LLM_E,   "LLM / chain (Step 2)"),
    (C_MDL,   C_MDL_E,   "Pydantic schema (Step 1)"),
    (C_MAP,   C_MAP_E,   "Field rename"),
    (C_OUT,   C_OUT_E,   "Output / return"),
]
lx, ly = 0.25, 3.4
for fc, ec, lbl in items:
    ax.add_patch(FancyBboxPatch(
        (lx, ly - 0.16), 0.36, 0.32,
        boxstyle="round,pad=0.04", facecolor=fc, edgecolor=ec,
        linewidth=1.2, zorder=5))
    ax.text(lx + 0.5, ly, lbl, va="center", fontsize=7.5,
            color="#1e293b", zorder=5)
    ly -= 0.46

plt.tight_layout(pad=0)
fig.savefig(OUT, dpi=180, bbox_inches="tight", facecolor=fig.get_facecolor())
print(f"Saved -> {os.path.abspath(OUT)}")
