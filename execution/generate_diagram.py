"""Generate end-to-end pipeline diagram as PNG."""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import os

OUT = os.path.join(os.path.dirname(__file__), "..", "pipeline_diagram.png")

# ── colour palette ────────────────────────────────────────────────────────────
C_SRC   = "#dbeafe"   # blue  – external sources / data stores
C_SRC_E = "#3b82f6"
C_SCR   = "#dcfce7"   # green – execution scripts
C_SCR_E = "#16a34a"
C_DBT   = "#d1fae5"   # teal  – dbt models
C_DBT_E = "#059669"
C_CFG   = "#fef9c3"   # yellow – config / schema
C_CFG_E = "#ca8a04"
C_AI    = "#f3e8ff"   # purple – AI orchestration
C_AI_E  = "#9333ea"
C_DIR   = "#fee2e2"   # red   – directives
C_DIR_E = "#dc2626"
C_OUT   = "#e0f2fe"   # sky   – outputs / analyses
C_OUT_E = "#0284c7"

ARROW = dict(arrowstyle="-|>", lw=1.4, color="#64748b",
             connectionstyle="arc3,rad=0.0",
             mutation_scale=14)

fig, ax = plt.subplots(figsize=(24, 13))
ax.set_xlim(0, 24)
ax.set_ylim(0, 13)
ax.axis("off")
fig.patch.set_facecolor("#f8fafc")
ax.set_facecolor("#f8fafc")


def box(ax, x, y, w, h, label, fc, ec, fontsize=8.5, bold=False):
    patch = FancyBboxPatch((x - w/2, y - h/2), w, h,
                           boxstyle="round,pad=0.12",
                           facecolor=fc, edgecolor=ec, linewidth=1.5, zorder=3)
    ax.add_patch(patch)
    weight = "bold" if bold else "normal"
    ax.text(x, y, label, ha="center", va="center", fontsize=fontsize,
            fontweight=weight, color="#1e293b", zorder=4,
            multialignment="center", linespacing=1.35)
    return patch


def arrow(ax, x1, y1, x2, y2, label="", rad=0.0, color="#64748b"):
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(arrowstyle="-|>", lw=1.4, color=color,
                                connectionstyle=f"arc3,rad={rad}",
                                mutation_scale=14))
    if label:
        mx, my = (x1+x2)/2, (y1+y2)/2
        ax.text(mx, my+0.18, label, ha="center", va="bottom",
                fontsize=7, color="#475569")


# ── Title ─────────────────────────────────────────────────────────────────────
ax.text(12, 12.5, "ACEA Automotive Data Pipeline  ·  End-to-End Architecture",
        ha="center", va="center", fontsize=14, fontweight="bold", color="#1e293b")

# ── Layer bands ───────────────────────────────────────────────────────────────
for y0, y1, label, col in [
    (10.8, 12.0, "Layer 1 · Directives", "#fce7f3"),
    (8.4,  10.5, "Layer 2 · AI Orchestration", "#ede9fe"),
    (1.0,   8.0, "Layer 3 · Execution & Data", "#f0fdf4"),
]:
    ax.add_patch(mpatches.FancyBboxPatch(
        (0.3, y0), 23.4, y1-y0,
        boxstyle="round,pad=0.15", facecolor=col, edgecolor="#cbd5e1",
        linewidth=1, zorder=1, alpha=0.45))
    ax.text(0.55, (y0+y1)/2, label, ha="left", va="center",
            fontsize=8, color="#64748b", fontstyle="italic", zorder=2)

# ── Layer 1: Directives ───────────────────────────────────────────────────────
box(ax, 12, 11.4, 4.2, 0.72,
    "directives/pdf_scrape.md\nSteps 0–7 SOP  ·  living document",
    C_DIR, C_DIR_E, bold=True)

# ── Layer 2: AI Agent ─────────────────────────────────────────────────────────
box(ax, 12, 9.5, 6.0, 0.78,
    "AI Agent  (Claude / Gemini)\nReads directives · sequences scripts · handles errors · updates SOPs",
    C_AI, C_AI_E, bold=True)

arrow(ax, 12, 11.04, 12, 9.9, color=C_DIR_E)

# ── Execution pipeline nodes (y ≈ 6.8 main lane, stores y ≈ 5.0) ─────────────
#  x positions for pipeline stages
XS = [1.3, 3.5, 5.7, 7.9, 10.1, 12.3, 14.5, 16.7, 18.9, 21.5]

SCRIPTS_Y  = 6.8
STORES_Y   = 5.0
DBT_Y      = 6.8
SCHEMA_Y   = 8.0   # above pdf_scrape

# Stage 0 – source
box(ax, XS[0], SCRIPTS_Y, 1.8, 0.7,
    "ACEA Website\nacea.auto", C_SRC, C_SRC_E)

# Stage 1 – webscrape
box(ax, XS[1], SCRIPTS_Y, 1.8, 0.7,
    "webscrape.py\nHTTP / Playwright", C_SCR, C_SCR_E)

# PDFs store
box(ax, XS[1], STORES_Y, 1.8, 0.7,
    "PDFs/ACEA/\n*.pdf", C_SRC, C_SRC_E)

# Stage 2 – pdf_scrape
box(ax, XS[2], SCRIPTS_Y, 1.8, 0.7,
    "pdf_scrape.py\nPDF → CSV parser", C_SCR, C_SCR_E)

# schema reference
box(ax, XS[2], SCHEMA_Y, 1.9, 0.65,
    "schema/ACEA.csv\ncolumn schema", C_CFG, C_CFG_E, fontsize=8)

# CSV store
box(ax, XS[2], STORES_Y, 1.8, 0.7,
    "data/*.csv\nintermediate", C_SRC, C_SRC_E)

# Stage 3 – upload
box(ax, XS[3], SCRIPTS_Y, 1.8, 0.7,
    "upload_snowflake.py\nCSV → Snowflake", C_SCR, C_SCR_E)

# Snowflake raw
box(ax, XS[3], STORES_Y, 1.8, 0.75,
    "Snowflake\nACEA_DATA (raw)", C_SRC, C_SRC_E)

# ── dbt layer ────────────────────────────────────────────────────────────────
box(ax, XS[4], SCRIPTS_Y, 1.8, 0.7,
    "stg_acea_data\nrename & standardise\n(view)", C_DBT, C_DBT_E, fontsize=8)

box(ax, XS[5], SCRIPTS_Y, 1.9, 0.7,
    "int_acea_data\ndeduplicate · surrogate key\n(view)", C_DBT, C_DBT_E, fontsize=8)

box(ax, XS[6], SCRIPTS_Y, 1.9, 0.85,
    "marts_acea_metrics\ndates · YTD/TTM/PoP/YoY\nmelt long (table)", C_DBT, C_DBT_E, fontsize=8)

# dbt banner
ax.add_patch(FancyBboxPatch(
    (XS[4]-1.0, SCRIPTS_Y-0.7), XS[6]-XS[4]+2.0, 1.05,
    boxstyle="round,pad=0.08", facecolor="none", edgecolor=C_DBT_E,
    linewidth=1.6, linestyle="--", zorder=2))
ax.text((XS[4]+XS[6])/2, SCRIPTS_Y+0.62, "dbt Core CLI",
        ha="center", va="center", fontsize=7.5, color=C_DBT_E, fontstyle="italic")

# Snowflake mart
box(ax, XS[6], STORES_Y, 1.9, 0.75,
    "Snowflake\nMARTS_ACEA_METRICS", C_SRC, C_SRC_E)

# Stage 5 – publish
box(ax, XS[7], SCRIPTS_Y, 1.8, 0.7,
    "publish_tableau\n_datasource.py", C_SCR, C_SCR_E)

# Tableau DS
box(ax, XS[7], STORES_Y, 1.8, 0.7,
    "Tableau Datasource\nmarts_acea (Hyper)", C_SRC, C_SRC_E)

# Stage 6 – MCP query
box(ax, XS[8], SCRIPTS_Y, 1.8, 0.7,
    "Tableau MCP\nVDS query → CSV", C_SCR, C_SCR_E)

# analyses store
box(ax, XS[8], STORES_Y, 1.8, 0.7,
    "analyses/\nYYYY-MM-DD/<title>/", C_OUT, C_OUT_E)

# Stage 7 – plot
box(ax, XS[9], SCRIPTS_Y, 1.8, 0.7,
    "plot_monthly_ytd.py\ndata-viz-plots skill", C_SCR, C_SCR_E)

# outputs
box(ax, XS[9], STORES_Y, 1.8, 0.75,
    "*.png  +\nrun_metadata.json", C_OUT, C_OUT_E)

# ── logger ───────────────────────────────────────────────────────────────────
box(ax, 12, 2.2, 3.2, 0.65,
    "logger.py  ·  python.log", "#f1f5f9", "#94a3b8", fontsize=8)

# ── config nodes ─────────────────────────────────────────────────────────────
box(ax, 7.9, 3.4, 2.0, 0.65,
    ".env\nSnowflake + Tableau creds", C_CFG, C_CFG_E, fontsize=7.8)

box(ax, 16.7, 3.4, 2.2, 0.65,
    ".vscode/.env\nTableau PAT (MCP)", C_CFG, C_CFG_E, fontsize=7.8)

# ── arrows: main pipeline (script → store → next script) ─────────────────────
# source → webscrape
arrow(ax, XS[0]+0.9, SCRIPTS_Y, XS[1]-0.9, SCRIPTS_Y)
# webscrape → PDFs
arrow(ax, XS[1], SCRIPTS_Y-0.35, XS[1], STORES_Y+0.35)
# PDFs → pdf_scrape (bottom lane)
arrow(ax, XS[1]+0.9, STORES_Y, XS[2]-0.9, STORES_Y)
# PDFs → pdf_scrape (top trigger)
arrow(ax, XS[2], STORES_Y+0.35, XS[2], SCRIPTS_Y-0.35)
# schema → pdf_scrape
arrow(ax, XS[2], SCHEMA_Y-0.33, XS[2], SCRIPTS_Y+0.35, color=C_CFG_E)
# pdf_scrape → CSV
arrow(ax, XS[2], SCRIPTS_Y-0.35, XS[2], STORES_Y+0.35)
# CSV → upload
arrow(ax, XS[2]+0.9, STORES_Y, XS[3]-0.9, STORES_Y)
arrow(ax, XS[3], STORES_Y+0.35, XS[3], SCRIPTS_Y-0.35)
# .env → upload
arrow(ax, 7.9, 3.73, XS[3]-0.2, SCRIPTS_Y-0.35, color=C_CFG_E, rad=-0.15)
# upload → Snowflake raw
arrow(ax, XS[3], SCRIPTS_Y-0.35, XS[3], STORES_Y+0.38)
# Snowflake raw → stg
arrow(ax, XS[3]+0.9, STORES_Y, XS[4]-0.9, SCRIPTS_Y-0.1, rad=-0.25)
# stg → int → marts (dbt chain)
arrow(ax, XS[4]+0.9, SCRIPTS_Y, XS[5]-0.95, SCRIPTS_Y)
arrow(ax, XS[5]+0.95, SCRIPTS_Y, XS[6]-0.95, SCRIPTS_Y)
# marts → Snowflake mart
arrow(ax, XS[6], SCRIPTS_Y-0.43, XS[6], STORES_Y+0.38)
# Snowflake mart → publish
arrow(ax, XS[6]+0.95, STORES_Y, XS[7]-0.9, SCRIPTS_Y-0.1, rad=-0.25)
# .env → publish
arrow(ax, 7.9+1.0, 3.4, XS[7]-0.2, SCRIPTS_Y-0.35, color=C_CFG_E, rad=0.12)
# publish → Tableau DS
arrow(ax, XS[7], SCRIPTS_Y-0.35, XS[7], STORES_Y+0.35)
# Tableau DS → MCP
arrow(ax, XS[7]+0.9, STORES_Y, XS[8]-0.9, STORES_Y)
arrow(ax, XS[8], STORES_Y+0.35, XS[8], SCRIPTS_Y-0.35)
# .vscode/.env → MCP
arrow(ax, 16.7+0.2, 3.73, XS[8]-0.2, SCRIPTS_Y-0.35, color=C_CFG_E, rad=0.15)
# MCP → analyses
arrow(ax, XS[8], SCRIPTS_Y-0.35, XS[8], STORES_Y+0.35)
# analyses → plot
arrow(ax, XS[8]+0.9, STORES_Y, XS[9]-0.9, SCRIPTS_Y-0.1, rad=-0.2)
# plot → outputs
arrow(ax, XS[9], SCRIPTS_Y-0.35, XS[9], STORES_Y+0.38)

# AI agent → scripts (dashed representative arrows)
for xi in [XS[1], XS[2], XS[3], XS[4], XS[7], XS[8], XS[9]]:
    ax.annotate("", xy=(xi, SCRIPTS_Y+0.35), xytext=(12, 9.11),
                arrowprops=dict(arrowstyle="-|>", lw=0.9, color="#a78bfa",
                                connectionstyle="arc3,rad=0.0",
                                linestyle="dashed", mutation_scale=10),
                zorder=2)

# logger arrows (dashed thin)
for xi in [XS[1], XS[2], XS[3], XS[7], XS[8], XS[9]]:
    ax.annotate("", xy=(12, 2.53), xytext=(xi, SCRIPTS_Y-0.35),
                arrowprops=dict(arrowstyle="-|>", lw=0.7, color="#94a3b8",
                                connectionstyle="arc3,rad=0.0",
                                linestyle="dotted", mutation_scale=8), zorder=2)

# ── Legend ───────────────────────────────────────────────────────────────────
legend_items = [
    (C_SRC, C_SRC_E, "Data store / source"),
    (C_SCR, C_SCR_E, "Execution script"),
    (C_DBT, C_DBT_E, "dbt model"),
    (C_CFG, C_CFG_E, "Config / credentials"),
    (C_AI,  C_AI_E,  "AI orchestration"),
    (C_OUT, C_OUT_E, "Analysis output"),
]
lx, ly = 0.5, 4.5
for fc, ec, label in legend_items:
    ax.add_patch(FancyBboxPatch((lx, ly-0.18), 0.42, 0.36,
                                boxstyle="round,pad=0.05",
                                facecolor=fc, edgecolor=ec, linewidth=1.2, zorder=5))
    ax.text(lx+0.56, ly, label, va="center", fontsize=7.5, color="#1e293b", zorder=5)
    ly -= 0.52

plt.tight_layout(pad=0)
fig.savefig(OUT, dpi=180, bbox_inches="tight", facecolor=fig.get_facecolor())
print(f"Saved -> {os.path.abspath(OUT)}")
