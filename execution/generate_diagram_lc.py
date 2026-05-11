"""Generate end-to-end pipeline diagram (LangChain variant) as PNG.

pdf_extract_chain (LC) replaces the legacy pdf_scrape.py step.
"""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch
import os

OUT = os.path.join(os.path.dirname(__file__), "..", "pipeline_diagram_lc.png")

# ── colour palette ────────────────────────────────────────────────────────────
C_SRC  = "#dbeafe"; C_SRC_E  = "#3b82f6"   # blue   – stores / sources
C_SCR  = "#dcfce7"; C_SCR_E  = "#16a34a"   # green  – execution scripts
C_DBT  = "#d1fae5"; C_DBT_E  = "#059669"   # teal   – dbt models
C_CFG  = "#fef9c3"; C_CFG_E  = "#ca8a04"   # yellow – config / creds
C_AI   = "#f3e8ff"; C_AI_E   = "#9333ea"   # purple – AI orchestration
C_DIR  = "#fee2e2"; C_DIR_E  = "#dc2626"   # red    – directives
C_OUT  = "#e0f2fe"; C_OUT_E  = "#0284c7"   # sky    – analysis outputs
C_LC   = "#fff7ed"; C_LC_E   = "#ea580c"   # orange – LangChain LC tool
C_LEG  = "#fef2f2"; C_LEG_E  = "#ef4444"   # rose   – legacy (struck out)

fig, ax = plt.subplots(figsize=(26, 14))
ax.set_xlim(0, 26)
ax.set_ylim(0, 14)
ax.axis("off")
fig.patch.set_facecolor("#f8fafc")
ax.set_facecolor("#f8fafc")


# ── helpers ───────────────────────────────────────────────────────────────────
def box(ax, x, y, w, h, label, fc, ec, fontsize=8.5, bold=False, alpha=1.0):
    ax.add_patch(FancyBboxPatch(
        (x - w/2, y - h/2), w, h,
        boxstyle="round,pad=0.12", facecolor=fc, edgecolor=ec,
        linewidth=1.5, zorder=3, alpha=alpha))
    ax.text(x, y, label, ha="center", va="center", fontsize=fontsize,
            fontweight="bold" if bold else "normal", color="#1e293b",
            zorder=4, multialignment="center", linespacing=1.4, alpha=alpha)


def arrow(ax, x1, y1, x2, y2, color="#64748b", rad=0.0, lw=1.4, ls="-"):
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(
                    arrowstyle="-|>", lw=lw, color=color,
                    connectionstyle=f"arc3,rad={rad}",
                    linestyle=ls, mutation_scale=14))


def strike(ax, x, y, w):
    for dx in [(-1, -1), (1, -1)]:
        ax.plot([x + dx[0]*w*0.46, x - dx[0]*w*0.46],
                [y - 0.26, y + 0.26],
                color="#ef4444", lw=2.2, zorder=6)


# ── layer bands ───────────────────────────────────────────────────────────────
for y0, y1, label, col in [
    (11.8, 13.1, "Layer 1 · Directives",        "#fce7f3"),
    (9.3,  11.5, "Layer 2 · AI Orchestration",  "#ede9fe"),
    (1.0,   9.0, "Layer 3 · Execution & Data",  "#f0fdf4"),
]:
    ax.add_patch(mpatches.FancyBboxPatch(
        (0.3, y0), 25.4, y1-y0,
        boxstyle="round,pad=0.15", facecolor=col, edgecolor="#cbd5e1",
        linewidth=1, zorder=1, alpha=0.45))
    ax.text(0.55, (y0+y1)/2, label, ha="left", va="center",
            fontsize=8, color="#64748b", fontstyle="italic", zorder=2)

# ── title ─────────────────────────────────────────────────────────────────────
ax.text(13, 13.55,
        "ACEA Automotive Data Pipeline  ·  LangChain Upgrade: pdf_extract_chain",
        ha="center", va="center", fontsize=14, fontweight="bold", color="#1e293b")

# ── Layer 1 ───────────────────────────────────────────────────────────────────
box(ax, 13, 12.45, 4.2, 0.72,
    "directives/pdf_scrape.md\nSteps 0-7 SOP  ·  living document",
    C_DIR, C_DIR_E, bold=True)

# ── Layer 2 ───────────────────────────────────────────────────────────────────
box(ax, 13, 10.4, 6.2, 0.78,
    "AI Agent  (Claude / Gemini)\nReads directives  ·  sequences scripts  ·  handles errors  ·  updates SOPs",
    C_AI, C_AI_E, bold=True)
arrow(ax, 13, 12.09, 13, 10.79, color=C_DIR_E)

# ── Grid ──────────────────────────────────────────────────────────────────────
#  10 pipeline columns, evenly spaced
XS  = [1.4, 3.7, 6.0, 8.3, 10.6, 13.0, 15.4, 17.8, 20.2, 23.0]
SY  = 7.5   # script / model row
STY = 5.6   # data store row

# ─ Col 0: ACEA source ─────────────────────────────────────────────────────────
box(ax, XS[0], SY,  1.9, 0.7, "ACEA Website\nacea.auto",           C_SRC, C_SRC_E)

# ─ Col 1: webscrape ───────────────────────────────────────────────────────────
box(ax, XS[1], SY,  1.9, 0.7, "webscrape.py\nHTTP / Playwright",   C_SCR, C_SCR_E)
box(ax, XS[1], STY, 1.9, 0.7, "PDFs/ACEA/\n*.pdf",                 C_SRC, C_SRC_E)

# ─ Col 2: LEGACY pdf_scrape (struck out) ─────────────────────────────────────
LX = XS[2]
box(ax, LX, SY, 1.9, 0.7, "pdf_scrape.py\nPDF -> CSV parser",
    C_LEG, C_LEG_E, fontsize=8, alpha=0.35)
strike(ax, LX, SY, 1.9)
ax.text(LX, SY - 0.55, "REPLACED", ha="center", va="center",
        fontsize=7, color="#ef4444", fontweight="bold", zorder=7)

# ─ NEW LangChain node – floats above legacy, comfortably inside Layer 3 ───────
#   LC_Y = 8.55  =>  top = 8.55 + 0.45 = 9.0  < layer-2 band floor 9.3  ✓
#   legacy top  = 7.5 + 0.35 = 7.85  =>  gap below LC = 8.55-0.45-7.85 = 0.25 ✓
LC_Y = 8.55
box(ax, LX, LC_Y, 2.3, 0.9,
    "pdf_extract_chain\n(LangChain  LC)\nLLM-structured extraction",
    C_LC, C_LC_E, fontsize=8.5, bold=True)

# "NEW" badge – top-right corner, fully inside the box
BW, BH = 0.50, 0.24
ax.add_patch(FancyBboxPatch(
    (LX + 2.3/2 - BW - 0.08, LC_Y + 0.9/2 - BH - 0.07), BW, BH,
    boxstyle="round,pad=0.05", facecolor=C_LC_E, edgecolor="none", zorder=8))
ax.text(LX + 2.3/2 - BW/2 - 0.08, LC_Y + 0.9/2 - BH/2 - 0.07,
        "NEW", ha="center", va="center",
        fontsize=7, fontweight="bold", color="white", zorder=9)

# schema – placed right of LC, same row, clear gap
SCH_X = LX + 2.35   # = 6.0 + 2.35 = 8.35  (sits between LC and upload col)
SCH_Y = LC_Y
box(ax, SCH_X, SCH_Y, 2.0, 0.65,
    "schema/ACEA.csv\ncolumn schema", C_CFG, C_CFG_E, fontsize=8)

# Arrows around LC
arrow(ax, XS[1], STY + 0.35, LX, LC_Y - 0.45,   # PDFs -> LC (bottom)
      color=C_LC_E, rad=-0.25, lw=1.8)
arrow(ax, SCH_X - 1.0, SCH_Y, LX + 1.15, LC_Y,   # schema -> LC (horizontal)
      color=C_CFG_E, lw=1.4)

# ─ Col 2 store: CSV ──────────────────────────────────────────────────────────
box(ax, XS[2], STY, 1.9, 0.7, "data/*.csv\nintermediate", C_SRC, C_SRC_E)
arrow(ax, LX, LC_Y - 0.45, LX, STY + 0.35,        # LC -> CSV (down)
      color=C_LC_E, lw=1.8)

# ─ Col 3: upload ──────────────────────────────────────────────────────────────
box(ax, XS[3], SY,  1.9, 0.7, "upload_snowflake.py\nCSV -> Snowflake", C_SCR, C_SCR_E)
box(ax, XS[3], STY, 1.9, 0.75, "Snowflake\nACEA_DATA (raw)",           C_SRC, C_SRC_E)

# ─ dbt cols 4-6 ───────────────────────────────────────────────────────────────
box(ax, XS[4], SY, 1.9, 0.7,
    "stg_acea_data\nrename & standardise\n(view)",           C_DBT, C_DBT_E, fontsize=8)
box(ax, XS[5], SY, 1.9, 0.7,
    "int_acea_data\ndeduplicate · surrogate key\n(view)",    C_DBT, C_DBT_E, fontsize=8)
box(ax, XS[6], SY, 1.9, 0.85,
    "marts_acea_metrics\ndates · YTD/TTM/PoP/YoY\nmelt long (table)", C_DBT, C_DBT_E, fontsize=8)

# dbt bounding box
ax.add_patch(FancyBboxPatch(
    (XS[4]-1.0, SY-0.7), XS[6]-XS[4]+2.0, 1.05,
    boxstyle="round,pad=0.08", facecolor="none", edgecolor=C_DBT_E,
    linewidth=1.6, linestyle="--", zorder=2))
ax.text((XS[4]+XS[6])/2, SY+0.62, "dbt Core CLI",
        ha="center", va="center", fontsize=7.5, color=C_DBT_E, fontstyle="italic")

box(ax, XS[6], STY, 1.9, 0.75, "Snowflake\nMARTS_ACEA_METRICS", C_SRC, C_SRC_E)

# ─ Col 7: publish ─────────────────────────────────────────────────────────────
box(ax, XS[7], SY,  1.9, 0.7, "publish_tableau\n_datasource.py", C_SCR, C_SCR_E)
box(ax, XS[7], STY, 1.9, 0.7, "Tableau Datasource\nmarts_acea (Hyper)", C_SRC, C_SRC_E)

# ─ Col 8: MCP ────────────────────────────────────────────────────────────────
box(ax, XS[8], SY,  1.9, 0.7, "Tableau MCP\nVDS query -> CSV",       C_SCR, C_SCR_E)
box(ax, XS[8], STY, 1.9, 0.7, "analyses/\nYYYY-MM-DD/<title>/",      C_OUT, C_OUT_E)

# ─ Col 9: plot ────────────────────────────────────────────────────────────────
box(ax, XS[9], SY,  1.9, 0.7, "plot_monthly_ytd.py\ndata-viz-plots skill", C_SCR, C_SCR_E)
box(ax, XS[9], STY, 1.9, 0.75, "*.png  +\nrun_metadata.json",              C_OUT, C_OUT_E)

# ─ Logger ────────────────────────────────────────────────────────────────────
box(ax, 13, 2.2, 3.2, 0.65, "logger.py  ·  python.log", "#f1f5f9", "#94a3b8", fontsize=8)

# ─ Config nodes ──────────────────────────────────────────────────────────────
box(ax, XS[3], 3.5, 2.1, 0.65, ".env\nSnowflake + Tableau creds", C_CFG, C_CFG_E, fontsize=7.8)
box(ax, XS[8], 3.5, 2.2, 0.65, ".vscode/.env\nTableau PAT (MCP)",  C_CFG, C_CFG_E, fontsize=7.8)

# ── Main pipeline arrows ──────────────────────────────────────────────────────
arrow(ax, XS[0]+0.95, SY,      XS[1]-0.95, SY)               # source -> webscrape
arrow(ax, XS[1],      SY-0.35, XS[1],      STY+0.35)          # webscrape -> PDFs
arrow(ax, XS[2]+0.95, STY,     XS[3]-0.95, STY)               # CSV -> upload store
arrow(ax, XS[3],      STY+0.38,XS[3],      SY-0.35)           # store -> upload
arrow(ax, XS[3],      SY-0.35, XS[3],      STY+0.38)          # upload -> SF raw
arrow(ax, XS[3]+0.95, STY,     XS[4]-0.95, SY-0.1, rad=-0.25) # SF raw -> stg
arrow(ax, XS[4]+0.95, SY,      XS[5]-0.95, SY)                # stg -> int
arrow(ax, XS[5]+0.95, SY,      XS[6]-0.95, SY)                # int -> marts
arrow(ax, XS[6],      SY-0.43, XS[6],      STY+0.38)          # marts -> SF mart
arrow(ax, XS[6]+0.95, STY,     XS[7]-0.95, SY-0.1, rad=-0.25) # SF mart -> publish
arrow(ax, XS[7],      SY-0.35, XS[7],      STY+0.35)          # publish -> Tableau DS
arrow(ax, XS[7]+0.95, STY,     XS[8]-0.95, STY)               # DS -> MCP store
arrow(ax, XS[8],      STY+0.35,XS[8],      SY-0.35)           # store -> MCP
arrow(ax, XS[8],      SY-0.35, XS[8],      STY+0.35)          # MCP -> analyses
arrow(ax, XS[8]+0.95, STY,     XS[9]-0.95, SY-0.1, rad=-0.2)  # analyses -> plot
arrow(ax, XS[9],      SY-0.35, XS[9],      STY+0.38)          # plot -> outputs

# .env -> upload & publish
arrow(ax, XS[3], 3.83, XS[3]-0.1, SY-0.35, color=C_CFG_E, rad=-0.1)
arrow(ax, XS[3]+1.05, 3.5, XS[7]-0.2, SY-0.35, color=C_CFG_E, rad=0.1)
# .vscode/.env -> MCP
arrow(ax, XS[8], 3.83, XS[8], SY-0.35, color=C_CFG_E)

# AI agent -> scripts (dashed purple)
for xi in [XS[1], XS[3], XS[4], XS[7], XS[8], XS[9]]:
    ax.annotate("", xy=(xi, SY+0.35), xytext=(13, 10.01),
                arrowprops=dict(arrowstyle="-|>", lw=0.9, color="#a78bfa",
                                connectionstyle="arc3,rad=0.0",
                                linestyle="dashed", mutation_scale=10), zorder=2)
# AI -> LC node
ax.annotate("", xy=(LX, LC_Y+0.45), xytext=(13, 10.01),
            arrowprops=dict(arrowstyle="-|>", lw=1.2, color="#a78bfa",
                            connectionstyle="arc3,rad=0.0",
                            linestyle="dashed", mutation_scale=10), zorder=2)

# logger (dotted)
for xi in [XS[1], XS[3], XS[7], XS[8], XS[9]]:
    ax.annotate("", xy=(13, 2.53), xytext=(xi, SY-0.35),
                arrowprops=dict(arrowstyle="-|>", lw=0.7, color="#94a3b8",
                                connectionstyle="arc3,rad=0.0",
                                linestyle="dotted", mutation_scale=8), zorder=2)

# ── Legend ────────────────────────────────────────────────────────────────────
items = [
    (C_SRC, C_SRC_E, "Data store / source"),
    (C_SCR, C_SCR_E, "Execution script"),
    (C_DBT, C_DBT_E, "dbt model"),
    (C_CFG, C_CFG_E, "Config / credentials"),
    (C_AI,  C_AI_E,  "AI orchestration"),
    (C_OUT, C_OUT_E, "Analysis output"),
    (C_LC,  C_LC_E,  "LangChain LC tool  (NEW)"),
    (C_LEG, C_LEG_E, "Legacy step (replaced)"),
]
lx, ly = 0.5, 5.2
for fc, ec, lbl in items:
    ax.add_patch(FancyBboxPatch(
        (lx, ly-0.18), 0.42, 0.36,
        boxstyle="round,pad=0.05", facecolor=fc, edgecolor=ec,
        linewidth=1.2, zorder=5))
    ax.text(lx+0.56, ly, lbl, va="center", fontsize=7.5, color="#1e293b", zorder=5)
    ly -= 0.52

plt.tight_layout(pad=0)
fig.savefig(OUT, dpi=180, bbox_inches="tight", facecolor=fig.get_facecolor())
print(f"Saved -> {os.path.abspath(OUT)}")
