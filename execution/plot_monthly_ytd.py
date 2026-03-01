import argparse
import json
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


def sanitize_name(value: str) -> str:
    safe = "".join(ch.lower() if ch.isalnum() or ch in {"-", "_"} else "_" for ch in value.strip())
    return safe.strip("_") or "triage"


def build_run_directory(repo_root: Path, title: str, run_ts: datetime) -> Path:
    date_part = run_ts.strftime("%Y-%m-%d")
    title_part = sanitize_name(title) if title else "triage"
    base_dir = repo_root / "analyses" / date_part
    base_dir.mkdir(parents=True, exist_ok=True)

    run_dir = base_dir / title_part
    suffix = 2
    while run_dir.exists():
        run_dir = base_dir / f"{title_part}_{suffix}"
        suffix += 1

    run_dir.mkdir(parents=True, exist_ok=False)
    return run_dir


def resolve_input_csv(repo_root: Path, input_arg: str) -> Path:
    candidate = repo_root / input_arg
    if candidate.exists():
        return candidate

    pattern = "**/monthly_ytd_units_sold_per_manufacturer.csv"
    matches = sorted(
        (repo_root / "analyses").glob(pattern),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if matches:
        return matches[0]

    raise FileNotFoundError(
        "Input CSV not found. Provide --input-csv or place monthly_ytd_units_sold_per_manufacturer.csv under analyses/."
    )


def load_data(file_path: Path) -> pd.DataFrame:
    df = pd.read_csv(file_path)
    df["MONTH"] = pd.to_datetime(df["MONTH"])
    df = df.sort_values(["MANUFACTURER", "MONTH"]).reset_index(drop=True)
    return df


def plot_all_manufacturers(df: pd.DataFrame, output_path: Path) -> None:
    sns.set_style("whitegrid")
    plt.rcParams["figure.dpi"] = 150
    plt.rcParams["savefig.dpi"] = 300
    plt.rcParams["font.size"] = 10

    fig, ax = plt.subplots(figsize=(14, 8))

    for manufacturer, group in df.groupby("MANUFACTURER"):
        ax.plot(
            group["MONTH"],
            group["YTD_UNITS_SOLD"],
            linewidth=1.0,
            alpha=0.35,
            color="steelblue",
        )

    ax.set_title("Monthly YTD Units Sold per Manufacturer (All Manufacturers)", fontsize=14, fontweight="bold")
    ax.set_xlabel("Month")
    ax.set_ylabel("YTD Units Sold")
    ax.grid(alpha=0.3)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.tight_layout()
    plt.savefig(output_path, bbox_inches="tight")
    plt.close(fig)


def plot_top_manufacturers(df: pd.DataFrame, output_path: Path, top_n: int = 12) -> None:
    sns.set_style("whitegrid")
    plt.rcParams["figure.dpi"] = 150
    plt.rcParams["savefig.dpi"] = 300
    plt.rcParams["font.size"] = 10

    latest_month = df["MONTH"].max()
    top_manufacturers = (
        df[df["MONTH"] == latest_month]
        .sort_values("YTD_UNITS_SOLD", ascending=False)
        .head(top_n)["MANUFACTURER"]
        .tolist()
    )

    plot_df = df[df["MANUFACTURER"].isin(top_manufacturers)].copy()

    fig, ax = plt.subplots(figsize=(14, 8))
    sns.lineplot(
        data=plot_df,
        x="MONTH",
        y="YTD_UNITS_SOLD",
        hue="MANUFACTURER",
        linewidth=2,
        ax=ax,
    )

    ax.set_title(f"Monthly YTD Units Sold (Top {top_n} Manufacturers at Latest Month)", fontsize=14, fontweight="bold")
    ax.set_xlabel("Month")
    ax.set_ylabel("YTD Units Sold")
    ax.grid(alpha=0.3)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.legend(title="Manufacturer", bbox_to_anchor=(1.02, 1), loc="upper left", frameon=True)

    plt.tight_layout()
    plt.savefig(output_path, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-csv", default="analyses/monthly_ytd_units_sold_per_manufacturer.csv")
    parser.add_argument("--title", default="monthly_ytd_units_sold_per_manufacturer")
    parser.add_argument("--top-n", type=int, default=12)
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    run_ts = datetime.now()
    run_dir = build_run_directory(repo_root, args.title, run_ts)

    input_csv = resolve_input_csv(repo_root, args.input_csv)
    copied_csv = run_dir / "monthly_ytd_units_sold_per_manufacturer.csv"
    out_all = run_dir / "monthly_ytd_units_all_manufacturers.png"
    out_top = run_dir / f"monthly_ytd_units_top{args.top_n}_manufacturers.png"
    metadata_path = run_dir / "run_metadata.json"

    df = load_data(input_csv)
    df.to_csv(copied_csv, index=False)
    plot_all_manufacturers(df, out_all)
    plot_top_manufacturers(df, out_top, top_n=args.top_n)

    metadata = {
        "run_timestamp": run_ts.isoformat(),
        "title": args.title,
        "input_csv": input_csv.as_posix(),
        "artifacts": {
            "dataset_csv": copied_csv.as_posix(),
            "all_manufacturers_plot": out_all.as_posix(),
            "top_manufacturers_plot": out_top.as_posix(),
        },
        "query_context": {
            "measure": "YTD",
            "frequency": "M",
            "aggregation": "SUM(Value)",
            "grouping": ["MANUFACTURER", "TRUNC_MONTH(DATE)"],
        },
        "data_profile": {
            "rows": int(len(df)),
            "manufacturers": int(df["MANUFACTURER"].nunique()),
            "month_start": df["MONTH"].min().strftime("%Y-%m-%d"),
            "month_end": df["MONTH"].max().strftime("%Y-%m-%d"),
            "top_n": int(args.top_n),
        },
    }
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")

    print(f"Run folder: {run_dir.as_posix()}")
    print(f"Created: {copied_csv.as_posix()}")
    print(f"Created: {out_all.as_posix()}")
    print(f"Created: {out_top.as_posix()}")
    print(f"Created: {metadata_path.as_posix()}")


if __name__ == "__main__":
    main()
