from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from common import (
    FAMILY_ORDER,
    INK,
    INK_3,
    PERIODICAL_PALETTE,
    RED,
    ROOT,
    first_code_family,
    output_line,
    read_csv,
    save_figure,
    setup_theme,
)


def build_data(gold_csv: Path) -> pd.DataFrame:
    # Single-label backup to fig07: same human-gold sample (n = 180), no LLM labels.
    gold = read_csv(gold_csv)
    metric = gold.copy()
    # first_code_family folds TRAD into AUTH and returns None for dropped
    # families (WCL, LEGIT); drop those articles rather than bucket them.
    metric["primary_family"] = metric["human_gold_codes"].apply(first_code_family)
    metric = metric[metric["primary_family"].notna()]

    counts = (
        metric.groupby(["periodical", "primary_family"], observed=False)
        .size()
        .rename("n")
        .reset_index()
    )
    totals = metric.groupby("periodical").size().rename("total")
    counts = counts.merge(totals, on="periodical")
    counts["share"] = counts["n"] / counts["total"] * 100
    counts["periodical_label"] = counts["periodical"].str.title()

    grid = pd.MultiIndex.from_product(
        [sorted(counts["periodical"].unique()), FAMILY_ORDER],
        names=["periodical", "primary_family"],
    ).to_frame(index=False)
    counts = grid.merge(counts, on=["periodical", "primary_family"], how="left")
    counts["n"] = counts["n"].fillna(0).astype(int)
    counts["total"] = counts.groupby("periodical")["total"].transform(lambda s: s.ffill().bfill())
    counts["share"] = counts["share"].fillna(0)
    counts["periodical_label"] = counts["periodical"].str.title()
    return counts


def plot(data: pd.DataFrame, outdir: Path | None = None) -> list[Path]:
    import matplotlib.pyplot as plt
    import seaborn as sns

    setup_theme()
    fig, ax = plt.subplots(figsize=(11.5, 6.8))
    sns.barplot(
        data=data,
        y="primary_family",
        x="share",
        hue="periodical_label",
        order=FAMILY_ORDER,
        hue_order=["Candil", "Jaleo"],
        palette=PERIODICAL_PALETTE,
        ax=ax,
    )

    ax.set_xlabel("Share of human-gold sample", fontsize=12)
    ax.set_ylabel("")
    ax.set_xlim(0, max(45, data["share"].max() + 8))
    ax.xaxis.set_major_formatter(lambda x, _pos: f"{x:.0f}%")
    ax.legend(title="", frameon=False, loc="lower right", fontsize=12)
    ax.tick_params(axis="both", labelsize=11)

    for container in ax.containers:
        for bar in container:
            width = bar.get_width()
            if width <= 0:
                continue
            ax.text(
                width + 0.6,
                bar.get_y() + bar.get_height() / 2,
                f"{width:.1f}%",
                va="center",
                ha="left",
                fontsize=10,
                color=INK,
            )

    ax.axvline(0, color=INK, linewidth=1)

    return save_figure(fig, "fig01_primary_mode_share", outdir)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--gold-csv",
        type=Path,
        default=ROOT / "human_gold_audit_complete copy.csv",
        help="Human gold audit CSV. Defaults to the complete copy requested for the deck.",
    )
    parser.add_argument("--outdir", type=Path, default=ROOT / "figs")
    args = parser.parse_args()
    paths = plot(build_data(args.gold_csv), args.outdir)
    print(output_line(paths))


if __name__ == "__main__":
    main()
