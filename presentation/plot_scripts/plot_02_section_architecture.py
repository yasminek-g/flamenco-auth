from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from common import (
    INK,
    INK_2,
    INK_3,
    PAPER_2,
    RED,
    ROOT,
    output_line,
    read_csv,
    save_figure,
    setup_theme,
)


def build_data(
    candil_sections_csv: Path,
    candil_articles_csv: Path,
    jaleo_sections_csv: Path,
    min_jaleo_section_count: int,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, float]]:
    candil = read_csv(candil_sections_csv)
    candil = candil.rename(
        columns={
            "detected_section_label": "section",
            "n_articles": "articles",
            "n_issues": "issues",
        }
    )
    candil["periodical"] = "Candil"
    candil_articles = read_csv(candil_articles_csv)
    candil_total = int(len(candil_articles))
    candil_sectioned = int(candil_articles["kept_as_recurring"].fillna(False).astype(bool).sum())

    jaleo_review = read_csv(jaleo_sections_csv)
    jaleo_counts = (
        jaleo_review["section_candidate"]
        .value_counts(dropna=False)
        .rename_axis("section")
        .reset_index(name="articles")
    )
    jaleo_counts = jaleo_counts[jaleo_counts["section"].ne("Unclassified")].copy()
    jaleo_counts = jaleo_counts[jaleo_counts["articles"].ge(min_jaleo_section_count)]
    jaleo_counts["issues"] = pd.NA
    jaleo_counts["periodical"] = "Jaleo"

    total_jaleo = len(jaleo_review)
    recurring_jaleo_articles = int(jaleo_counts["articles"].sum())
    coverage = pd.DataFrame(
        [
            {
                "periodical": "Candil",
                "status": "Recurring section",
                "articles": candil_sectioned,
                "share": candil_sectioned / candil_total * 100,
            },
            {
                "periodical": "Candil",
                "status": "No recurring section",
                "articles": candil_total - candil_sectioned,
                "share": (candil_total - candil_sectioned) / candil_total * 100,
            },
            {
                "periodical": "Jaleo",
                "status": "Recurring section",
                "articles": recurring_jaleo_articles,
                "share": recurring_jaleo_articles / total_jaleo * 100,
            },
            {
                "periodical": "Jaleo",
                "status": "No recurring section",
                "articles": total_jaleo - recurring_jaleo_articles,
                "share": (total_jaleo - recurring_jaleo_articles) / total_jaleo * 100,
            },
        ]
    )
    rank_data = pd.concat(
        [
            candil[["periodical", "section", "articles"]].assign(
                rank=lambda df: df["articles"].rank(method="first", ascending=False).astype(int)
            ),
            jaleo_counts[["periodical", "section", "articles"]].assign(
                rank=lambda df: df["articles"].rank(method="first", ascending=False).astype(int)
            ),
        ],
        ignore_index=True,
    )
    stats = {
        "candil_sections": int(len(candil)),
        "candil_total_articles": candil_total,
        "candil_sectioned_articles": candil_sectioned,
        "candil_max_years": int(candil["issues"].max()),
        "jaleo_sections": int(len(jaleo_counts)),
        "jaleo_total_articles": int(total_jaleo),
        "jaleo_unsectioned_share": 1 - recurring_jaleo_articles / total_jaleo,
    }
    return coverage, rank_data, stats


def plot(
    coverage: pd.DataFrame,
    rank_data: pd.DataFrame,
    stats: dict[str, float],
    outdir: Path | None = None,
) -> list[Path]:
    import matplotlib.pyplot as plt
    import seaborn as sns

    setup_theme()
    fig = plt.figure(figsize=(12, 6.8))
    gs = fig.add_gridspec(1, 2, width_ratios=[1.05, 1.45], wspace=0.32)
    ax_cover = fig.add_subplot(gs[0, 0])
    ax_rank = fig.add_subplot(gs[0, 1])

    # Coverage: stacked horizontal bars make the sectioned/unsectioned contrast explicit.
    y_positions = {"Candil": 1, "Jaleo": 0}
    for periodical in ["Candil", "Jaleo"]:
        row = coverage[coverage["periodical"].eq(periodical)]
        sectioned = float(row[row["status"].eq("Recurring section")]["share"].iloc[0])
        unsectioned = 100 - sectioned
        color = RED if periodical == "Candil" else INK_2
        y = y_positions[periodical]
        ax_cover.barh(y, sectioned, color=color, height=0.38)
        ax_cover.barh(y, unsectioned, left=sectioned, color=PAPER_2, edgecolor="#d8d0c6", height=0.38)
        ax_cover.text(sectioned / 2, y, f"{sectioned:.0f}%", ha="center", va="center", color="white", fontsize=12, fontweight="bold")
        ax_cover.text(sectioned + unsectioned / 2, y, f"{unsectioned:.0f}%", ha="center", va="center", color=INK_3, fontsize=12, fontweight="bold")

    ax_cover.set_yticks([1, 0])
    ax_cover.set_yticklabels(["Candil", "Jaleo"])
    ax_cover.set_xlim(0, 100)
    ax_cover.set_xlabel("Share of articles", fontsize=12)
    ax_cover.xaxis.set_major_formatter(lambda x, _pos: f"{x:.0f}%")
    ax_cover.grid(axis="y", visible=False)
    ax_cover.tick_params(axis="both", labelsize=11)

    # Rank-size view avoids long section-label whitespace while preserving architecture depth.
    sns.lineplot(
        data=rank_data,
        x="rank",
        y="articles",
        hue="periodical",
        hue_order=["Candil", "Jaleo"],
        palette={"Candil": RED, "Jaleo": INK_2},
        marker="o",
        linewidth=2.2,
        markersize=5,
        ax=ax_rank,
    )
    ax_rank.set_xlabel("Section rank by article count", fontsize=12)
    ax_rank.set_ylabel("Articles in section", fontsize=12)
    ax_rank.set_xlim(0.5, max(rank_data["rank"]) + 0.75)
    ax_rank.set_ylim(0, rank_data["articles"].max() + 12)
    ax_rank.legend(title="", frameon=False, loc="upper right", fontsize=11)
    ax_rank.tick_params(axis="both", labelsize=11)
    ax_rank.axvline(stats["jaleo_sections"], color=INK_2, linestyle=(0, (2, 3)), linewidth=1)
    ax_rank.axvline(stats["candil_sections"], color=RED, linestyle=(0, (2, 3)), linewidth=1)
    ax_rank.text(stats["jaleo_sections"] + 0.3, 7, "Jaleo ends", color=INK_3, fontsize=9)
    ax_rank.text(stats["candil_sections"] - 5.6, 18, "Candil continues", color=RED, fontsize=9)

    fig.subplots_adjust(top=0.95, left=0.11, right=0.98, bottom=0.14)
    return save_figure(fig, "fig02_section_architecture", outdir)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--candil-sections-csv",
        type=Path,
        default=ROOT / "tmp" / "candil-recurring-section-kept-analysis" / "recurring_section_summary.csv",
    )
    parser.add_argument(
        "--candil-articles-csv",
        type=Path,
        default=ROOT / "tmp" / "candil-recurring-section-kept-analysis" / "article_footprint.csv",
    )
    parser.add_argument(
        "--jaleo-sections-csv",
        type=Path,
        default=ROOT / "reports" / "jaleo_recurring_sections_by_title" / "article_title_section_review.csv",
    )
    parser.add_argument(
        "--min-jaleo-section-count",
        type=int,
        default=2,
        help="Minimum full-corpus articles for a Jaleo section to count as recurring.",
    )
    parser.add_argument("--outdir", type=Path, default=ROOT / "figs")
    args = parser.parse_args()
    paths = plot(
        *build_data(
            args.candil_sections_csv,
            args.candil_articles_csv,
            args.jaleo_sections_csv,
            args.min_jaleo_section_count,
        ),
        args.outdir,
    )
    print(output_line(paths))


if __name__ == "__main__":
    main()
