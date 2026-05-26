from __future__ import annotations

import argparse
from pathlib import Path

from common import INK, INK_3, PERIODICAL_PALETTE, ROOT, output_line, save_figure, setup_theme
from critical_common import CORE_FAMILIES, GOLD_CSV, explode_set, metric_gold, sources_text


SOURCES = [GOLD_CSV]


def plot(outdir: Path | None = None) -> list[Path]:
    import matplotlib.pyplot as plt
    import seaborn as sns

    setup_theme()
    # Human-gold-only finding: use the full hand-coded sample (n = 180), not the
    # LLM-comparison subset, since no LLM labels enter this chart.
    gold = metric_gold(metrics_only=False)
    exploded = explode_set(gold, "gold_families", "family")
    counts = exploded.groupby(["periodical", "family"]).size().reset_index(name="articles")
    totals = gold.groupby("periodical").size().rename("total").reset_index()
    counts = counts.merge(totals, on="periodical")
    counts["share"] = counts["articles"] / counts["total"] * 100
    counts = counts[counts["family"].isin(CORE_FAMILIES)]

    fig, ax = plt.subplots(figsize=(11.5, 5.8))
    sns.barplot(
        data=counts,
        x="family",
        y="share",
        hue="periodical",
        order=CORE_FAMILIES,
        palette=PERIODICAL_PALETTE,
        ax=ax,
    )
    ax.set_xlabel("")
    ax.set_ylabel("Share of human-gold sample (n = 180)")
    ax.yaxis.set_major_formatter(lambda y, _pos: f"{y:.0f}%")
    ax.legend(title="", frameon=False)
    for container in ax.containers:
        for bar in container:
            height = bar.get_height()
            if height != height:
                continue
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                height + 1.0,
                f"{height:.0f}%",
                va="bottom",
                ha="center",
                fontsize=8,
                color=INK,
            )
    fig.subplots_adjust(top=0.94)
    return save_figure(fig, "fig07_any_gold_family_distribution", outdir)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--outdir", type=Path, default=ROOT / "figs")
    args = parser.parse_args()
    paths = plot(args.outdir)
    print(sources_text(SOURCES))
    print(output_line(paths))


if __name__ == "__main__":
    main()
