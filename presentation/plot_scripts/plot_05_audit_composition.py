from __future__ import annotations

import argparse
from pathlib import Path

from common import INK, INK_2, INK_3, PAPER, PERIODICAL_PALETTE, RED, ROOT, RULE, output_line, read_csv, save_figure, setup_theme
from critical_common import GOLD_CSV, sources_text


SOURCES = [GOLD_CSV]


def _framed_legend(ax, **kwargs):
    legend = ax.legend(frameon=True, framealpha=0.95, **kwargs)
    legend.get_frame().set_facecolor(PAPER)
    legend.get_frame().set_edgecolor(RULE)
    return legend


def plot(outdir: Path | None = None) -> list[Path]:
    import matplotlib.pyplot as plt
    import seaborn as sns

    setup_theme()
    # Full audit (n = 180): the inclusion panel needs the excluded articles too.
    gold = read_csv(GOLD_CSV)
    gold["periodical"] = gold["periodical"].str.title()
    period = gold.groupby(["periodical", "period_bin"]).size().reset_index(name="articles")
    included = gold.groupby(["periodical", "final_include_for_metrics"]).size().reset_index(name="articles")

    fig, axes = plt.subplots(1, 2, figsize=(13.5, 5.6), gridspec_kw={"width_ratios": [1.35, 1]})
    sns.barplot(
        data=period,
        x="period_bin",
        y="articles",
        hue="periodical",
        palette=PERIODICAL_PALETTE,
        ax=axes[0],
    )
    axes[0].set_xlabel("")
    axes[0].set_ylabel("Hand-coded articles")
    axes[0].tick_params(axis="x", rotation=25, labelsize=9)
    _framed_legend(axes[0], title="", loc="upper right")

    sns.barplot(
        data=included,
        x="periodical",
        y="articles",
        hue="final_include_for_metrics",
        hue_order=["yes", "no"],
        palette={"yes": RED, "no": INK_2},
        ax=axes[1],
    )
    axes[1].set_xlabel("")
    axes[1].set_ylabel("Hand-coded articles")
    axes[1].set_ylim(0, 100)
    _framed_legend(axes[1], title="LLM-compare", loc="center")
    for ax in axes:
        for container in ax.containers:
            for bar in container:
                height = bar.get_height()
                if not height or height != height or height <= 0:
                    continue
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    height + 1.0,
                    f"{int(height)}",
                    va="bottom",
                    ha="center",
                    fontsize=8,
                    color=INK,
                )

    fig.subplots_adjust(top=0.93, wspace=0.32)
    return save_figure(fig, "fig05_audit_composition_fixed", outdir)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--outdir", type=Path, default=ROOT / "figs")
    args = parser.parse_args()
    paths = plot(args.outdir)
    print(sources_text(SOURCES))
    print(output_line(paths))


if __name__ == "__main__":
    main()
