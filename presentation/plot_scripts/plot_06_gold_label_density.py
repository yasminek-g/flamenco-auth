from __future__ import annotations

import argparse
from pathlib import Path

from common import INK, PERIODICAL_PALETTE, ROOT, output_line, save_figure, setup_theme
from critical_common import GOLD_CSV, metric_gold, sources_text


SOURCES = [GOLD_CSV]


def plot(outdir: Path | None = None) -> list[Path]:
    import matplotlib.pyplot as plt
    import seaborn as sns

    setup_theme()
    gold = metric_gold()
    fig, axes = plt.subplots(1, 2, figsize=(11.5, 5.5), sharey=True)
    for ax, y, ylabel in [
        (axes[0], "gold_code_count", "Gold codes per article"),
        (axes[1], "gold_family_count", "Gold families per article"),
    ]:
        sns.boxplot(data=gold, x="periodical", y=y, palette=PERIODICAL_PALETTE, width=0.42, ax=ax)
        sns.stripplot(data=gold, x="periodical", y=y, color=INK, alpha=0.45, size=3, jitter=0.18, ax=ax)
        ax.set_xlabel("")
        ax.set_ylabel(ylabel)
    fig.subplots_adjust(top=0.95, wspace=0.22)
    return save_figure(fig, "fig06_gold_label_density", outdir)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--outdir", type=Path, default=ROOT / "figs")
    args = parser.parse_args()
    paths = plot(args.outdir)
    print(sources_text(SOURCES))
    print(output_line(paths))


if __name__ == "__main__":
    main()
