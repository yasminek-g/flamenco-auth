"""Plot 02 -- distribution of emitted codes per article.

Compares the per-article codebook footprint across the two periodicals.
Boxen + strip overlay so spread and density are both visible.
"""
from __future__ import annotations

import matplotlib.pyplot as plt
import seaborn as sns

from common import (
    NEUTRAL_DARK,
    PERIODICAL_ORDER,
    PERIODICAL_PALETTE,
    load_articles,
    output_line,
    save_figure,
    setup_theme,
)


def main() -> None:
    setup_theme()
    df = load_articles()
    df = df[(~df["no_relevant_discourse"]) & (~df["insufficient_context"])]

    fig, axes = plt.subplots(1, 2, figsize=(13, 5))

    sns.boxenplot(
        data=df,
        x="periodical_label",
        y="n_codes",
        order=PERIODICAL_ORDER,
        palette=PERIODICAL_PALETTE,
        ax=axes[0],
    )
    sns.stripplot(
        data=df,
        x="periodical_label",
        y="n_codes",
        order=PERIODICAL_ORDER,
        color="white",
        edgecolor=NEUTRAL_DARK,
        linewidth=0.4,
        size=2.2,
        alpha=0.45,
        jitter=0.28,
        ax=axes[0],
    )
    axes[0].set_title("Codes emitted per article")
    axes[0].set_xlabel("")
    axes[0].set_ylabel("# emitted codes")

    sns.histplot(
        data=df,
        x="n_codes",
        hue="periodical_label",
        multiple="dodge",
        discrete=True,
        palette=PERIODICAL_PALETTE,
        hue_order=PERIODICAL_ORDER,
        edgecolor="white",
        linewidth=0.6,
        ax=axes[1],
    )
    axes[1].set_title("Histogram of code count")
    axes[1].set_xlabel("# emitted codes")
    axes[1].set_ylabel("articles")
    axes[1].legend_.set_title("")

    means = df.groupby("periodical_label")["n_codes"].mean()
    medians = df.groupby("periodical_label")["n_codes"].median()
    summary = ", ".join(
        f"{p}: mean {means[p]:.2f}, median {medians[p]:.0f}" for p in PERIODICAL_ORDER
    )
    fig.suptitle(f"Codes per coded article -- {summary}", y=1.02)

    paths = save_figure(fig, "plot_02_codes_per_article")
    print(output_line(paths))


if __name__ == "__main__":
    main()
