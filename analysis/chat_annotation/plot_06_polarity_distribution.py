"""Plot 06 -- derived polarity distribution per periodical."""
from __future__ import annotations

import matplotlib.pyplot as plt
import pandas as pd

from common import (
    PERIODICAL_ORDER,
    POLARITY_COLORS,
    POLARITY_ORDER,
    load_articles,
    output_line,
    save_figure,
    setup_theme,
)


def main() -> None:
    setup_theme()
    df = load_articles().copy()
    df = df[~df["insufficient_context"]]
    df["polarity"] = df["polarity"].fillna("unclear")

    counts = (
        df.groupby(["periodical_label", "polarity"]).size().unstack("polarity", fill_value=0)
        .reindex(index=PERIODICAL_ORDER, columns=POLARITY_ORDER, fill_value=0)
    )
    shares = counts.div(counts.sum(axis=1), axis=0)

    fig, axes = plt.subplots(1, 2, figsize=(14, 4.5))
    counts.plot(
        kind="barh",
        stacked=True,
        ax=axes[0],
        color=[POLARITY_COLORS[c] for c in counts.columns],
        edgecolor="white",
        linewidth=0.6,
    )
    axes[0].set_title("Polarity counts")
    axes[0].set_xlabel("articles")
    axes[0].set_ylabel("")
    axes[0].invert_yaxis()
    axes[0].legend(loc="lower right", frameon=False, fontsize=9)

    shares.plot(
        kind="barh",
        stacked=True,
        ax=axes[1],
        color=[POLARITY_COLORS[c] for c in shares.columns],
        edgecolor="white",
        linewidth=0.6,
        legend=False,
    )
    axes[1].set_title("Polarity shares")
    axes[1].set_xlabel("share")
    axes[1].set_ylabel("")
    axes[1].invert_yaxis()
    axes[1].set_xlim(0, 1)
    for container, share_row in zip(axes[1].containers, shares.T.values):
        for rect, value in zip(container, share_row):
            if value > 0.05:
                axes[1].text(
                    rect.get_x() + rect.get_width() / 2,
                    rect.get_y() + rect.get_height() / 2,
                    f"{value:.0%}",
                    ha="center",
                    va="center",
                    color="white",
                    fontsize=9,
                )

    fig.suptitle("Derived polarity by periodical", y=1.02)
    paths = save_figure(fig, "plot_06_polarity_distribution")
    print(output_line(paths))


if __name__ == "__main__":
    main()
