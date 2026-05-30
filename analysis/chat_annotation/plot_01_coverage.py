"""Plot 01 -- annotation coverage and decision counts per periodical.

Stacked bar of three article-level decisions:
  * coded (>=1 emitted code)
  * no_relevant_discourse (annotator marked nothing applicable)
  * insufficient_context (passage too short / OCR damaged)
"""
from __future__ import annotations

import matplotlib.pyplot as plt
import pandas as pd

from common import (
    NEUTRAL_LIGHT,
    NEUTRAL_MID,
    OI_BLUE,
    OI_ORANGE,
    PERIODICAL_ORDER,
    load_articles,
    output_line,
    save_figure,
    setup_theme,
)


def classify(row: pd.Series) -> str:
    if row["insufficient_context"]:
        return "insufficient context"
    if row["no_relevant_discourse"]:
        return "no relevant discourse"
    if row["n_codes"] > 0:
        return "coded (>=1 code)"
    return "no codes / no flag"


CATEGORY_ORDER = [
    "coded (>=1 code)",
    "no relevant discourse",
    "insufficient context",
    "no codes / no flag",
]
CATEGORY_COLORS = {
    "coded (>=1 code)": OI_BLUE,
    "no relevant discourse": NEUTRAL_MID,
    "insufficient context": OI_ORANGE,
    "no codes / no flag": NEUTRAL_LIGHT,
}


def main() -> None:
    setup_theme()
    df = load_articles()
    df["category"] = df.apply(classify, axis=1)

    counts = (
        df.groupby(["periodical_label", "category"]).size().unstack(fill_value=0)
        .reindex(index=PERIODICAL_ORDER, columns=CATEGORY_ORDER, fill_value=0)
    )
    shares = counts.div(counts.sum(axis=1), axis=0)

    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    counts.plot(
        kind="barh",
        stacked=True,
        ax=axes[0],
        color=[CATEGORY_COLORS[c] for c in counts.columns],
        edgecolor="white",
        linewidth=0.6,
    )
    axes[0].set_title("Articles annotated, by decision")
    axes[0].set_xlabel("articles")
    axes[0].set_ylabel("")
    axes[0].invert_yaxis()
    axes[0].legend(loc="lower right", frameon=False, fontsize=10)

    shares.plot(
        kind="barh",
        stacked=True,
        ax=axes[1],
        color=[CATEGORY_COLORS[c] for c in shares.columns],
        edgecolor="white",
        linewidth=0.6,
        legend=False,
    )
    axes[1].set_title("Share of decisions per periodical")
    axes[1].set_xlabel("share")
    axes[1].set_ylabel("")
    axes[1].invert_yaxis()
    axes[1].set_xlim(0, 1)
    for container, share_row in zip(axes[1].containers, shares.T.values):
        for rect, value in zip(container, share_row):
            if value > 0.04:
                axes[1].text(
                    rect.get_x() + rect.get_width() / 2,
                    rect.get_y() + rect.get_height() / 2,
                    f"{value:.0%}",
                    ha="center",
                    va="center",
                    color="white",
                    fontsize=10,
                )

    fig.suptitle("Chat-annotation coverage by periodical", y=1.02)
    paths = save_figure(fig, "plot_01_coverage")
    print(output_line(paths))


if __name__ == "__main__":
    main()
