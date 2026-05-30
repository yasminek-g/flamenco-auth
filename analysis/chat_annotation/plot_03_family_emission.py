"""Plot 03 -- family emission rate per periodical.

Two views:
  (a) share of articles emitting at least one code in the family
  (b) total emissions per 100 articles (intensity)
"""
from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from common import (
    NEUTRAL_MID,
    FAMILY_COLORS,
    FAMILY_ORDER,
    PERIODICAL_ORDER,
    load_articles,
    load_codes,
    output_line,
    save_figure,
    setup_theme,
)


def main() -> None:
    setup_theme()
    arts = load_articles()
    codes = load_codes()

    totals = arts.groupby("periodical_label")["article_id"].nunique()

    # (a) Share of articles where the family is emitted at least once
    fam_present = (
        codes.dropna(subset=["family"]).drop_duplicates(["article_id", "family"])
        .groupby(["periodical_label", "family"]).size().unstack(fill_value=0)
    )
    fam_share = fam_present.div(totals, axis=0)
    fam_share = fam_share.reindex(index=PERIODICAL_ORDER, columns=FAMILY_ORDER, fill_value=0)

    # (b) Emissions per 100 articles
    fam_emit = (
        codes.dropna(subset=["family"]).groupby(["periodical_label", "family"]).size()
        .unstack(fill_value=0)
    )
    fam_rate = fam_emit.div(totals, axis=0) * 100
    fam_rate = fam_rate.reindex(index=PERIODICAL_ORDER, columns=FAMILY_ORDER, fill_value=0)

    fig, axes = plt.subplots(1, 2, figsize=(14, 5.5))

    width = 0.38
    x = np.arange(len(FAMILY_ORDER))
    for ax, frame, title, ylabel, value_format in (
        (axes[0], fam_share, "Share of articles emitting family", "share of articles", "{:.0%}"),
        (axes[1], fam_rate, "Emissions per 100 articles", "emissions / 100 articles", "{:.0f}"),
    ):
        for i, p in enumerate(PERIODICAL_ORDER):
            vals = frame.loc[p].values
            offset = (i - 0.5) * width
            bars = ax.bar(
                x + offset,
                vals,
                width=width,
                label=p,
                color=[FAMILY_COLORS[c] for c in FAMILY_ORDER],
                edgecolor="white",
                linewidth=0.7,
                alpha=0.95 if p == "Candil" else 0.55,
                hatch="" if p == "Candil" else "//",
            )
            for rect, v in zip(bars, vals):
                if v > 0:
                    ax.text(
                        rect.get_x() + rect.get_width() / 2,
                        rect.get_height() + (0.01 * frame.values.max()),
                        value_format.format(v),
                        ha="center",
                        va="bottom",
                        fontsize=9,
                    )
        ax.set_xticks(x)
        ax.set_xticklabels(FAMILY_ORDER)
        ax.set_title(title)
        ax.set_ylabel(ylabel)
        ax.set_xlabel("")
        if frame is fam_share:
            ax.set_ylim(0, max(0.05, frame.values.max() * 1.18))
        else:
            ax.set_ylim(0, frame.values.max() * 1.18)
    handles = [
        plt.Rectangle((0, 0), 1, 1, facecolor=NEUTRAL_MID, alpha=0.95),
        plt.Rectangle((0, 0), 1, 1, facecolor=NEUTRAL_MID, alpha=0.55, hatch="//"),
    ]
    axes[0].legend(handles, PERIODICAL_ORDER, frameon=False, loc="upper right")
    fig.suptitle("Family-level emission profile", y=1.02)

    paths = save_figure(fig, "plot_03_family_emission")
    print(output_line(paths))


if __name__ == "__main__":
    main()
