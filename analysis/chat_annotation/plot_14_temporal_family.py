"""Plot 14 -- family emission share by year, per periodical.

Stacked area showing how the discursive mix shifts over the run of each
magazine. Bucketed by issue year.
"""
from __future__ import annotations

import matplotlib.pyplot as plt
import pandas as pd

from common import (
    NEUTRAL,
    FAMILY_COLORS,
    FAMILY_ORDER,
    PERIODICAL_ORDER,
    load_codes,
    output_line,
    save_figure,
    setup_theme,
)


def main() -> None:
    setup_theme()
    df = load_codes().dropna(subset=["family", "year"]).copy()
    df["year"] = df["year"].astype(int)

    fig, axes = plt.subplots(1, 2, figsize=(15, 5.2), sharey=True)
    for ax, periodical in zip(axes, PERIODICAL_ORDER):
        sub = df[df["periodical_label"] == periodical]
        pivot = (
            sub.groupby(["year", "family"]).size().unstack("family", fill_value=0)
            .reindex(columns=FAMILY_ORDER, fill_value=0)
        )
        if pivot.empty:
            ax.set_title(f"{periodical} (no data)")
            continue
        totals = pivot.sum(axis=1).replace(0, pd.NA)
        shares = pivot.div(totals, axis=0).fillna(0)
        shares.plot.area(
            ax=ax,
            color=[FAMILY_COLORS[f] for f in FAMILY_ORDER],
            alpha=0.92,
            linewidth=0,
        )
        ax.set_title(f"{periodical}")
        ax.set_xlabel("issue year")
        ax.set_ylabel("share of emissions" if periodical == "Candil" else "")
        ax.set_ylim(0, 1)
        ax.legend(loc="lower right", frameon=False, fontsize=9, ncol=2)
        # Year-bucket sample sizes printed across the top
        for year, n in pivot.sum(axis=1).items():
            ax.text(year, 1.015, f"{int(n)}", ha="center", fontsize=7, color=NEUTRAL)

    fig.suptitle("Family-share over time (emissions per year)", y=1.04)
    paths = save_figure(fig, "plot_14_temporal_family")
    print(output_line(paths))


if __name__ == "__main__":
    main()
