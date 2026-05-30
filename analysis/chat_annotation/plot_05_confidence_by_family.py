"""Plot 05 -- confidence distribution per family x periodical.

Stacked-share bars (high / medium / low) plus emission count printed at the
bar end so reviewers see both proportion and base rate.
"""
from __future__ import annotations

import matplotlib.pyplot as plt
import pandas as pd

from common import (
    NEUTRAL_DARK,
    CONFIDENCE_COLORS,
    CONFIDENCE_ORDER,
    FAMILY_ORDER,
    PERIODICAL_ORDER,
    load_codes,
    output_line,
    save_figure,
    setup_theme,
)


def main() -> None:
    setup_theme()
    codes = load_codes()
    codes = codes.dropna(subset=["family"])
    codes["confidence"] = codes["confidence"].fillna("low")

    grouped = (
        codes.groupby(["periodical_label", "family", "confidence"]).size()
        .unstack("confidence", fill_value=0)
        .reindex(columns=CONFIDENCE_ORDER, fill_value=0)
    )
    totals = grouped.sum(axis=1)
    shares = grouped.div(totals.replace(0, pd.NA), axis=0).fillna(0)

    fig, axes = plt.subplots(1, 2, figsize=(14, 5.5), sharex=True)
    for ax, periodical in zip(axes, PERIODICAL_ORDER):
        sub = shares.xs(periodical, level="periodical_label").reindex(FAMILY_ORDER, fill_value=0)
        base = [0] * len(sub)
        for level in CONFIDENCE_ORDER:
            vals = sub[level].values
            ax.barh(
                sub.index,
                vals,
                left=base,
                color=CONFIDENCE_COLORS[level],
                edgecolor="white",
                linewidth=0.6,
                label=level,
            )
            for i, (b, v) in enumerate(zip(base, vals)):
                if v > 0.05:
                    ax.text(
                        b + v / 2,
                        i,
                        f"{v:.0%}",
                        ha="center",
                        va="center",
                        fontsize=10,
                        color="white",
                    )
            base = [b + v for b, v in zip(base, vals)]
        for i, fam in enumerate(sub.index):
            count = grouped.loc[(periodical, fam)].sum() if (periodical, fam) in grouped.index else 0
            ax.text(1.02, i, f"n={count}", va="center", fontsize=9, color=NEUTRAL_DARK)
        ax.set_xlim(0, 1.13)
        ax.invert_yaxis()
        ax.set_title(periodical)
        ax.set_xlabel("share of emissions")
        ax.set_ylabel("")
    axes[0].legend(loc="lower right", frameon=False, fontsize=10, title="confidence", title_fontsize=10)
    fig.suptitle("Confidence distribution by family", y=1.02)

    paths = save_figure(fig, "plot_05_confidence_by_family")
    print(output_line(paths))


if __name__ == "__main__":
    main()
