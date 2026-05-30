"""Plot 11 -- emit-vs-possible ratio per code.

Scatter: x = emissions, y = `possible_but_not_emitted` mentions, dot per
(code, periodical). Diagonal is `possible == emit` (a "balanced" code). Codes
above the diagonal are *more often considered than chosen* (high uncertainty
zone); codes below are confidently emitted.
"""
from __future__ import annotations

import matplotlib.pyplot as plt
import pandas as pd

from common import (
    NEUTRAL_DARK,
    NEUTRAL_LIGHT,
    FAMILY_COLORS,
    FAMILY_ORDER,
    PERIODICAL_ORDER,
    load_codes,
    load_possible,
    output_line,
    save_figure,
    setup_theme,
)


def main() -> None:
    setup_theme()
    emit = load_codes().groupby(["periodical_label", "code"]).size().reset_index(name="emit")
    possible = load_possible().groupby(["periodical_label", "code"]).size().reset_index(name="possible")
    joined = emit.merge(possible, on=["periodical_label", "code"], how="outer").fillna(0)
    joined = joined[joined["code"].str.match(r"^[A-Z]+_\d{2}$")]
    joined["family"] = joined["code"].str.split("_").str[0]
    joined["near_miss_ratio"] = joined["possible"] / (joined["emit"] + joined["possible"]).replace(0, pd.NA)

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    max_val = max(joined["emit"].max(), joined["possible"].max()) * 1.08

    for ax, periodical in zip(axes, PERIODICAL_ORDER):
        sub = joined[joined["periodical_label"] == periodical].copy()
        # Label only codes above a relevance threshold (top by emit+possible)
        sub["weight"] = sub["emit"] + sub["possible"]
        label_codes = set(sub.nlargest(10, "weight")["code"])
        for fam in FAMILY_ORDER:
            fam_sub = sub[sub["family"] == fam]
            ax.scatter(
                fam_sub["emit"],
                fam_sub["possible"],
                color=FAMILY_COLORS[fam],
                s=110,
                alpha=0.85,
                edgecolor="white",
                linewidth=0.8,
                label=fam,
            )
            for _, row in fam_sub.iterrows():
                if row["code"] not in label_codes:
                    continue
                ax.annotate(
                    row["code"],
                    (row["emit"], row["possible"]),
                    xytext=(7, 4),
                    textcoords="offset points",
                    fontsize=9,
                    color=NEUTRAL_DARK,
                )
        ax.plot([0, max_val], [0, max_val], color=NEUTRAL_LIGHT, linestyle="--", linewidth=1)
        ax.set_xlim(0, max_val)
        ax.set_ylim(0, max_val)
        ax.set_title(periodical)
        ax.set_xlabel("# emitted")
        ax.set_ylabel("# possible (not emitted)")
        if periodical == PERIODICAL_ORDER[0]:
            ax.legend(frameon=False, fontsize=10)

    fig.suptitle("Near-miss vs. emission per code (above diagonal = often considered, rarely chosen)", y=1.02)
    paths = save_figure(fig, "plot_11_emit_vs_possible")
    print(output_line(paths))

    # Companion table.
    out_csv = joined.sort_values(["periodical_label", "near_miss_ratio"], ascending=[True, False])
    out_csv.to_csv("analysis/chat_annotation/data/emit_vs_possible.csv", index=False)


if __name__ == "__main__":
    main()
