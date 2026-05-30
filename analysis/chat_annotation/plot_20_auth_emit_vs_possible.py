"""Plot 20 -- AUTH near-miss vs emission per subcode and periodical.

Same idea as plot 11 but zoomed in on AUTH_01..AUTH_04. Tells us which
authenticity subtype the annotator hovered over without committing.
"""
from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from common import (
    AUTH_CODES,
    AUTH_LABELS,
    PERIODICAL_ORDER,
    PERIODICAL_PALETTE,
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
    joined = joined[joined["code"].isin(AUTH_CODES)]
    joined["near_miss_ratio"] = joined["possible"] / (joined["emit"] + joined["possible"]).replace(0, pd.NA)

    fig, axes = plt.subplots(1, 2, figsize=(14, 5.5))

    # Grouped bar: emit vs possible per code per periodical
    ax = axes[0]
    width = 0.38
    x = np.arange(len(AUTH_CODES))
    for i, periodical in enumerate(PERIODICAL_ORDER):
        sub = joined[joined["periodical_label"] == periodical].set_index("code").reindex(AUTH_CODES).fillna(0)
        offset = (i - 0.5) * width
        emit_bar = ax.bar(x + offset, sub["emit"], width=width * 0.45,
                          color=PERIODICAL_PALETTE[periodical], edgecolor="white", linewidth=0.6, label=f"{periodical} emit")
        possible_bar = ax.bar(x + offset + width * 0.45, sub["possible"], width=width * 0.45,
                              color=PERIODICAL_PALETTE[periodical], edgecolor="white", linewidth=0.6, alpha=0.45,
                              hatch="//", label=f"{periodical} possible")
        for b, v in zip(emit_bar, sub["emit"]):
            ax.text(b.get_x() + b.get_width() / 2, v + 1.5, f"{int(v)}", ha="center", fontsize=8)
        for b, v in zip(possible_bar, sub["possible"]):
            ax.text(b.get_x() + b.get_width() / 2, v + 1.5, f"{int(v)}", ha="center", fontsize=8)
    ax.set_xticks(x)
    ax.set_xticklabels([AUTH_LABELS[c] for c in AUTH_CODES], rotation=12, ha="right")
    ax.set_title("AUTH subcodes: emitted vs considered-not-emitted")
    ax.set_ylabel("count")
    ax.legend(frameon=False, fontsize=9, ncol=2, loc="upper right")

    # Near-miss ratio
    ax = axes[1]
    long = joined.pivot(index="code", columns="periodical_label", values="near_miss_ratio").reindex(AUTH_CODES)
    long.plot(kind="bar", ax=ax, color=[PERIODICAL_PALETTE[p] for p in long.columns], edgecolor="white", linewidth=0.6)
    for container in ax.containers:
        ax.bar_label(container, fmt="%.0f%%", labels=[f"{v*100:.0f}%" for v in container.datavalues], padding=2, fontsize=9)
    ax.set_xticklabels([AUTH_LABELS[c] for c in AUTH_CODES], rotation=12, ha="right")
    ax.set_title("near-miss ratio  =  possible / (emit + possible)")
    ax.set_xlabel("")
    ax.set_ylabel("share")
    ax.set_ylim(0, max(0.05, long.values.max() * 1.18))
    ax.legend(title="", frameon=False, fontsize=10)

    fig.suptitle("Where authenticity is hesitant -- AUTH emit vs possible", y=1.02)
    paths = save_figure(fig, "plot_20_auth_emit_vs_possible")
    print(output_line(paths))


if __name__ == "__main__":
    main()
