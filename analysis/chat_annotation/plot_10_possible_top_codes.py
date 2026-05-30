"""Plot 10 -- top "considered but not emitted" codes per periodical.

How often a code shows up in `possible_but_not_emitted` is a proxy for
near-miss frequency: codes the annotator *considered* but ultimately
rejected. Bars show counts; colour shows code family.
"""
from __future__ import annotations

import matplotlib.pyplot as plt
import seaborn as sns

from common import (
    NEUTRAL_MID,
    FAMILY_COLORS,
    FAMILY_ORDER,
    PERIODICAL_ORDER,
    load_possible,
    output_line,
    save_figure,
    setup_theme,
)


def main() -> None:
    setup_theme()
    df = load_possible().copy()
    df["family"] = df["family"].fillna("?")
    df = df[df["code"].notna() & df["code"].str.match(r"^[A-Z]+_\d{2}$")]

    top = (
        df.groupby(["periodical_label", "code"]).size().reset_index(name="count")
    )
    top["family"] = top["code"].str.split("_").str[0]

    fig, axes = plt.subplots(1, 2, figsize=(13, 6.5), sharex=True)
    for ax, periodical in zip(axes, PERIODICAL_ORDER):
        sub = top[top["periodical_label"] == periodical].sort_values("count", ascending=True).tail(15)
        colors = [FAMILY_COLORS.get(f, NEUTRAL_MID) for f in sub["family"]]
        ax.barh(sub["code"], sub["count"], color=colors, edgecolor="white", linewidth=0.6)
        for y, v in enumerate(sub["count"]):
            ax.text(v + max(sub["count"]) * 0.01, y, str(int(v)), va="center", fontsize=9)
        ax.set_title(f"{periodical}: top near-miss codes")
        ax.set_xlabel("# `possible_but_not_emitted` mentions")
        ax.set_ylabel("")

    handles = [plt.Rectangle((0, 0), 1, 1, color=FAMILY_COLORS[f]) for f in FAMILY_ORDER]
    fig.legend(handles, FAMILY_ORDER, loc="upper center", ncol=5, frameon=False, bbox_to_anchor=(0.5, 1.02))
    paths = save_figure(fig, "plot_10_possible_top_codes")
    print(output_line(paths))


if __name__ == "__main__":
    main()
