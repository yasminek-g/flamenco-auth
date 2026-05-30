"""Plot 12 -- rationale & evidence length by confidence.

Pairs:
  * left  -- rationale word count by confidence x periodical
  * right -- evidence-quote word count by confidence x periodical
A useful sanity check: do high-confidence codes carry shorter, sharper
evidence than medium-confidence ones?
"""
from __future__ import annotations

import matplotlib.pyplot as plt
import seaborn as sns

from common import (
    CONFIDENCE_ORDER,
    PERIODICAL_ORDER,
    PERIODICAL_PALETTE,
    load_codes,
    output_line,
    save_figure,
    setup_theme,
)


def main() -> None:
    setup_theme()
    df = load_codes().copy()
    df = df[df["confidence"].isin(CONFIDENCE_ORDER)]

    fig, axes = plt.subplots(1, 2, figsize=(14, 5.5))
    metrics = [
        ("rationale_words", "rationale words"),
        ("evidence_quote_words", "evidence-quote words"),
    ]
    for ax, (col, label) in zip(axes, metrics):
        sns.violinplot(
            data=df,
            x="confidence",
            y=col,
            hue="periodical_label",
            order=CONFIDENCE_ORDER,
            hue_order=PERIODICAL_ORDER,
            palette=PERIODICAL_PALETTE,
            split=True,
            inner="quartile",
            cut=0,
            ax=ax,
        )
        ax.set_title(label)
        ax.set_xlabel("")
        ax.set_ylabel(label)
        if ax is axes[1]:
            ax.legend_.remove()
        else:
            ax.legend(title="", frameon=False, loc="upper right")

    fig.suptitle("Reasoning surface vs. confidence", y=1.02)
    paths = save_figure(fig, "plot_12_rationale_length")
    print(output_line(paths))


if __name__ == "__main__":
    main()
