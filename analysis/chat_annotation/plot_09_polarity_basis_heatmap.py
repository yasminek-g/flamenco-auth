"""Plot 09 -- polarity x basis co-occurrence heatmap per periodical.

Cell value = number of (article, basis) pairs that landed in each polarity.
A row-normalised version (share within polarity) lives in the lower row of
the figure so reviewers can read both volume and within-row composition.
"""
from __future__ import annotations

import matplotlib.pyplot as plt
import seaborn as sns

from common import (
    BASIS_ORDER,
    PERIODICAL_ORDER,
    POLARITY_ORDER,
    SEQUENTIAL_CMAP,
    load_basis,
    output_line,
    save_figure,
    setup_theme,
)


def main() -> None:
    setup_theme()
    df = load_basis().copy()
    df = df[df["basis"].isin(BASIS_ORDER) & df["polarity"].notna()]

    fig, axes = plt.subplots(2, 2, figsize=(13, 9), sharex="col")
    for col, periodical in enumerate(PERIODICAL_ORDER):
        sub = df[df["periodical_label"] == periodical]
        pivot = (
            sub.groupby(["polarity", "basis"]).size().unstack(fill_value=0)
            .reindex(index=POLARITY_ORDER, columns=BASIS_ORDER, fill_value=0)
        )
        sns.heatmap(
            pivot,
            annot=True,
            fmt="d",
            cmap=SEQUENTIAL_CMAP,
            ax=axes[0, col],
            cbar_kws={"label": "count" if col == 1 else None},
            linewidths=0.4,
            linecolor="white",
        )
        axes[0, col].set_title(f"{periodical} -- counts")
        axes[0, col].set_xlabel("")
        axes[0, col].set_ylabel("polarity" if col == 0 else "")

        norm = pivot.div(pivot.sum(axis=1).replace(0, 1), axis=0)
        sns.heatmap(
            norm,
            annot=True,
            fmt=".0%",
            cmap=SEQUENTIAL_CMAP,
            ax=axes[1, col],
            cbar_kws={"label": "share within polarity" if col == 1 else None},
            linewidths=0.4,
            linecolor="white",
            vmin=0,
            vmax=1,
        )
        axes[1, col].set_title(f"{periodical} -- share within polarity")
        axes[1, col].set_xlabel("basis")
        axes[1, col].set_ylabel("polarity" if col == 0 else "")
        for label in axes[1, col].get_xticklabels():
            label.set_rotation(25)
            label.set_ha("right")

    fig.suptitle("Polarity x basis co-occurrence", y=1.01)
    paths = save_figure(fig, "plot_09_polarity_basis_heatmap")
    print(output_line(paths))


if __name__ == "__main__":
    main()
