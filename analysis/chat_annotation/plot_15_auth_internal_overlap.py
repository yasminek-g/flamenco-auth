"""Plot 15 -- within-article overlap among AUTH_01..AUTH_04.

Two views per periodical:
  * co-occurrence count matrix (lower triangle), diagonal = solo article count
  * Jaccard overlap (upper triangle) -- proportional overlap independent of
    base rate.
"""
from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

from common import (
    AUTH_CODES,
    AUTH_LABELS,
    PERIODICAL_ORDER,
    SEQUENTIAL_CMAP,
    load_codes,
    output_line,
    save_figure,
    setup_theme,
)


def main() -> None:
    setup_theme()
    df = load_codes()
    df = df[df["code"].isin(AUTH_CODES)]

    fig, axes = plt.subplots(1, 2, figsize=(16, 7.2))
    labels = [AUTH_LABELS[c] for c in AUTH_CODES]

    for ax, periodical in zip(axes, PERIODICAL_ORDER):
        sub = df[df["periodical_label"] == periodical]
        article_codes = sub.groupby("article_id")["code"].agg(set)

        counts = np.zeros((len(AUTH_CODES), len(AUTH_CODES)), dtype=int)
        for code_set in article_codes:
            for i, ci in enumerate(AUTH_CODES):
                for j, cj in enumerate(AUTH_CODES):
                    if ci in code_set and cj in code_set:
                        counts[i, j] += 1
        diag = np.diag(counts)

        # Jaccard
        jacc = np.zeros_like(counts, dtype=float)
        for i in range(len(AUTH_CODES)):
            for j in range(len(AUTH_CODES)):
                if i == j:
                    jacc[i, j] = 1.0
                    continue
                union = diag[i] + diag[j] - counts[i, j]
                jacc[i, j] = counts[i, j] / union if union else 0

        combined = np.zeros_like(jacc)
        annot = np.empty_like(jacc, dtype=object)
        for i in range(len(AUTH_CODES)):
            for j in range(len(AUTH_CODES)):
                if i == j:
                    combined[i, j] = 0.5  # mid-color so diagonal reads as a band
                    annot[i, j] = f"n={diag[i]}"
                elif i > j:
                    combined[i, j] = counts[i, j] / max(1, diag.max())
                    annot[i, j] = str(int(counts[i, j]))
                else:
                    combined[i, j] = jacc[i, j]
                    annot[i, j] = f"J={jacc[i, j]:.2f}"

        sns.heatmap(
            combined,
            xticklabels=labels,
            yticklabels=labels,
            annot=annot,
            fmt="",
            cmap=SEQUENTIAL_CMAP,
            ax=ax,
            cbar=False,
            linewidths=0.4,
            linecolor="white",
            vmin=0,
            vmax=1,
            annot_kws={"fontsize": 10},
        )
        ax.set_title(f"{periodical}\nlower=count   |   diag=#articles with code   |   upper=Jaccard", fontsize=11)
        ax.set_xlabel("")
        ax.set_ylabel("")
        for label in ax.get_xticklabels():
            label.set_rotation(25)
            label.set_ha("right")
        for label in ax.get_yticklabels():
            label.set_rotation(0)

    fig.tight_layout()

    fig.suptitle("Authenticity subtype overlap within articles", y=1.04)
    paths = save_figure(fig, "plot_15_auth_internal_overlap")
    print(output_line(paths))


if __name__ == "__main__":
    main()
