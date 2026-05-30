"""Plot 08 -- legitimation effect, exclusion boundary, right-to-define rates.

Three article-level Boolean flags from `derived_analysis` summarised as the
share of *coded* articles where each flag is true.
"""
from __future__ import annotations

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

from common import (
    PERIODICAL_ORDER,
    PERIODICAL_PALETTE,
    load_articles,
    output_line,
    save_figure,
    setup_theme,
)


FLAGS = [
    ("legitimation_effect_present", "legitimation effect present"),
    ("exclusion_boundary_present", "exclusion boundary present"),
    ("right_to_define_present", "right-to-define present"),
]


def main() -> None:
    setup_theme()
    df = load_articles()
    df = df[(df["n_codes"] > 0) & (~df["insufficient_context"])]

    rows = []
    for col, label in FLAGS:
        per = df.groupby("periodical_label")[col].mean()
        for periodical, value in per.items():
            rows.append({"flag": label, "periodical_label": periodical, "share": value})
    summary = pd.DataFrame(rows)

    fig, ax = plt.subplots(figsize=(11, 4.7))
    sns.barplot(
        data=summary,
        y="flag",
        x="share",
        hue="periodical_label",
        hue_order=PERIODICAL_ORDER,
        palette=PERIODICAL_PALETTE,
        ax=ax,
    )
    ax.set_title("Derived-analysis flags (share of coded articles)")
    ax.set_xlabel("share of coded articles")
    ax.set_ylabel("")
    ax.set_xlim(0, 1)
    for container in ax.containers:
        ax.bar_label(container, fmt="%.0f%%", labels=[f"{v*100:.0f}%" for v in container.datavalues], padding=3, fontsize=10)
    ax.legend(title="", frameon=False, loc="lower right")
    paths = save_figure(fig, "plot_08_legitimation_and_boundaries")
    print(output_line(paths))


if __name__ == "__main__":
    main()
