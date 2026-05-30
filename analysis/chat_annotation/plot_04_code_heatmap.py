"""Plot 04 -- full code-level emission heatmap.

Rows: every concept code in the codebook order.
Columns: periodical.
Values: emissions per 100 articles (so the two periodicals are comparable).
"""
from __future__ import annotations

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

from common import (
    AUTH_CODES,
    FAMILY_ORDER,
    PERIODICAL_ORDER,
    SEQUENTIAL_CMAP,
    load_articles,
    load_codes,
    output_line,
    save_figure,
    setup_theme,
)


CODE_ORDER = [
    *AUTH_CODES,
    "CRIT_01", "CRIT_02", "CRIT_03", "CRIT_04",
    "COMM_01", "COMM_02", "COMM_03", "COMM_04",
    "PED_01", "PED_02", "PED_03",
    "HERIT_01", "HERIT_02", "HERIT_03",
]


def main() -> None:
    setup_theme()
    arts = load_articles()
    codes = load_codes()

    totals = arts.groupby("periodical_label")["article_id"].nunique()
    emit = (
        codes.groupby(["code", "periodical_label"]).size().unstack(fill_value=0)
    )
    rate = emit.div(totals, axis=1) * 100
    rate = rate.reindex(index=CODE_ORDER, columns=PERIODICAL_ORDER, fill_value=0)

    fig, ax = plt.subplots(figsize=(7, 9))
    sns.heatmap(
        rate,
        annot=True,
        fmt=".1f",
        cmap=SEQUENTIAL_CMAP,
        cbar_kws={"label": "emissions per 100 articles"},
        linewidths=0.4,
        linecolor="white",
        ax=ax,
    )
    ax.set_xlabel("")
    ax.set_ylabel("")
    ax.set_title("Code emissions per 100 articles", pad=12)

    # Visual divider between families.
    seen = []
    for i, c in enumerate(CODE_ORDER):
        fam = c.split("_")[0]
        if fam not in seen:
            seen.append(fam)
            if i > 0:
                ax.axhline(i, color="white", lw=2.5)
    paths = save_figure(fig, "plot_04_code_heatmap")
    print(output_line(paths))


if __name__ == "__main__":
    main()
