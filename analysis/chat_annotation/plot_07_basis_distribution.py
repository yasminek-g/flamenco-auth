"""Plot 07 -- legitimation `basis` distribution per periodical.

`basis` is multi-valued, so we show:
  (a) total tag rate (basis tags per coded article)
  (b) share of articles touching each basis at least once.
"""
from __future__ import annotations

import matplotlib.pyplot as plt
import seaborn as sns

from common import (
    BASIS_ORDER,
    PERIODICAL_ORDER,
    PERIODICAL_PALETTE,
    load_articles,
    load_basis,
    output_line,
    save_figure,
    setup_theme,
)


def main() -> None:
    setup_theme()
    basis = load_basis()
    basis = basis[basis["basis"].isin(BASIS_ORDER)]
    arts = load_articles()
    totals = arts.groupby("periodical_label")["article_id"].nunique()

    article_basis = basis.drop_duplicates(["article_id", "basis"]).groupby(
        ["periodical_label", "basis"]
    ).size().unstack(fill_value=0).reindex(index=PERIODICAL_ORDER, columns=BASIS_ORDER, fill_value=0)
    article_share = article_basis.div(totals, axis=0)

    tag_counts = basis.groupby(["periodical_label", "basis"]).size().unstack(fill_value=0).reindex(
        index=PERIODICAL_ORDER, columns=BASIS_ORDER, fill_value=0
    )
    tag_rate = tag_counts.div(totals, axis=0)

    fig, axes = plt.subplots(1, 2, figsize=(14, 5.5))

    long_share = article_share.reset_index().melt(id_vars="periodical_label", var_name="basis", value_name="share")
    sns.barplot(
        data=long_share,
        x="basis",
        y="share",
        hue="periodical_label",
        order=BASIS_ORDER,
        hue_order=PERIODICAL_ORDER,
        palette=PERIODICAL_PALETTE,
        ax=axes[0],
    )
    axes[0].set_title("Share of articles touching each basis")
    axes[0].set_ylabel("share of articles")
    axes[0].set_xlabel("")
    axes[0].set_ylim(0, max(0.05, long_share["share"].max() * 1.18))
    for container in axes[0].containers:
        axes[0].bar_label(container, fmt="%.0f%%", labels=[f"{v*100:.0f}%" for v in container.datavalues], padding=2, fontsize=8)
    axes[0].legend(title="", frameon=False, loc="upper right")

    long_rate = tag_rate.reset_index().melt(id_vars="periodical_label", var_name="basis", value_name="rate")
    sns.barplot(
        data=long_rate,
        x="basis",
        y="rate",
        hue="periodical_label",
        order=BASIS_ORDER,
        hue_order=PERIODICAL_ORDER,
        palette=PERIODICAL_PALETTE,
        ax=axes[1],
    )
    axes[1].set_title("Basis tags per article (incl. multi-tag)")
    axes[1].set_ylabel("tags / article")
    axes[1].set_xlabel("")
    axes[1].set_ylim(0, max(0.05, long_rate["rate"].max() * 1.18))
    axes[1].legend_.remove()

    for ax in axes:
        for label in ax.get_xticklabels():
            label.set_rotation(20)
            label.set_ha("right")

    fig.suptitle("Legitimation basis tags per periodical", y=1.02)
    paths = save_figure(fig, "plot_07_basis_distribution")
    print(output_line(paths))


if __name__ == "__main__":
    main()
