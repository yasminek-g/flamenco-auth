"""Plot 16 -- AUTH x other-family co-occurrence at the article level.

For each non-AUTH family, computes the share of AUTH-emitting articles that
also emit that family, and the symmetric Jaccard. Tells us which discourses
authenticity travels with.
"""
from __future__ import annotations

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

from common import (
    FAMILY_ORDER,
    PERIODICAL_ORDER,
    PERIODICAL_PALETTE,
    load_codes,
    output_line,
    save_figure,
    setup_theme,
)


OTHER_FAMILIES = [f for f in FAMILY_ORDER if f != "AUTH"]


def main() -> None:
    setup_theme()
    codes = load_codes().dropna(subset=["family"]).copy()

    rows = []
    for periodical in PERIODICAL_ORDER:
        sub = codes[codes["periodical_label"] == periodical]
        fams_per_article = sub.groupby("article_id")["family"].agg(set)
        auth_articles = {aid for aid, fams in fams_per_article.items() if "AUTH" in fams}
        for fam in OTHER_FAMILIES:
            fam_articles = {aid for aid, fams in fams_per_article.items() if fam in fams}
            both = len(auth_articles & fam_articles)
            union = len(auth_articles | fam_articles)
            cond = both / len(auth_articles) if auth_articles else 0
            cond_rev = both / len(fam_articles) if fam_articles else 0
            jacc = both / union if union else 0
            rows.append(
                {
                    "periodical_label": periodical,
                    "family": fam,
                    "p_fam_given_auth": cond,
                    "p_auth_given_fam": cond_rev,
                    "jaccard": jacc,
                    "n_auth": len(auth_articles),
                    "n_fam": len(fam_articles),
                    "n_both": both,
                }
            )
    summary = pd.DataFrame(rows)
    summary.to_csv("analysis/chat_annotation/data/auth_family_overlap.csv", index=False)

    fig, axes = plt.subplots(1, 3, figsize=(15, 4.5))
    metric_specs = [
        ("p_fam_given_auth", "P(family | AUTH)\n(AUTH article also emits family)"),
        ("p_auth_given_fam", "P(AUTH | family)\n(family article also emits AUTH)"),
        ("jaccard", "Jaccard overlap\n(symmetric)"),
    ]
    for ax, (metric, title) in zip(axes, metric_specs):
        sns.barplot(
            data=summary,
            x="family",
            y=metric,
            hue="periodical_label",
            order=OTHER_FAMILIES,
            hue_order=PERIODICAL_ORDER,
            palette=PERIODICAL_PALETTE,
            ax=ax,
        )
        for container in ax.containers:
            ax.bar_label(container, fmt="%.0f%%", labels=[f"{v*100:.0f}%" for v in container.datavalues], padding=2, fontsize=9)
        ax.set_title(title, fontsize=12)
        ax.set_xlabel("")
        ax.set_ylabel("")
        ax.set_ylim(0, max(0.05, summary[metric].max() * 1.18))
        if ax is axes[-1]:
            ax.legend(title="", frameon=False, loc="upper right", fontsize=9)
        else:
            if ax.legend_:
                ax.legend_.remove()

    fig.suptitle("Authenticity overlap with other discourse families", y=1.04)
    paths = save_figure(fig, "plot_16_auth_family_overlap")
    print(output_line(paths))


if __name__ == "__main__":
    main()
