"""Plot 18 -- polarity & boundary signals within AUTH-emitting articles.

How does the derived analysis read when authenticity is on the table?
  * polarity distribution among AUTH-emitting articles vs. baseline
  * exclusion-boundary / right-to-define rates AUTH vs. non-AUTH coded
"""
from __future__ import annotations

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

from common import (
    OI_BLUE,
    OI_ORANGE,
    OI_SKY,
    OI_VERMILLION,
    PERIODICAL_ORDER,
    POLARITY_COLORS,
    POLARITY_ORDER,
    load_articles,
    load_codes,
    output_line,
    save_figure,
    setup_theme,
)


def main() -> None:
    setup_theme()
    arts = load_articles()
    codes = load_codes()
    auth_articles = set(codes[codes["code"].str.startswith("AUTH", na=False)]["article_id"])

    df = arts[arts["n_codes"] > 0].copy()
    df["has_auth"] = df["article_id"].isin(auth_articles)
    df["bucket"] = df["has_auth"].map({True: "AUTH-emitting", False: "non-AUTH coded"})

    pol_counts = (
        df.groupby(["periodical_label", "bucket", "polarity"]).size()
        .unstack("polarity", fill_value=0)
        .reindex(columns=POLARITY_ORDER, fill_value=0)
    )
    pol_share = pol_counts.div(pol_counts.sum(axis=1).replace(0, 1), axis=0)

    fig, axes = plt.subplots(1, 2, figsize=(17, 6), gridspec_kw={"wspace": 0.45})

    ax = axes[0]
    idx = pol_share.index.tolist()
    bottom = [0] * len(idx)
    labels = [f"{p}\n{b}" for p, b in idx]
    for level in POLARITY_ORDER:
        vals = pol_share[level].values
        ax.bar(range(len(idx)), vals, bottom=bottom, color=POLARITY_COLORS[level], edgecolor="white", linewidth=0.6, label=level)
        for i, (b, v) in enumerate(zip(bottom, vals)):
            if v > 0.05:
                ax.text(i, b + v / 2, f"{v:.0%}", ha="center", va="center", fontsize=9, color="white")
        bottom = [b + v for b, v in zip(bottom, vals)]
    ax.set_xticks(range(len(idx)))
    ax.set_xticklabels(labels, fontsize=10)
    ax.set_ylabel("share")
    ax.set_title("Polarity composition\nAUTH-emitting vs non-AUTH coded", fontsize=12)
    ax.legend(loc="lower right", frameon=False, fontsize=9, ncol=2)

    rows = []
    for col, label in [
        ("exclusion_boundary_present", "exclusion boundary"),
        ("right_to_define_present", "right-to-define"),
        ("legitimation_effect_present", "legitimation effect"),
    ]:
        grp = df.groupby(["periodical_label", "bucket"])[col].mean().reset_index()
        grp["flag"] = label
        grp = grp.rename(columns={col: "share"})
        rows.append(grp[["periodical_label", "bucket", "flag", "share"]])
    flag_summary = pd.concat(rows, ignore_index=True)
    flag_summary["facet"] = flag_summary["periodical_label"] + " -- " + flag_summary["bucket"]

    facet_order = [
        f"{p} -- {b}" for p in PERIODICAL_ORDER for b in ("AUTH-emitting", "non-AUTH coded")
    ]
    facet_palette = {
        f"{PERIODICAL_ORDER[0]} -- AUTH-emitting": OI_BLUE,
        f"{PERIODICAL_ORDER[0]} -- non-AUTH coded": OI_SKY,
        f"{PERIODICAL_ORDER[1]} -- AUTH-emitting": OI_VERMILLION,
        f"{PERIODICAL_ORDER[1]} -- non-AUTH coded": OI_ORANGE,
    }
    sns.barplot(
        data=flag_summary,
        y="flag",
        x="share",
        hue="facet",
        hue_order=facet_order,
        ax=axes[1],
        palette=facet_palette,
    )
    for container in axes[1].containers:
        axes[1].bar_label(container, fmt="%.0f%%", labels=[f"{v*100:.0f}%" for v in container.datavalues], padding=2, fontsize=8)
    axes[1].set_title("Boundary / legitimation flag rates\nshare of articles in each subgroup", fontsize=12)
    axes[1].set_xlabel("share of articles")
    axes[1].set_ylabel("")
    axes[1].set_xlim(0, 1)
    axes[1].legend(title="", frameon=False, loc="lower right", fontsize=8)

    fig.suptitle("Polarity & boundary signals when authenticity is on the table", y=1.02)
    paths = save_figure(fig, "plot_18_auth_polarity_boundaries")
    print(output_line(paths))


if __name__ == "__main__":
    main()
