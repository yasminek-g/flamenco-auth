"""Plot 19 -- AUTH share and subtype mix over time.

Per-periodical:
  * line: share of coded articles emitting AUTH (any subtype) by year
  * stacked area: composition of AUTH_01..AUTH_04 within AUTH emissions by year
"""
from __future__ import annotations

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

from common import (
    AUTH_CODES,
    AUTH_LABELS,
    AUTH_SUBCODE_COLORS,
    NEUTRAL,
    PERIODICAL_ORDER,
    PERIODICAL_PALETTE,
    load_articles,
    load_codes,
    output_line,
    save_figure,
    setup_theme,
)


SUBCODE_COLORS = AUTH_SUBCODE_COLORS


def main() -> None:
    setup_theme()
    arts = load_articles().copy()
    codes = load_codes().copy()
    arts["year"] = arts["year"].astype("Int64")
    codes["year"] = codes["year"].astype("Int64")

    fig, axes = plt.subplots(2, 2, figsize=(16, 9.5))
    for col, periodical in enumerate(PERIODICAL_ORDER):
        # Share of coded articles emitting AUTH per year
        sub_arts = arts[(arts["periodical_label"] == periodical) & (arts["n_codes"] > 0)]
        auth_ids = set(codes[(codes["periodical_label"] == periodical) & codes["code"].str.startswith("AUTH", na=False)]["article_id"])
        sub_arts = sub_arts.assign(has_auth=sub_arts["article_id"].isin(auth_ids))
        by_year = sub_arts.dropna(subset=["year"]).copy()
        by_year["year"] = by_year["year"].astype(int)
        by_year = by_year.groupby("year").agg(share=("has_auth", "mean"), n=("article_id", "nunique"))

        ax_top = axes[0, col]
        ax_top.plot(by_year.index, by_year["share"], marker="o", color=PERIODICAL_PALETTE[periodical], linewidth=2)
        ax_top.fill_between(by_year.index, by_year["share"], color=PERIODICAL_PALETTE[periodical], alpha=0.18)
        ax_top.set_ylim(0, 1.05)
        ax_top.xaxis.set_major_locator(mticker.MaxNLocator(integer=True, nbins=8))
        ax_top.set_title(f"{periodical}: share of coded articles emitting AUTH", fontsize=12)
        ax_top.set_ylabel("share" if col == 0 else "")
        ax_top.set_xlabel("")
        for x, (s, n) in zip(by_year.index, zip(by_year["share"], by_year["n"])):
            ax_top.text(x, s + 0.03, f"{int(n)}", ha="center", fontsize=7, color=NEUTRAL)

        # AUTH subtype share per year
        ax_bot = axes[1, col]
        sub_codes = codes[(codes["periodical_label"] == periodical) & codes["code"].isin(AUTH_CODES)].copy()
        sub_codes = sub_codes.dropna(subset=["year"])
        sub_codes["year"] = sub_codes["year"].astype(int)
        pivot = sub_codes.groupby(["year", "code"]).size().unstack("code", fill_value=0).reindex(columns=AUTH_CODES, fill_value=0)
        if pivot.empty:
            ax_bot.text(0.5, 0.5, "(no data)", transform=ax_bot.transAxes, ha="center", va="center")
            continue
        totals = pivot.sum(axis=1).replace(0, pd.NA)
        share = pivot.div(totals, axis=0).fillna(0)
        share.plot.area(
            ax=ax_bot,
            color=[SUBCODE_COLORS[c] for c in AUTH_CODES],
            alpha=0.92,
            linewidth=0,
        )
        ax_bot.set_ylim(0, 1)
        ax_bot.set_xlim(pivot.index.min(), pivot.index.max())
        ax_bot.xaxis.set_major_locator(mticker.MaxNLocator(integer=True, nbins=8))
        ax_bot.set_title(f"{periodical}: AUTH subtype mix", fontsize=12)
        ax_bot.set_xlabel("issue year")
        ax_bot.set_ylabel("share within AUTH emissions" if col == 0 else "")
        if col == 1:
            ax_bot.legend(
                [f"{c}  {AUTH_LABELS[c]}" for c in AUTH_CODES],
                loc="center left",
                bbox_to_anchor=(1.02, 0.5),
                frameon=False,
                fontsize=9,
            )
        else:
            ax_bot.get_legend().remove() if ax_bot.get_legend() else None

    fig.suptitle("Authenticity over time (top: emission rate, with per-year coded count; bottom: subtype mix)", y=1.00)
    fig.tight_layout()
    paths = save_figure(fig, "plot_19_auth_temporal")
    print(output_line(paths))


if __name__ == "__main__":
    main()
