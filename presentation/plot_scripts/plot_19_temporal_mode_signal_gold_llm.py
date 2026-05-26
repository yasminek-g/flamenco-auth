from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd

from common import (
    INK,
    INK_2,
    INK_3,
    PAPER,
    PERIODICAL_PALETTE,
    RED_SOFT,
    ROOT,
    family_set,
    load_annotation_summary,
    output_line,
    read_csv,
    save_figure,
    setup_theme,
    split_labels,
)


TEMPORAL_ORDER = ["AUTH", "HERIT", "COMM", "PED", "CRIT"]


def _has_family(value: object, family: str) -> bool:
    return family in family_set(split_labels(value))


def _bin_midpoint(period_bin: str) -> int | None:
    years = [int(year) for year in re.findall(r"\d{4}", str(period_bin))]
    if len(years) >= 2:
        return round((years[0] + years[1]) / 2)
    if len(years) == 1:
        return years[0]
    return None


def build_llm_data() -> pd.DataFrame:
    rows = []
    for periodical in ["candil", "jaleo"]:
        summary = load_annotation_summary(periodical).copy()
        summary["year"] = summary["issue_id"].astype(str).str.extract(r"(\d{4})").astype(int)
        for year, year_df in summary.groupby("year"):
            denom = len(year_df)
            for family in TEMPORAL_ORDER:
                n = int(year_df["all_accepted_codes"].apply(lambda value: _has_family(value, family)).sum())
                rows.append(
                    {
                        "source": "LLM annual",
                        "periodical": periodical.title(),
                        "year": int(year),
                        "family": family,
                        "share": n / denom * 100,
                        "n": n,
                        "denom": denom,
                    }
                )
    return pd.DataFrame(rows)


def build_gold_data(gold_csv: Path) -> pd.DataFrame:
    gold = read_csv(gold_csv)
    gold = gold[gold["final_include_for_metrics"].astype(str).str.lower().eq("yes")].copy()
    gold["periodical_title"] = gold["periodical"].str.title()
    gold["bin_midpoint"] = gold["period_bin"].apply(_bin_midpoint)
    gold = gold[gold["bin_midpoint"].notna()].copy()

    rows = []
    for (periodical, period_bin, midpoint), bin_df in gold.groupby(
        ["periodical_title", "period_bin", "bin_midpoint"]
    ):
        denom = len(bin_df)
        for family in TEMPORAL_ORDER:
            n = int(bin_df["human_gold_codes"].apply(lambda value: _has_family(value, family)).sum())
            rows.append(
                {
                    "source": "Human gold bin",
                    "periodical": periodical,
                    "period_bin": period_bin,
                    "year": int(midpoint),
                    "family": family,
                    "share": n / denom * 100,
                    "n": n,
                    "denom": denom,
                }
            )
    return pd.DataFrame(rows)


def build_data(gold_csv: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    return build_llm_data(), build_gold_data(gold_csv)


def plot(llm: pd.DataFrame, gold: pd.DataFrame, outdir: Path | None = None) -> list[Path]:
    import matplotlib.lines as mlines
    import matplotlib.pyplot as plt
    import seaborn as sns

    setup_theme()
    fig, axes = plt.subplots(2, len(TEMPORAL_ORDER), figsize=(15.5, 6.9), sharey=True)

    for row_idx, periodical in enumerate(["Candil", "Jaleo"]):
        color = PERIODICAL_PALETTE[periodical]
        fill = RED_SOFT if periodical == "Candil" else "#e8e3dc"
        periodical_llm = llm[llm["periodical"].eq(periodical)]
        periodical_gold = gold[gold["periodical"].eq(periodical)]

        for col_idx, family in enumerate(TEMPORAL_ORDER):
            ax = axes[row_idx, col_idx]
            fam_llm = periodical_llm[periodical_llm["family"].eq(family)].copy()
            fam_gold = periodical_gold[periodical_gold["family"].eq(family)].copy()

            sns.lineplot(data=fam_llm, x="year", y="share", color=color, linewidth=1.8, ax=ax)
            ax.fill_between(fam_llm["year"], fam_llm["share"], color=fill, alpha=0.42)
            ax.scatter(fam_llm["year"], fam_llm["share"], color=color, s=12, zorder=3)

            if not fam_gold.empty:
                sizes = 42 + fam_gold["denom"] * 1.4
                ax.scatter(
                    fam_gold["year"],
                    fam_gold["share"],
                    s=sizes,
                    marker="s",
                    facecolors=PAPER,
                    edgecolors=color,
                    linewidths=1.7,
                    zorder=5,
                )

            if col_idx == 0:
                ax.set_ylabel(f"{periodical}\narticle share", fontsize=10.5, color=INK_2)
            else:
                ax.set_ylabel("")

            ax.set_xlabel(family if row_idx == 1 else "")
            ax.set_ylim(0, 105)
            if periodical == "Candil":
                ticks = [1979, 1988, 1998]
                labels = ["'79", "'88", "'98"]
            else:
                ticks = [1977, 1984, 1992]
                labels = ["'77", "'84", "'92"]
            ax.set_xticks(ticks)
            ax.set_xticklabels(labels)
            ax.grid(axis="x", visible=False)
            ax.tick_params(axis="both", labelsize=10)

            if row_idx == 0 and col_idx == 0:
                ax.text(
                    0.04,
                    0.86,
                    "line = LLM annual\nsquares = gold bins",
                    transform=ax.transAxes,
                    fontsize=8.5,
                    color=INK_3,
                    bbox={"facecolor": PAPER, "edgecolor": "none", "alpha": 0.9, "pad": 1.5},
                )

    line_handle = mlines.Line2D([], [], color=INK, linewidth=1.8, label="Full-corpus LLM annual share")
    square_handle = mlines.Line2D(
        [],
        [],
        color=INK,
        marker="s",
        markersize=7,
        linestyle="None",
        markerfacecolor=PAPER,
        label="Human-gold period-bin share",
    )
    fig.legend(
        handles=[line_handle, square_handle],
        loc="upper center",
        ncol=2,
        frameon=False,
        fontsize=10,
        bbox_to_anchor=(0.55, 1.02),
    )
    fig.subplots_adjust(wspace=0.18, hspace=0.34, top=0.9, left=0.08, right=0.99, bottom=0.1)
    return save_figure(fig, "fig19_temporal_mode_signal_gold_llm", outdir)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gold-csv", type=Path, default=ROOT / "human_gold_audit_complete.csv")
    parser.add_argument("--outdir", type=Path, default=ROOT / "figs")
    args = parser.parse_args()
    paths = plot(*build_data(args.gold_csv), outdir=args.outdir)
    print(output_line(paths))


if __name__ == "__main__":
    main()
