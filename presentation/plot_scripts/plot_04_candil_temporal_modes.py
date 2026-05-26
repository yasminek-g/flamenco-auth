from __future__ import annotations

import argparse
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
    code_family,
    load_annotation_summary,
    output_line,
    save_figure,
    setup_theme,
    split_labels,
)


TEMPORAL_ORDER = ["AUTH", "HERIT", "COMM", "PED", "CRIT"]


def has_family(value: object, family: str) -> bool:
    return any(code_family(code) == family for code in split_labels(value))


def build_data() -> pd.DataFrame:
    rows = []
    for periodical in ["candil", "jaleo"]:
        summary = load_annotation_summary(periodical).copy()
        summary["year"] = summary["issue_id"].astype(str).str.extract(r"(\d{4})").astype(int)
        for year, year_df in summary.groupby("year"):
            denom = len(year_df)
            for family in TEMPORAL_ORDER:
                n = int(year_df["all_accepted_codes"].apply(lambda value: has_family(value, family)).sum())
                rows.append(
                    {
                        "periodical": periodical.title(),
                        "year": year,
                        "family": family,
                        "share": n / denom * 100,
                        "n": n,
                        "denom": denom,
                    }
                )
    return pd.DataFrame(rows).sort_values(["periodical", "family", "year"])


def plot(data: pd.DataFrame, outdir: Path | None = None) -> list[Path]:
    import matplotlib.pyplot as plt
    import seaborn as sns

    setup_theme()
    fig, axes = plt.subplots(2, len(TEMPORAL_ORDER), figsize=(15.5, 6.8), sharey=True)

    for row_idx, periodical in enumerate(["Candil", "Jaleo"]):
        color = PERIODICAL_PALETTE[periodical]
        fill = RED_SOFT if periodical == "Candil" else "#e8e3dc"
        periodical_data = data[data["periodical"].eq(periodical)]
        for col_idx, family in enumerate(TEMPORAL_ORDER):
            ax = axes[row_idx, col_idx]
            fam_df = periodical_data[periodical_data["family"].eq(family)].copy()
            sns.lineplot(data=fam_df, x="year", y="share", color=color, linewidth=2.0, ax=ax)
            ax.scatter(fam_df["year"], fam_df["share"], color=color, s=14, zorder=3)
            ax.fill_between(fam_df["year"], fam_df["share"], color=fill, alpha=0.55)

            start = fam_df["share"].iloc[0]
            end = fam_df["share"].iloc[-1]
            delta = end - start
            sign = "+" if delta >= 0 else ""
            if col_idx == 0:
                ax.set_ylabel(f"{periodical}\narticle share", fontsize=10.5, color=INK_2)
            else:
                ax.set_ylabel("")

            ax.text(
                0.04,
                0.84,
                f"{start:.0f}% -> {end:.0f}% ({sign}{delta:.0f})",
                transform=ax.transAxes,
                fontsize=8.8,
                color=INK_3,
                bbox={"facecolor": PAPER, "edgecolor": "none", "alpha": 0.86, "pad": 1.4},
            )
            ax.set_xlabel("")
            if row_idx == 1:
                ax.set_xlabel(family)
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

    fig.subplots_adjust(wspace=0.18, hspace=0.32, top=0.95, left=0.08, right=0.99, bottom=0.1)
    return save_figure(fig, "fig04_temporal_modes_by_periodical", outdir)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--outdir", type=Path, default=ROOT / "figs")
    args = parser.parse_args()
    paths = plot(build_data(), args.outdir)
    print(output_line(paths))


if __name__ == "__main__":
    main()
