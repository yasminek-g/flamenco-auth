from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from common import INK, INK_2, INK_3, RED, ROOT, output_line, read_csv, save_figure, setup_theme
from critical_common import CANDIL_ARTICLE_FOOTPRINT, JALEO_ARTICLE_SECTIONS, sources_text


SOURCES = [CANDIL_ARTICLE_FOOTPRINT, JALEO_ARTICLE_SECTIONS]


def build_data() -> pd.DataFrame:
    candil = read_csv(CANDIL_ARTICLE_FOOTPRINT)
    candil["periodical"] = "Candil"
    candil["year"] = pd.to_numeric(candil["year"], errors="coerce")
    candil["sectioned"] = candil["kept_as_recurring"].astype(bool)

    jaleo = read_csv(JALEO_ARTICLE_SECTIONS)
    jaleo["periodical"] = "Jaleo"
    jaleo["year"] = pd.to_datetime(jaleo["issue_date"], errors="coerce").dt.year
    jaleo["sectioned"] = jaleo["section_candidate"].notna() & ~jaleo["section_candidate"].eq("Unclassified")

    data = pd.concat(
        [
            candil[["periodical", "year", "sectioned"]],
            jaleo[["periodical", "year", "sectioned"]],
        ],
        ignore_index=True,
    ).dropna(subset=["year"])
    data["year"] = data["year"].astype(int)
    return (
        data.groupby(["periodical", "year"], observed=True)
        .agg(sectioned_share=("sectioned", "mean"), articles=("sectioned", "size"))
        .reset_index()
    )


def plot(outdir: Path | None = None) -> list[Path]:
    import matplotlib.pyplot as plt
    import seaborn as sns

    setup_theme()
    data = build_data()
    palette = {"Candil": RED, "Jaleo": INK_2}

    fig, ax = plt.subplots(figsize=(12.5, 6.2))
    sns.lineplot(data=data, x="year", y="sectioned_share", hue="periodical", palette=palette, marker="o", linewidth=2.2, ax=ax)
    ax.set_ylim(0, 1.05)
    ax.yaxis.set_major_formatter(lambda value, _pos: f"{value:.0%}")
    ax.set_xlabel("")
    ax.set_ylabel("Sectioned article share")
    ax.legend(title="", frameon=False)
    fig.subplots_adjust(top=0.94)
    return save_figure(fig, "fig16_section_architecture_over_time", outdir)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--outdir", type=Path, default=ROOT / "figs")
    args = parser.parse_args()
    paths = plot(args.outdir)
    print(sources_text(SOURCES))
    print(output_line(paths))


if __name__ == "__main__":
    main()
