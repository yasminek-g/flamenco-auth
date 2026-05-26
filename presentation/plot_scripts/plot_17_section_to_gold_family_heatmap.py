from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from common import INK, INK_3, RED, ROOT, family_set, output_line, read_csv, save_figure, setup_theme, split_labels
from critical_common import CANDIL_SECTIONS, CORE_FAMILIES, JALEO_SECTIONS, sources_text


SOURCES = [CANDIL_SECTIONS, JALEO_SECTIONS]


def build_matrix(min_articles: int = 3, top_sections: int = 18) -> pd.DataFrame:
    candil = read_csv(CANDIL_SECTIONS)
    candil["periodical"] = "Candil"
    candil["section"] = candil["detected_section_label"].where(
        candil["kept_as_recurring"].astype(bool),
        "Unsectioned",
    )

    jaleo = read_csv(JALEO_SECTIONS)
    jaleo["periodical"] = "Jaleo"
    jaleo["section"] = jaleo["section_candidate"].where(
        jaleo["section_candidate"].notna() & ~jaleo["section_candidate"].eq("Unclassified"),
        "Unsectioned",
    )

    rows = []
    for frame in [candil, jaleo]:
        for _, row in frame.iterrows():
            families = family_set(split_labels(row.get("human_gold_codes")))
            for family in families:
                if family in CORE_FAMILIES:
                    rows.append(
                        {
                            "section": f"{row['periodical']} | {row['section']}",
                            "audit_id": row["audit_id"],
                            "family": family,
                        }
                    )
    long = pd.DataFrame(rows)
    if long.empty:
        return pd.DataFrame(columns=CORE_FAMILIES)

    support = long.drop_duplicates(["section", "audit_id"]).groupby("section").size()
    keep = support[support >= min_articles].nlargest(top_sections).index
    long = long[long["section"].isin(keep)]
    counts = pd.crosstab(long["section"], long["family"]).reindex(columns=CORE_FAMILIES, fill_value=0)
    shares = counts.div(counts.sum(axis=1).replace(0, pd.NA), axis=0).fillna(0)
    order = counts.sum(axis=1).sort_values(ascending=True).index
    return shares.loc[order]


def plot(outdir: Path | None = None) -> list[Path]:
    import matplotlib.pyplot as plt
    import seaborn as sns

    setup_theme()
    matrix = build_matrix()
    fig, ax = plt.subplots(figsize=(11.8, 8.4))
    sns.heatmap(
        matrix,
        annot=True,
        fmt=".0%",
        cmap=sns.light_palette(RED, as_cmap=True),
        cbar_kws={"label": "Share of human-gold family assignments"},
        linewidths=0.5,
        linecolor="#efe8de",
        ax=ax,
    )
    ax.set_xlabel("Human-gold family")
    ax.set_ylabel("")
    ax.tick_params(axis="x", rotation=0)
    ax.tick_params(axis="y", rotation=0)
    fig.subplots_adjust(top=0.95, left=0.29)
    return save_figure(fig, "fig17_section_to_gold_family_heatmap", outdir)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--outdir", type=Path, default=ROOT / "figs")
    args = parser.parse_args()
    paths = plot(args.outdir)
    print(sources_text(SOURCES))
    print(output_line(paths))


if __name__ == "__main__":
    main()
