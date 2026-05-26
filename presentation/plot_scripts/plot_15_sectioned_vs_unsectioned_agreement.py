from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from common import INK, INK_2, INK_3, RED, ROOT, output_line, read_csv, save_figure, setup_theme
from critical_common import CANDIL_SECTIONS, GOLD_CSV, JALEO_SECTIONS, merged_gold_llm, sources_text


SOURCES = [
    GOLD_CSV,
    CANDIL_SECTIONS,
    JALEO_SECTIONS,
    ROOT / "candil_llm_annotation_pipeline" / "llm_annotation_outputs_v12_backup",
    ROOT / "jaleo_llm_annotation_pipeline_v12" / "llm_annotation_outputs_v12_backup",
]


def section_flags() -> pd.DataFrame:
    candil = read_csv(CANDIL_SECTIONS)[["audit_id", "kept_as_recurring", "detected_section_label"]].copy()
    candil["section_status"] = candil["kept_as_recurring"].map({True: "Recurring section", False: "Unsectioned"})
    candil["section_label"] = candil["detected_section_label"].fillna("Unsectioned")

    jaleo = read_csv(JALEO_SECTIONS)[["audit_id", "section_candidate"]].copy()
    jaleo["section_status"] = jaleo["section_candidate"].where(
        jaleo["section_candidate"].notna() & ~jaleo["section_candidate"].eq("Unclassified"),
        "Unsectioned",
    )
    jaleo["section_status"] = jaleo["section_status"].mask(jaleo["section_status"].ne("Unsectioned"), "Named section")
    jaleo["section_label"] = jaleo["section_candidate"].fillna("Unclassified")
    return pd.concat(
        [
            candil[["audit_id", "section_status", "section_label"]],
            jaleo[["audit_id", "section_status", "section_label"]],
        ],
        ignore_index=True,
    )


def build_data() -> pd.DataFrame:
    data = merged_gold_llm().merge(section_flags(), on="audit_id", how="left")
    data["section_status"] = data["section_status"].fillna("Unsectioned")
    data["periodical_label"] = data["periodical"].str.title()
    long = data.melt(
        id_vars=["audit_id", "periodical_label", "section_status"],
        value_vars=["family_f1", "code_f1"],
        var_name="level",
        value_name="article_f1",
    )
    long["level"] = long["level"].map({"family_f1": "Family", "code_f1": "Code"})
    return (
        long.groupby(["periodical_label", "section_status", "level"], observed=True)
        .agg(mean_f1=("article_f1", "mean"), articles=("audit_id", "count"))
        .reset_index()
    )


def plot(outdir: Path | None = None) -> list[Path]:
    import matplotlib.pyplot as plt
    import seaborn as sns

    setup_theme()
    data = build_data()
    data["group"] = data["periodical_label"] + " | " + data["section_status"]
    order = ["Candil | Recurring section", "Candil | Unsectioned", "Jaleo | Named section", "Jaleo | Unsectioned"]
    order = [value for value in order if value in set(data["group"])]
    palette = {"Family": RED, "Code": INK_2}

    fig, ax = plt.subplots(figsize=(12.5, 6.4))
    sns.barplot(data=data, x="group", y="mean_f1", hue="level", order=order, palette=palette, ax=ax)
    ax.set_ylim(0, 1.0)
    ax.set_xlabel("")
    ax.set_ylabel("Mean article-level F1")
    ax.tick_params(axis="x", rotation=18)
    ax.legend(title="", frameon=False)
    for container in ax.containers:
        for bar in container:
            height = bar.get_height()
            if height != height:
                continue
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                height + 0.02,
                f"{height:.2f}",
                va="bottom",
                ha="center",
                fontsize=8,
                color=INK,
            )
    fig.subplots_adjust(top=0.95, bottom=0.22)
    return save_figure(fig, "fig15_sectioned_vs_unsectioned_agreement", outdir)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--outdir", type=Path, default=ROOT / "figs")
    args = parser.parse_args()
    paths = plot(args.outdir)
    print(sources_text(SOURCES))
    print(output_line(paths))


if __name__ == "__main__":
    main()
