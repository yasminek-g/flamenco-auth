from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from common import INK, INK_2, RED, ROOT, output_line, save_figure, setup_theme
from critical_common import GOLD_CSV, merged_gold_llm, sources_text


SOURCES = [
    GOLD_CSV,
    ROOT / "candil_llm_annotation_pipeline" / "llm_annotation_outputs_v12_backup",
    ROOT / "jaleo_llm_annotation_pipeline_v12" / "llm_annotation_outputs_v12_backup",
]


def build_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    data = merged_gold_llm().copy()
    data["periodical_label"] = data["periodical"].str.title()
    long = data.melt(
        id_vars=["audit_id", "periodical_label", "human_confidence"],
        value_vars=["family_f1", "code_f1"],
        var_name="level",
        value_name="article_f1",
    )
    long["level"] = long["level"].map({"family_f1": "Family", "code_f1": "Code"})
    periodical = (
        long.groupby(["periodical_label", "level"], observed=True)
        .agg(mean_f1=("article_f1", "mean"), articles=("audit_id", "count"))
        .reset_index()
    )
    confidence = (
        data.groupby(["human_confidence", "periodical_label"], observed=True)
        .agg(mean_family_f1=("family_f1", "mean"), articles=("audit_id", "count"))
        .reset_index()
    )
    order = ["low", "medium", "high"]
    confidence["human_confidence"] = pd.Categorical(confidence["human_confidence"], order, ordered=True)
    confidence = confidence.sort_values("human_confidence")
    return long, periodical, confidence


def plot(outdir: Path | None = None) -> list[Path]:
    import matplotlib.pyplot as plt
    import seaborn as sns

    setup_theme()
    _long, periodical, confidence = build_data()
    palette = {"Candil": RED, "Jaleo": INK_2}
    level_palette = {"Family": RED, "Code": INK_2}

    fig, axes = plt.subplots(1, 2, figsize=(13.5, 6.2), gridspec_kw={"width_ratios": [1, 1.15]})
    sns.barplot(
        data=periodical,
        x="periodical_label",
        y="mean_f1",
        hue="level",
        palette=level_palette,
        ax=axes[0],
    )
    axes[0].set_ylim(0, 1.0)
    axes[0].set_xlabel("")
    axes[0].set_ylabel("Mean article-level F1")
    axes[0].legend(title="", frameon=False)
    for container in axes[0].containers:
        for bar in container:
            height = bar.get_height()
            if height != height:
                continue
            axes[0].text(
                bar.get_x() + bar.get_width() / 2,
                height + 0.02,
                f"{height:.2f}",
                va="bottom",
                ha="center",
                fontsize=8,
                color=INK,
            )

    sns.pointplot(
        data=confidence,
        x="human_confidence",
        y="mean_family_f1",
        hue="periodical_label",
        palette=palette,
        dodge=0.25,
        markers="o",
        ci=None,
        ax=axes[1],
    )
    axes[1].set_ylim(0, 1.0)
    axes[1].set_xlabel("Human confidence")
    axes[1].set_ylabel("Mean family F1")
    axes[1].legend(title="", frameon=False)

    fig.subplots_adjust(top=0.95, wspace=0.28)
    return save_figure(fig, "fig14_agreement_by_periodical_confidence", outdir)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--outdir", type=Path, default=ROOT / "figs")
    args = parser.parse_args()
    paths = plot(args.outdir)
    print(sources_text(SOURCES))
    print(output_line(paths))


if __name__ == "__main__":
    main()
