from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from common import INK_2, INK_3, RED, ROOT, output_line, save_figure, setup_theme
from critical_common import GOLD_CSV, merged_gold_llm, sources_text


SOURCES = [
    GOLD_CSV,
    ROOT / "candil_llm_annotation_pipeline" / "llm_annotation_outputs_v12_backup",
    ROOT / "jaleo_llm_annotation_pipeline_v12" / "llm_annotation_outputs_v12_backup",
]


def build_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    data = merged_gold_llm().copy()
    data["word_count"] = pd.to_numeric(data["word_count"], errors="coerce")
    data = data.dropna(subset=["word_count", "family_f1"]).copy()
    data = data[data["word_count"] > 0]
    data["periodical_label"] = data["periodical"].str.title()
    data["length_bin"] = pd.qcut(data["word_count"], q=5, duplicates="drop")
    binned = (
        data.groupby(["length_bin", "periodical_label"], observed=True)
        .agg(
            family_f1=("family_f1", "mean"),
            code_f1=("code_f1", "mean"),
            word_count=("word_count", "median"),
            articles=("audit_id", "count"),
        )
        .reset_index()
    )
    return data, binned


def plot(outdir: Path | None = None) -> list[Path]:
    import matplotlib.pyplot as plt
    import seaborn as sns

    setup_theme()
    data, binned = build_data()
    palette = {"Candil": RED, "Jaleo": INK_2}

    fig, ax = plt.subplots(figsize=(11.5, 6.4))
    sns.scatterplot(
        data=data,
        x="word_count",
        y="family_f1",
        hue="periodical_label",
        palette=palette,
        alpha=0.5,
        s=55,
        edgecolor="none",
        ax=ax,
    )
    sns.lineplot(
        data=binned,
        x="word_count",
        y="family_f1",
        hue="periodical_label",
        palette=palette,
        marker="o",
        linewidth=2.2,
        legend=False,
        ax=ax,
    )
    ax.axhline(0.5, color="#b9afa2", linewidth=1, linestyle="--")
    ax.text(data["word_count"].min(), 0.52, "0.50 reference", color=INK_3, fontsize=9)
    ax.set_xscale("log")
    ax.set_ylim(-0.03, 1.05)
    ax.set_xlabel("Article word count (log scale)")
    ax.set_ylabel("Article-level family F1")
    ax.legend(title="", frameon=False)
    fig.subplots_adjust(top=0.95)
    return save_figure(fig, "fig13_length_vs_agreement", outdir)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--outdir", type=Path, default=ROOT / "figs")
    args = parser.parse_args()
    paths = plot(args.outdir)
    print(sources_text(SOURCES))
    print(output_line(paths))


if __name__ == "__main__":
    main()
