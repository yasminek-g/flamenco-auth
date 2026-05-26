from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from common import INK, INK_3, INK_2, RED, ROOT, output_line, save_figure, setup_theme
from critical_common import GOLD_CSV, label_metrics, merged_gold_llm, sources_text


SOURCES = [GOLD_CSV, ROOT / "candil_llm_annotation_pipeline" / "llm_annotation_outputs_v12_backup", ROOT / "jaleo_llm_annotation_pipeline_v12" / "llm_annotation_outputs_v12_backup"]


def plot(outdir: Path | None = None) -> list[Path]:
    import matplotlib.pyplot as plt
    import seaborn as sns

    setup_theme()
    metrics = label_metrics(merged_gold_llm(), "family")
    long = metrics.melt(id_vars="family", value_vars=["gold_support", "llm_support"], var_name="source", value_name="articles")
    long["source"] = long["source"].map({"gold_support": "Human gold", "llm_support": "LLM"})

    fig, ax = plt.subplots(figsize=(11.5, 5.8))
    sns.barplot(data=long, x="family", y="articles", hue="source", palette={"Human gold": RED, "LLM": INK_2}, ax=ax)
    ax.set_xlabel("")
    ax.set_ylabel("Article-label decisions")
    ax.legend(title="", frameon=False)
    for container in ax.containers:
        for bar in container:
            height = bar.get_height()
            if not height or height != height or height <= 0:
                continue
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                height + 1.2,
                f"{int(height)}",
                va="bottom",
                ha="center",
                fontsize=8,
                color=INK,
            )
    fig.subplots_adjust(top=0.94)
    return save_figure(fig, "fig08_gold_vs_llm_family_support", outdir)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--outdir", type=Path, default=ROOT / "figs")
    args = parser.parse_args()
    paths = plot(args.outdir)
    print(sources_text(SOURCES))
    print(output_line(paths))


if __name__ == "__main__":
    main()
