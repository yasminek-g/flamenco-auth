from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from common import INK, INK_2, INK_3, RED, ROOT, output_line, save_figure, setup_theme
from critical_common import GOLD_CSV, label_metrics, merged_gold_llm, sources_text


SOURCES = [GOLD_CSV, ROOT / "candil_llm_annotation_pipeline" / "llm_annotation_outputs_v12_backup", ROOT / "jaleo_llm_annotation_pipeline_v12" / "llm_annotation_outputs_v12_backup"]


def plot(outdir: Path | None = None) -> list[Path]:
    import matplotlib.pyplot as plt
    import seaborn as sns

    setup_theme()
    metrics = label_metrics(merged_gold_llm(), "family")
    metrics = metrics.sort_values("f1", ascending=True)
    long = metrics.melt(id_vars="family", value_vars=["precision", "recall", "f1"], var_name="metric", value_name="value")
    palette = {"precision": INK_2, "recall": "#8f6f4e", "f1": RED}

    fig, ax = plt.subplots(figsize=(10.5, 6.2))
    sns.scatterplot(data=long, x="value", y="family", hue="metric", style="metric", palette=palette, s=95, ax=ax)
    for _, row in metrics.iterrows():
        ax.hlines(row["family"], row[["precision", "recall", "f1"]].min(), row[["precision", "recall", "f1"]].max(), color="#d9d1c6", linewidth=1)
    ax.set_xlim(0, 1.02)
    ax.xaxis.set_major_formatter(lambda x, _pos: f"{x:.1f}")
    ax.set_xlabel("Score")
    ax.set_ylabel("")
    ax.legend(title="", frameon=False, loc="lower right")
    fig.subplots_adjust(top=0.94)
    return save_figure(fig, "fig09_family_error_profile", outdir)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--outdir", type=Path, default=ROOT / "figs")
    args = parser.parse_args()
    paths = plot(args.outdir)
    print(sources_text(SOURCES))
    print(output_line(paths))


if __name__ == "__main__":
    main()
