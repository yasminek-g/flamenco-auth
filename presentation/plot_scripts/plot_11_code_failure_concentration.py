from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from common import INK_2, RED, ROOT, output_line, save_figure, setup_theme
from critical_common import GOLD_CSV, label_metrics, merged_gold_llm, sources_text


SOURCES = [GOLD_CSV, ROOT / "candil_llm_annotation_pipeline" / "llm_annotation_outputs_v12_backup", ROOT / "jaleo_llm_annotation_pipeline_v12" / "llm_annotation_outputs_v12_backup"]


def plot(outdir: Path | None = None, top_n: int = 16) -> list[Path]:
    import matplotlib.pyplot as plt

    setup_theme()
    metrics = label_metrics(merged_gold_llm(), "code")
    metrics["error_total"] = metrics["fp"] + metrics["fn"]
    plot_df = metrics.nlargest(top_n, "error_total").sort_values("error_total", ascending=True)

    fig, ax = plt.subplots(figsize=(10.5, 7.2))
    y = range(len(plot_df))
    ax.barh(y, plot_df["fn"], color=RED, label="missed gold")
    ax.barh(y, plot_df["fp"], left=plot_df["fn"], color=INK_2, label="extra LLM")
    ax.barh(y, plot_df["tp"], left=plot_df["fn"] + plot_df["fp"], color="#9f8f78", label="true positive")
    ax.set_yticks(list(y))
    ax.set_yticklabels(plot_df["code"])
    ax.set_xlabel("Article-label decisions")
    ax.set_ylabel("")
    ax.legend(title="", frameon=False, loc="lower right")
    fig.subplots_adjust(top=0.95, left=0.17)
    return save_figure(fig, "fig11_code_failure_concentration", outdir)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--outdir", type=Path, default=ROOT / "figs")
    parser.add_argument("--top-n", type=int, default=16)
    args = parser.parse_args()
    paths = plot(args.outdir, args.top_n)
    print(sources_text(SOURCES))
    print(output_line(paths))


if __name__ == "__main__":
    main()
