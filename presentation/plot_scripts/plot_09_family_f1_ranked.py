from __future__ import annotations

import argparse
from pathlib import Path

from common import INK, INK_2, RED, ROOT, output_line, save_figure, setup_theme
from critical_common import GOLD_CSV, label_metrics, merged_gold_llm, sources_text


SOURCES = [
    GOLD_CSV,
    ROOT / "candil_llm_annotation_pipeline" / "llm_annotation_outputs_v12_backup",
    ROOT / "jaleo_llm_annotation_pipeline_v12" / "llm_annotation_outputs_v12_backup",
]

DISPLAY_FAMILIES = ["CRIT", "COMM", "PED", "WCL", "TRAD", "AUTH", "HERIT"]


def plot(outdir: Path | None = None) -> list[Path]:
    import matplotlib.pyplot as plt

    setup_theme()
    metrics = label_metrics(merged_gold_llm(), "family")
    metrics = metrics[metrics["family"].isin(DISPLAY_FAMILIES)].copy()
    metrics["family"] = metrics["family"].astype(str)
    metrics = metrics.sort_values(["f1", "gold_support"], ascending=[True, True])

    fig, ax = plt.subplots(figsize=(10.8, 6.1))
    bars = ax.barh(metrics["family"], metrics["f1"], color=RED, height=0.62)

    ax.set_xlim(0, 1.0)
    ax.set_xlabel("F1")
    ax.set_ylabel("")
    ax.xaxis.set_major_formatter(lambda x, _pos: f"{x:.1f}")
    ax.grid(axis="x", color="#e5ded4", linewidth=0.8)
    ax.grid(axis="y", visible=False)
    ax.tick_params(axis="y", labelsize=13, colors=INK_2)

    for bar, value in zip(bars, metrics["f1"]):
        x = min(value + 0.025, 0.94)
        ha = "left" if value <= 0.90 else "right"
        color = INK if value <= 0.90 else "#ffffff"
        ax.text(
            x,
            bar.get_y() + bar.get_height() / 2,
            f"{value:.2f}",
            va="center",
            ha=ha,
            fontsize=11,
            fontweight="bold",
            color=color,
        )

    fig.subplots_adjust(top=0.96, bottom=0.14, left=0.12, right=0.98)
    return save_figure(fig, "fig09_family_f1_ranked", outdir)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--outdir", type=Path, default=ROOT / "figs")
    args = parser.parse_args()
    paths = plot(args.outdir)
    print(sources_text(SOURCES))
    print(output_line(paths))


if __name__ == "__main__":
    main()
