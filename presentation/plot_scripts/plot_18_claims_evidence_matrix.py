from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from common import INK, INK_3, RED, ROOT, output_line, save_figure, setup_theme
from critical_common import (
    CANDIL_ARTICLE_FOOTPRINT,
    CANDIL_SECTIONS,
    GOLD_CSV,
    JALEO_ARTICLE_SECTIONS,
    JALEO_SECTIONS,
    sources_text,
)


SOURCES = [
    GOLD_CSV,
    CANDIL_SECTIONS,
    JALEO_SECTIONS,
    CANDIL_ARTICLE_FOOTPRINT,
    JALEO_ARTICLE_SECTIONS,
    ROOT / "candil_llm_annotation_pipeline" / "llm_annotation_outputs_v12_backup",
    ROOT / "jaleo_llm_annotation_pipeline_v12" / "llm_annotation_outputs_v12_backup",
]


def build_matrix() -> pd.DataFrame:
    rows = {
        "Human-gold sample & sampling": {
            "Human gold": 2,
            "Corpus metadata": 1,
            "LLM annotations": 0,
            "Use in talk": 2,
        },
        "Human-gold mode differences": {
            "Human gold": 2,
            "Corpus metadata": 0,
            "LLM annotations": 0,
            "Use in talk": 2,
        },
        "Section architecture": {
            "Human gold": 1,
            "Corpus metadata": 2,
            "LLM annotations": 0,
            "Use in talk": 2,
        },
        "Family-level LLM trends": {
            "Human gold": 1,
            "Corpus metadata": 1,
            "LLM annotations": 1,
            "Use in talk": 1,
        },
        "Code-level LLM claims": {
            "Human gold": 1,
            "Corpus metadata": 0,
            "LLM annotations": 1,
            "Use in talk": 0,
        },
        "Temporal mode signals": {
            "Human gold": 0,
            "Corpus metadata": 1,
            "LLM annotations": 1,
            "Use in talk": 1,
        },
    }
    return pd.DataFrame.from_dict(rows, orient="index")


def plot(outdir: Path | None = None) -> list[Path]:
    import matplotlib.pyplot as plt
    import seaborn as sns
    from matplotlib.colors import ListedColormap

    setup_theme()
    matrix = build_matrix()
    cmap = ListedColormap(["#e8e0d6", "#d9a69f", RED])
    labels = matrix.replace({0: "Avoid", 1: "Use with caveat", 2: "Strong"})

    fig, ax = plt.subplots(figsize=(11.8, 6.8))
    sns.heatmap(
        matrix,
        annot=labels,
        fmt="",
        cmap=cmap,
        vmin=0,
        vmax=2,
        cbar=False,
        linewidths=1,
        linecolor="#f5eee6",
        annot_kws={"fontsize": 9, "color": INK},
        ax=ax,
    )
    ax.set_xlabel("")
    ax.set_ylabel("")
    ax.tick_params(axis="x", rotation=18)
    ax.tick_params(axis="y", rotation=0)
    fig.subplots_adjust(top=0.93, left=0.25, bottom=0.18)
    return save_figure(fig, "fig18_claims_evidence_matrix", outdir)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--outdir", type=Path, default=ROOT / "figs")
    args = parser.parse_args()
    paths = plot(args.outdir)
    print(sources_text(SOURCES))
    print(output_line(paths))


if __name__ == "__main__":
    main()
