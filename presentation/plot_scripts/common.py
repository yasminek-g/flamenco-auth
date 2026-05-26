from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Iterable

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
FIGS_DIR = ROOT / "figs"
MPLCONFIGDIR = ROOT / "tmp" / "presentation_matplotlib"
MPLCONFIGDIR.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", str(MPLCONFIGDIR))
os.environ.setdefault("XDG_CACHE_HOME", str(MPLCONFIGDIR))
os.environ.setdefault("MPLBACKEND", "Agg")

PAPER = "#fbfaf7"
PAPER_2 = "#f3f1ec"
INK = "#15110e"
INK_2 = "#3a342e"
INK_3 = "#6c6258"
INK_4 = "#a39a8e"
RULE = "#d7d1c7"
RED = "#c8281f"
RED_DEEP = "#8a160f"
RED_TINT = "#f3dad7"
RED_SOFT = "#fbeae8"

PERIODICAL_PALETTE = {
    "Candil": RED,
    "Jaleo": INK_2,
    "candil": RED,
    "jaleo": INK_2,
}

FAMILY_ORDER = ["CRIT", "AUTH", "COMM", "PED", "HERIT", "OTHER"]
MODE_FAMILIES = {"AUTH", "COMM", "CRIT", "HERIT", "PED"}

# TRAD concepts (TRAD_01–04) live inside the AUTH family in codebook v12
# ("authenticity / tradition / continuity claims"), so they report as AUTH.
FAMILY_FOLD = {"TRAD": "AUTH"}
# Dropped from the deck: legitimacy is treated as an effect of the five modes
# rather than its own family, and WCL has too little support to evaluate.
DROPPED_FAMILIES = {"WCL", "LEGIT"}


def setup_theme() -> None:
    import matplotlib

    matplotlib.use("Agg", force=True)
    import matplotlib.pyplot as plt
    import seaborn as sns

    sns.set_theme(style="whitegrid", context="talk")
    plt.rcParams.update(
        {
            "figure.facecolor": PAPER,
            "axes.facecolor": PAPER,
            "savefig.facecolor": PAPER,
            "axes.edgecolor": RULE,
            "axes.labelcolor": INK_2,
            "axes.titlecolor": INK,
            "xtick.color": INK_3,
            "ytick.color": INK_3,
            "grid.color": "#e5ded4",
            "grid.linewidth": 0.8,
            "font.family": "DejaVu Sans",
            "axes.titleweight": "bold",
            "axes.spines.top": False,
            "axes.spines.right": False,
        }
    )


def read_csv(path: Path, **kwargs) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required data file: {path}")
    return pd.read_csv(path, encoding="utf-8-sig", **kwargs)


def split_labels(value: object) -> list[str]:
    if pd.isna(value):
        return []
    return [part.strip() for part in re.split(r"[;,]", str(value)) if part.strip()]


def code_family(code: str) -> str:
    raw = code.split("_", 1)[0] if "_" in code else code
    return FAMILY_FOLD.get(raw, raw)


def first_code_family(value: object, *, collapse_other: bool = True) -> str | None:
    labels = split_labels(value)
    if not labels:
        return "OTHER"
    fam = code_family(labels[0])
    if fam in DROPPED_FAMILIES:
        return None
    if collapse_other and fam not in MODE_FAMILIES:
        return "OTHER"
    return fam


def family_set(codes: Iterable[str]) -> set[str]:
    return {code_family(code) for code in codes if "_" in code} - DROPPED_FAMILIES


def ensure_figs_dir(path: Path | None = None) -> Path:
    out = path or FIGS_DIR
    out.mkdir(parents=True, exist_ok=True)
    return out


def save_figure(fig, stem: str, outdir: Path | None = None) -> list[Path]:
    out = ensure_figs_dir(outdir)
    paths = [out / f"{stem}.png", out / f"{stem}.svg"]
    fig.savefig(paths[0], dpi=240, bbox_inches="tight")
    fig.savefig(paths[1], bbox_inches="tight")
    return paths


def annotation_json_dir(periodical: str) -> Path:
    if periodical == "candil":
        base = ROOT / "candil_llm_annotation_pipeline"
    elif periodical == "jaleo":
        base = ROOT / "jaleo_llm_annotation_pipeline_v12"
    else:
        raise ValueError(f"Unknown periodical: {periodical}")
    return base / "llm_annotation_outputs_v12_backup"


def load_annotation_summary(periodical: str) -> pd.DataFrame:
    """Load accepted LLM labels directly from the validated backup JSON outputs."""
    directory = annotation_json_dir(periodical)
    if not directory.exists():
        raise FileNotFoundError(f"Missing annotation backup directory for {periodical}: {directory}")

    rows = []
    for path in sorted(directory.glob("*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        summary = data.get("article_summary") or {}
        dominant = summary.get("dominant_codes") or []
        secondary = summary.get("secondary_codes") or []
        present = []
        for ann in data.get("span_annotations") or []:
            if ann.get("decision") == "present" and ann.get("concept_id"):
                present.append(str(ann["concept_id"]))
        all_codes = sorted(set(dominant) | set(secondary) | set(present))
        rows.append(
            {
                "issue_id": data.get("issue_id"),
                "article_id": data.get("article_id"),
                "title": data.get("title"),
                "dominant_codes": "; ".join(dominant),
                "secondary_codes": "; ".join(secondary),
                "all_accepted_codes": "; ".join(all_codes),
            }
        )
    if not rows:
        raise FileNotFoundError(f"No annotation JSON outputs found for {periodical}: {directory}")
    df = pd.DataFrame(rows)

    keys = ["issue_id", "article_id"]
    if all(col in df.columns for col in keys):
        df = df.drop_duplicates(keys, keep="last")
    return df


def output_line(paths: list[Path]) -> str:
    return "Wrote " + ", ".join(str(path.relative_to(ROOT)) for path in paths)
