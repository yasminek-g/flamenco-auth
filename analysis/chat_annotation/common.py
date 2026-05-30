"""Shared paths, palette, theme, and IO helpers.

Visual design rationale
-----------------------
The categorical palette is the **Okabe-Ito** 8-colour scheme [1]: each pair
is distinguishable under protanopia, deuteranopia, and tritanopia, the
colours hold up in print, and luminance differences keep them readable in
greyscale. It is the most widely cited colour-blind-safe palette for
scientific figures (used in Nature Methods' "Points of view" guidance).

For sequential / heatmap shading we use **viridis**: perceptually uniform,
colour-blind safe, and the de-facto standard for matplotlib science figures.

Typography and layout follow standard journal-figure conventions: white
canvas, thin black axis lines, sans-serif body, embedded (Type 42) fonts in
PDF/PS so reviewers can copy text from a figure.

[1] Okabe, M. and Ito, K., 2008. Color Universal Design (CUD) -- How to make
    figures and presentations that are friendly to colorblind people.
"""
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Iterable

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
ANALYSIS_DIR = Path(__file__).resolve().parent
DATA_DIR = ANALYSIS_DIR / "data"
FIGS_DIR = ANALYSIS_DIR / "figs"

CANDIL_DIR = ROOT / "chat_annotation_packets_candil"
JALEO_DIR = ROOT / "chat_annotation_packets_jaleo"

MPLCONFIGDIR = ROOT / "tmp" / "chat_annotation_matplotlib"
MPLCONFIGDIR.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", str(MPLCONFIGDIR))
os.environ.setdefault("XDG_CACHE_HOME", str(MPLCONFIGDIR))
os.environ.setdefault("MPLBACKEND", "Agg")


# ---------------------------------------------------------------------------
# Okabe-Ito palette
# ---------------------------------------------------------------------------
OI_BLACK = "#000000"
OI_ORANGE = "#E69F00"
OI_SKY = "#56B4E9"
OI_GREEN = "#009E73"
OI_YELLOW = "#F0E442"
OI_BLUE = "#0072B2"
OI_VERMILLION = "#D55E00"
OI_PURPLE = "#CC79A7"

# Neutrals for axes / grids / annotations.
NEUTRAL_DARK = "#222222"
NEUTRAL = "#555555"
NEUTRAL_MID = "#888888"
NEUTRAL_LIGHT = "#bbbbbb"
NEUTRAL_PALE = "#e6e6e6"


# ---------------------------------------------------------------------------
# Semantic colour mappings
# ---------------------------------------------------------------------------
PERIODICAL_ORDER = ["Candil", "Jaleo"]
PERIODICAL_PALETTE = {
    "Candil": OI_BLUE,
    "Jaleo": OI_VERMILLION,
}

FAMILY_ORDER = ["AUTH", "CRIT", "COMM", "PED", "HERIT"]
FAMILY_COLORS = {
    "AUTH": OI_PURPLE,
    "CRIT": OI_SKY,
    "COMM": OI_GREEN,
    "PED": OI_ORANGE,
    "HERIT": NEUTRAL,
}

CONFIDENCE_ORDER = ["high", "medium", "low"]
CONFIDENCE_COLORS = {
    "high": OI_BLUE,
    "medium": OI_SKY,
    "low": NEUTRAL_LIGHT,
}

POLARITY_ORDER = [
    "legitimating",
    "mixed",
    "contested",
    "delegitimating",
    "neutral",
    "unclear",
]
POLARITY_COLORS = {
    "legitimating": OI_BLUE,
    "mixed": OI_SKY,
    "contested": OI_PURPLE,
    "delegitimating": OI_VERMILLION,
    "neutral": NEUTRAL_MID,
    "unclear": NEUTRAL_PALE,
}

BASIS_ORDER = [
    "authenticity",
    "tradition",
    "heritage",
    "community",
    "pedagogy",
    "criticism",
]

AUTH_CODES = ["AUTH_01", "AUTH_02", "AUTH_03", "AUTH_04"]
AUTH_LABELS = {
    "AUTH_01": "authenticity claim",
    "AUTH_02": "purity / boundary",
    "AUTH_03": "tradition / continuity",
    "AUTH_04": "anti-commercial / intimate",
}
# Four perceptually distinct hues for the AUTH subtypes. Picked from the OI
# palette so they slot into any AUTH-only figure without recolouring.
AUTH_SUBCODE_COLORS = {
    "AUTH_01": OI_PURPLE,
    "AUTH_02": OI_BLUE,
    "AUTH_03": OI_ORANGE,
    "AUTH_04": OI_GREEN,
}

# Sequential / continuous colormap (used for every heatmap).
SEQUENTIAL_CMAP = "viridis"


# ---------------------------------------------------------------------------
# Theme
# ---------------------------------------------------------------------------
def setup_theme() -> None:
    """Apply the paper-figure theme: white canvas, thin axes, Type 42 fonts."""
    import matplotlib

    matplotlib.use("Agg", force=True)
    import matplotlib.pyplot as plt
    import seaborn as sns

    sns.set_theme(style="ticks", context="paper", font_scale=1.1)
    plt.rcParams.update(
        {
            "figure.facecolor": "white",
            "axes.facecolor": "white",
            "savefig.facecolor": "white",
            "axes.edgecolor": NEUTRAL_DARK,
            "axes.linewidth": 0.8,
            "axes.labelcolor": NEUTRAL_DARK,
            "axes.titlecolor": NEUTRAL_DARK,
            "axes.titleweight": "semibold",
            "axes.titlesize": 11,
            "axes.titlepad": 8,
            "axes.labelsize": 10,
            "axes.labelpad": 5,
            "xtick.color": NEUTRAL,
            "ytick.color": NEUTRAL,
            "xtick.labelsize": 9,
            "ytick.labelsize": 9,
            "xtick.major.width": 0.7,
            "ytick.major.width": 0.7,
            "xtick.major.size": 3,
            "ytick.major.size": 3,
            "legend.fontsize": 9,
            "legend.frameon": False,
            "axes.grid": True,
            "axes.axisbelow": True,
            "grid.color": NEUTRAL_PALE,
            "grid.linewidth": 0.6,
            "grid.linestyle": "-",
            "axes.spines.top": False,
            "axes.spines.right": False,
            "font.family": "DejaVu Sans",
            "font.size": 10,
            # Type 42 ("TrueType") so PDF/PS submissions ship editable text.
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
            "svg.fonttype": "none",
            "figure.dpi": 120,
            "savefig.dpi": 300,
            "savefig.bbox": "tight",
            "image.cmap": SEQUENTIAL_CMAP,
        }
    )


def ensure_figs_dir(path: Path | None = None) -> Path:
    out = path or FIGS_DIR
    out.mkdir(parents=True, exist_ok=True)
    return out


def save_figure(fig, stem: str, outdir: Path | None = None) -> list[Path]:
    out = ensure_figs_dir(outdir)
    paths = [out / f"{stem}.png", out / f"{stem}.svg"]
    fig.savefig(paths[0], dpi=300, bbox_inches="tight")
    fig.savefig(paths[1], bbox_inches="tight")
    return paths


def output_line(paths: list[Path]) -> str:
    return "Wrote " + ", ".join(str(p.relative_to(ROOT)) for p in paths)


# ---------------------------------------------------------------------------
# Loaders -- read the CSVs produced by build_dataset.py
# ---------------------------------------------------------------------------


def _require(path: Path) -> Path:
    if not path.exists():
        raise FileNotFoundError(
            f"Missing {path}. Run `python analysis/chat_annotation/build_dataset.py` first."
        )
    return path


def load_articles() -> pd.DataFrame:
    return pd.read_csv(_require(DATA_DIR / "articles.csv"))


def load_codes() -> pd.DataFrame:
    return pd.read_csv(_require(DATA_DIR / "codes.csv"))


def load_possible() -> pd.DataFrame:
    return pd.read_csv(_require(DATA_DIR / "possible.csv"))


def load_basis() -> pd.DataFrame:
    return pd.read_csv(_require(DATA_DIR / "basis.csv"))


# ---------------------------------------------------------------------------
# Small utilities
# ---------------------------------------------------------------------------


def family_of(code: str | None) -> str | None:
    if not isinstance(code, str) or "_" not in code:
        return None
    fam = code.split("_", 1)[0]
    return fam if fam in FAMILY_ORDER else None


def periodical_label(periodical: str) -> str:
    return {"candil": "Candil", "jaleo": "Jaleo"}.get(periodical.lower(), periodical)


def issue_to_year(issue_id: str) -> int | None:
    if not isinstance(issue_id, str):
        return None
    m = re.search(r"(19|20)\d{2}", issue_id)
    return int(m.group(0)) if m else None


def jaccard(a: Iterable, b: Iterable) -> float:
    sa, sb = set(a), set(b)
    if not sa and not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)
