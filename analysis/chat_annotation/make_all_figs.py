"""Rebuild data CSVs + every figure under analysis/chat_annotation/figs.

Run from the repo root:
    ./flamenco-env/bin/python analysis/chat_annotation/make_all_figs.py
"""
from __future__ import annotations

import importlib
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

# Step 1: rebuild data CSVs.
import build_dataset  # noqa: E402

build_dataset.build()

# Step 2: run every plot script in order.
PLOTS = [
    "plot_01_coverage",
    "plot_02_codes_per_article",
    "plot_03_family_emission",
    "plot_04_code_heatmap",
    "plot_05_confidence_by_family",
    "plot_06_polarity_distribution",
    "plot_07_basis_distribution",
    "plot_08_legitimation_and_boundaries",
    "plot_09_polarity_basis_heatmap",
    "plot_10_possible_top_codes",
    "plot_11_emit_vs_possible",
    "plot_12_rationale_length",
    "plot_13_target_top_terms",
    "plot_14_temporal_family",
    "plot_15_auth_internal_overlap",
    "plot_16_auth_family_overlap",
    "plot_17_auth_target_terms",
    "plot_18_auth_polarity_boundaries",
    "plot_19_auth_temporal",
    "plot_20_auth_emit_vs_possible",
]

for name in PLOTS:
    print(f"-- {name}")
    mod = importlib.import_module(name)
    mod.main()
