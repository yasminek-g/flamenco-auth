"""Plot 17 -- top target terms among AUTH emissions, faceted by AUTH subcode.

Shows what authenticity claims are *about* per subtype: what
authenticity_claim points at vs. purity-boundary work vs. tradition vs.
anti-commercial -- per periodical.
"""
from __future__ import annotations

import re
from collections import Counter

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

from common import (
    NEUTRAL_LIGHT,
    AUTH_CODES,
    AUTH_LABELS,
    PERIODICAL_ORDER,
    PERIODICAL_PALETTE,
    load_codes,
    output_line,
    save_figure,
    setup_theme,
)


STOPWORDS = {
    "the", "a", "an", "of", "and", "or", "to", "in", "on", "for", "as", "by",
    "with", "is", "are", "was", "be", "its", "it", "this", "that", "from",
    "their", "vs", "versus", "rather", "than", "between",
    "de", "del", "la", "el", "los", "las", "y", "en", "a", "al", "que",
    "se", "su", "sus", "un", "una", "unos", "unas", "con", "por", "para",
    "como", "lo", "es", "son", "no", "ni", "o", "u", "sobre", "ante",
    "frente", "entre",
    "flamenco", "flamencas", "flamencos", "flamenca",
}
TOKEN_RE = re.compile(r"[A-Za-zÁÉÍÓÚÑáéíóúñ]+")


def tokens(text: str) -> list[str]:
    return [t.lower() for t in TOKEN_RE.findall(text or "") if len(t) > 2 and t.lower() not in STOPWORDS]


def main() -> None:
    setup_theme()
    df = load_codes()
    df = df[df["code"].isin(AUTH_CODES)]

    fig, axes = plt.subplots(2, 4, figsize=(16, 8.5), sharex="col")
    n_top = 10
    for col, code in enumerate(AUTH_CODES):
        for row, periodical in enumerate(PERIODICAL_ORDER):
            ax = axes[row, col]
            sub = df[(df["code"] == code) & (df["periodical_label"] == periodical)]
            counter: Counter[str] = Counter()
            for t in sub["target"]:
                for tok in tokens(str(t)):
                    counter[tok] += 1
            if not counter:
                ax.text(0.5, 0.5, "(no data)", ha="center", va="center", transform=ax.transAxes, color=NEUTRAL_LIGHT)
                ax.set_xticks([])
                ax.set_yticks([])
                ax.set_title("" if row else f"{code}\n{AUTH_LABELS[code]}", fontsize=11)
                continue
            top = pd.DataFrame(counter.most_common(n_top), columns=["term", "count"])
            sns.barplot(
                data=top,
                y="term",
                x="count",
                color=PERIODICAL_PALETTE[periodical],
                ax=ax,
            )
            for i, v in enumerate(top["count"]):
                ax.text(v + max(top["count"]) * 0.02, i, str(int(v)), va="center", fontsize=8)
            ax.set_xlabel("")
            ax.set_ylabel(periodical if col == 0 else "")
            if row == 0:
                ax.set_title(f"{code}\n{AUTH_LABELS[code]}", fontsize=11)

    fig.suptitle("What AUTH is pointing at -- top target terms by subcode x periodical", y=1.01)
    fig.tight_layout()
    paths = save_figure(fig, "plot_17_auth_target_terms")
    print(output_line(paths))


if __name__ == "__main__":
    main()
