"""Plot 13 -- top frequent terms in `target` strings, faceted by family.

Tokenises the `target` field (lowercased), drops common Spanish/English stop
words, and counts top tokens per family. Useful to read what the annotator is
*pointing at* under each family without reading 7,000 free-text strings.
"""
from __future__ import annotations

import re
from collections import Counter

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

from common import (
    FAMILY_COLORS,
    FAMILY_ORDER,
    load_codes,
    output_line,
    save_figure,
    setup_theme,
)


STOPWORDS = {
    # English
    "the", "a", "an", "of", "and", "or", "to", "in", "on", "for", "as", "by",
    "with", "is", "are", "was", "be", "its", "it", "this", "that", "their",
    "from", "at", "into", "than", "but", "not", "no", "so", "such", "if",
    "vs", "versus", "rather", "one", "two", "between",
    # Spanish
    "de", "del", "la", "el", "los", "las", "y", "en", "a", "al", "que",
    "se", "su", "sus", "un", "una", "unos", "unas", "con", "por", "para",
    "como", "lo", "es", "son", "no", "ni", "o", "u", "sobre", "ante",
    "este", "esta", "estas", "estos", "ese", "esa", "esos", "esas",
    "frente", "entre",
    # noise
    "flamenco",  # ubiquitous -- swamps the chart
    "flamencas", "flamencos", "flamenca",
}
TOKEN_RE = re.compile(r"[A-Za-zÁÉÍÓÚÑáéíóúñ]+")


def tokenize(text: str) -> list[str]:
    return [t.lower() for t in TOKEN_RE.findall(text or "")]


def main() -> None:
    setup_theme()
    df = load_codes()
    df = df.dropna(subset=["family"])
    df = df[df["family"].isin(FAMILY_ORDER)]

    n_top = 12
    fig, axes = plt.subplots(2, 3, figsize=(15, 8))
    flat_axes = axes.flatten()
    for ax, fam in zip(flat_axes, FAMILY_ORDER):
        sub = df[df["family"] == fam]
        counter: Counter[str] = Counter()
        for t in sub["target"]:
            for tok in tokenize(str(t)):
                if len(tok) > 2 and tok not in STOPWORDS:
                    counter[tok] += 1
        if not counter:
            ax.axis("off")
            continue
        top = pd.DataFrame(counter.most_common(n_top), columns=["term", "count"])
        sns.barplot(
            data=top,
            y="term",
            x="count",
            color=FAMILY_COLORS[fam],
            ax=ax,
        )
        ax.set_title(fam)
        ax.set_xlabel("# target occurrences")
        ax.set_ylabel("")
        for i, v in enumerate(top["count"]):
            ax.text(v + max(top["count"]) * 0.01, i, str(int(v)), va="center", fontsize=9)
    for ax in flat_axes[len(FAMILY_ORDER):]:
        ax.axis("off")

    fig.suptitle("What annotations are pointing at -- top target terms by family", y=1.02)
    fig.tight_layout()
    paths = save_figure(fig, "plot_13_target_top_terms")
    print(output_line(paths))


if __name__ == "__main__":
    main()
