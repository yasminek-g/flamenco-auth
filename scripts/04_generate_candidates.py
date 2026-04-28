from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console

from flamenco_frames.io_utils import read_jsonl
from flamenco_frames.retrieval.sparse_router import generate_candidate_file

app = typer.Typer()
console = Console()


@app.command()
def main(
    codebook: Path = typer.Argument(..., help="Path to flamenco_codebook_v10.json"),
    retrieval_docs: Path = typer.Argument(..., help="Path to codebook retrieval docs JSONL"),
    annotation_units: Path = typer.Argument(..., help="Path to annotation units JSONL"),
    output: Path = typer.Argument(..., help="Output candidate codes JSONL"),
    language: str = typer.Option(
        "es",
        help="Restrict sparse routing to one language, e.g. 'es' or 'en'. Use '' for all.",
    ),
    bm25_top_k: int = typer.Option(12, help="Number of BM25 docs to retrieve per unit"),
    final_k: int = typer.Option(8, help="Final number of candidate concepts per unit"),
    min_bm25_only_score: float = typer.Option(
        3.0,
        help="Minimum score for BM25-only candidates. Regex-supported candidates are handled separately.",
    ),
    min_regex_only_score: float = typer.Option(
        4.0,
        help="Minimum score for regex-only candidates.",
    ),
) -> None:
    language_filter = language.strip() or None

    generate_candidate_file(
        codebook_path=str(codebook),
        retrieval_docs_path=str(retrieval_docs),
        annotation_units_path=str(annotation_units),
        output_path=str(output),
        language=language_filter,
        bm25_top_k=bm25_top_k,
        final_k=final_k,
        min_bm25_only_score=min_bm25_only_score,
        min_regex_only_score=min_regex_only_score,
    )

    rows = read_jsonl(output)
    nonempty = sum(1 for row in rows if row["candidates"])

    console.print(f"[green]Wrote {len(rows)} candidate rows[/green] → {output}")
    console.print(f"[cyan]{nonempty} rows have at least one candidate code[/cyan]")

    if language_filter:
        console.print(f"[cyan]Sparse routing language filter:[/cyan] {language_filter}")
    else:
        console.print("[yellow]Sparse routing language filter disabled; using all languages.[/yellow]")


if __name__ == "__main__":
    app()