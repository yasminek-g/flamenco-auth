from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console

from flamenco_frames.io_utils import read_jsonl
from flamenco_frames.retrieval.article_router import write_article_candidates

app = typer.Typer()
console = Console()


@app.command()
def main(
    articles: Path = typer.Argument(..., help="Path to article units JSONL"),
    window_candidates: Path = typer.Argument(..., help="Path to window candidates JSONL"),
    output: Path = typer.Argument(..., help="Output article candidates JSONL"),
    max_candidate_codes: int = typer.Option(6, help="Max candidate codes per article"),
    max_windows_per_code: int = typer.Option(2, help="Max evidence windows per code"),
    max_total_windows: int = typer.Option(10, help="Max evidence windows total per article"),
    max_window_chars: int = typer.Option(700, help="Max chars per evidence window in prompt"),
    min_article_score: float = typer.Option(3.0, help="Minimum aggregated article score"),
) -> None:
    write_article_candidates(
        article_units_path=articles,
        window_candidates_path=window_candidates,
        output_path=output,
        max_candidate_codes=max_candidate_codes,
        max_windows_per_code=max_windows_per_code,
        max_total_windows=max_total_windows,
        max_window_chars=max_window_chars,
        min_article_score=min_article_score,
    )

    rows = read_jsonl(output)
    nonempty = sum(1 for row in rows if row.get("candidate_codes"))

    console.print(f"[green]Wrote {len(rows)} article candidate rows[/green] → {output}")
    console.print(f"[cyan]{nonempty} articles have at least one candidate code[/cyan]")


if __name__ == "__main__":
    app()