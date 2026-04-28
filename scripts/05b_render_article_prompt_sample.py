from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console

from flamenco_frames.io_utils import read_jsonl
from flamenco_frames.prompting.family_context import load_codebook
from flamenco_frames.prompting.article_prompt_builder import build_article_annotation_prompt

app = typer.Typer()
console = Console()


@app.command()
def main(
    codebook: Path = typer.Argument(..., help="Path to flamenco_codebook_v10.json"),
    article_candidates: Path = typer.Argument(..., help="Path to article candidates JSONL"),
    output_dir: Path = typer.Argument(..., help="Directory where article prompts will be written"),
    limit: int = typer.Option(10, help="Number of prompts to render"),
    start: int = typer.Option(0, help="Start offset into article candidates JSONL"),
    include_empty: bool = typer.Option(False, help="Render articles with no candidate codes"),
    only_article_contains: str | None = typer.Option(
        None,
        help="Optional substring filter on article_name",
    ),
) -> None:
    parsed_codebook = load_codebook(str(codebook))
    rows = read_jsonl(article_candidates)

    output_dir.mkdir(parents=True, exist_ok=True)

    written = 0

    for i, row in enumerate(rows[start:], start=start):
        if written >= limit:
            break

        if not include_empty and not row.get("candidate_codes"):
            continue

        if only_article_contains:
            article_name = str(row.get("article_name") or "").lower()
            if only_article_contains.lower() not in article_name:
                continue

        prompt = build_article_annotation_prompt(
            codebook=parsed_codebook,
            article_candidate_row=row,
        )

        article_id = str(row.get("article_id") or f"article_{i}")
        safe_article_id = (
            article_id.replace("/", "_")
            .replace(" ", "_")
            .replace(":", "_")
        )

        output_path = output_dir / f"{i:04d}__{safe_article_id}.article_prompt.txt"
        output_path.write_text(prompt, encoding="utf-8")

        written += 1

    console.print(f"[green]Wrote {written} article prompt files[/green] → {output_dir}")

    if written == 0:
        console.print("[yellow]No prompts were written. Try --include-empty or remove filters.[/yellow]")


if __name__ == "__main__":
    app()