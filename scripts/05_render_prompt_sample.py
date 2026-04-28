from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console

from flamenco_frames.io_utils import read_jsonl
from flamenco_frames.prompting.family_context import load_codebook
from flamenco_frames.prompting.prompt_builder import build_annotation_prompt

app = typer.Typer()
console = Console()


@app.command()
def main(
    codebook: Path = typer.Argument(..., help="Path to flamenco_codebook_v10.json"),
    candidates: Path = typer.Argument(..., help="Path to candidate codes JSONL"),
    output_dir: Path = typer.Argument(..., help="Directory where prompt text files will be written"),
    language: str = typer.Option("es", help="Language of the corpus units, e.g. es or en"),
    limit: int = typer.Option(10, help="Number of prompts to render"),
    start: int = typer.Option(0, help="Start offset into the candidates JSONL"),
    include_empty: bool = typer.Option(False, help="Render prompts even for rows with no candidates"),
    max_examples_per_concept: int = typer.Option(3, help="Max examples per concept in prompt"),
    only_article_contains: str | None = typer.Option(
        None,
        help="Optional substring filter on article_name",
    ),
) -> None:
    parsed_codebook = load_codebook(str(codebook))
    rows = read_jsonl(candidates)

    output_dir.mkdir(parents=True, exist_ok=True)

    written = 0

    for i, row in enumerate(rows[start:], start=start):
        if written >= limit:
            break

        if not include_empty and not row.get("candidates"):
            continue

        if only_article_contains:
            article_name = str(row.get("article_name") or "").lower()
            if only_article_contains.lower() not in article_name:
                continue

        prompt = build_annotation_prompt(
            codebook=parsed_codebook,
            candidate_row=row,
            language=language,
            include_all_sibling_concepts=True,
            max_examples_per_concept=max_examples_per_concept,
        )

        unit_id = str(row.get("unit_id") or f"row_{i}")
        safe_unit_id = (
            unit_id.replace("/", "_")
            .replace(" ", "_")
            .replace(":", "_")
        )

        output_path = output_dir / f"{i:04d}__{safe_unit_id}.prompt.txt"
        output_path.write_text(prompt, encoding="utf-8")

        written += 1

    console.print(f"[green]Wrote {written} prompt files[/green] → {output_dir}")

    if written == 0:
        console.print("[yellow]No prompts were written. Try --include-empty or remove filters.[/yellow]")


if __name__ == "__main__":
    app()