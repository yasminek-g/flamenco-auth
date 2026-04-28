from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console

from flamenco_frames.ingest.make_annotation_units import write_annotation_units
from flamenco_frames.io_utils import read_jsonl

app = typer.Typer()
console = Console()


@app.command()
def main(
    issue_json: Path = typer.Argument(..., help="Path to *_tagged.json"),
    output: Path = typer.Argument(..., help="Output annotation_units.jsonl path"),
    min_chars: int = typer.Option(80, help="Minimum text length for annotation units"),
) -> None:
    write_annotation_units(
        issue_path=issue_json,
        output_path=output,
        min_chars=min_chars,
    )

    rows = read_jsonl(output)
    console.print(f"[green]Wrote {len(rows)} annotation units[/green] → {output}")


if __name__ == "__main__":
    app()