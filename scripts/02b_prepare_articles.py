from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console

from flamenco_frames.ingest.make_article_units import write_article_units
from flamenco_frames.io_utils import read_jsonl

app = typer.Typer()
console = Console()


@app.command()
def main(
    block_units: Path = typer.Argument(..., help="Path to block annotation units JSONL"),
    output: Path = typer.Argument(..., help="Output article units JSONL"),
) -> None:
    write_article_units(
        block_units_path=block_units,
        output_path=output,
    )

    rows = read_jsonl(output)
    console.print(f"[green]Wrote {len(rows)} article units[/green] → {output}")


if __name__ == "__main__":
    app()