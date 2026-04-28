from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console

from flamenco_frames.ingest.make_evidence_windows import write_evidence_windows
from flamenco_frames.io_utils import read_jsonl

app = typer.Typer()
console = Console()


@app.command()
def main(
    articles: Path = typer.Argument(..., help="Path to article units JSONL"),
    output: Path = typer.Argument(..., help="Output evidence windows JSONL"),
    include_previous: bool = typer.Option(True, help="Include previous block in window"),
    include_next: bool = typer.Option(True, help="Include next block in window"),
    max_window_chars: int = typer.Option(900, help="Maximum characters per evidence window"),
) -> None:
    write_evidence_windows(
        article_units_path=articles,
        output_path=output,
        include_previous=include_previous,
        include_next=include_next,
        max_window_chars=max_window_chars,
    )

    rows = read_jsonl(output)
    console.print(f"[green]Wrote {len(rows)} evidence windows[/green] → {output}")


if __name__ == "__main__":
    app()