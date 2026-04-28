from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console

from flamenco_frames.io_utils import read_jsonl
from flamenco_frames.retrieval.build_codebook_docs import write_codebook_retrieval_docs

app = typer.Typer()
console = Console()


@app.command()
def main(
    codebook: Path = typer.Argument(..., help="Path to flamenco_codebook_v10.json"),
    output: Path = typer.Argument(..., help="Output retrieval_docs.jsonl path"),
) -> None:
    write_codebook_retrieval_docs(
        codebook_path=codebook,
        output_path=output,
    )

    rows = read_jsonl(output)
    console.print(f"[green]Wrote {len(rows)} retrieval docs[/green] → {output}")


if __name__ == "__main__":
    app()