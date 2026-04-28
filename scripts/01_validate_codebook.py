from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from flamenco_frames.io_utils import read_json
from flamenco_frames.schemas.codebook import Codebook

app = typer.Typer()
console = Console()


@app.command()
def main(
    codebook: Path = typer.Argument(..., help="Path to flamenco_codebook_v10.json"),
) -> None:
    raw = read_json(codebook)
    parsed = Codebook.model_validate(raw)

    table = Table(title="Codebook validation summary")
    table.add_column("Family")
    table.add_column("Concepts", justify="right")
    table.add_column("Status")

    for family_id, family in parsed.families.items():
        n_concepts = len(family.concepts)
        status = "OK" if n_concepts else "EMPTY - skipped during retrieval"
        table.add_row(family_id, str(n_concepts), status)

    console.print(table)

    empty = parsed.empty_family_ids()
    if empty:
        console.print(f"[yellow]Empty families:[/yellow] {', '.join(empty)}")

    console.print("[green]Codebook is structurally valid.[/green]")


if __name__ == "__main__":
    app()