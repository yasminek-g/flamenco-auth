from __future__ import annotations

from pathlib import Path
from typing import Any

from flamenco_frames.io_utils import read_json, write_jsonl
from flamenco_frames.schemas.codebook import Codebook


def as_bool(value: str | bool | None) -> bool | None:
    if isinstance(value, bool):
        return value
    if value == "yes":
        return True
    if value == "no":
        return False
    return None


def build_trigger_doc(
    *,
    family_id: str,
    concept_id: str,
    trigger_index: int,
    trigger: Any,
) -> dict[str, Any]:
    return {
        "doc_id": f"{concept_id}__trigger__{trigger.language}__{trigger_index:04d}",
        "doc_type": "trigger_variant",
        "family_id": family_id,
        "concept_id": concept_id,
        "language": trigger.language,
        "text_for_embedding": " ".join(
            part
            for part in [
                trigger.term,
                trigger.regex_or_seed,
                trigger.translation_warning,
            ]
            if part
        ),
        "metadata": {
            "term": trigger.term,
            "regex_or_seed": trigger.regex_or_seed,
            "translation_warning": trigger.translation_warning,
        },
    }


def build_example_doc(
    *,
    family_id: str,
    concept_id: str,
    example_index: int,
    example: Any,
) -> dict[str, Any]:
    is_good = as_bool(example.is_good_example)
    is_borderline = as_bool(example.is_borderline)

    if is_good is True:
        polarity = "positive_example"
    elif is_good is False:
        polarity = "trap_example"
    else:
        polarity = "unknown_example"

    return {
        "doc_id": f"{concept_id}__{polarity}__{example.language}__{example_index:04d}",
        "doc_type": polarity,
        "family_id": family_id,
        "concept_id": concept_id,
        "language": example.language,
        "text_for_embedding": " ".join(
            part
            for part in [
                example.excerpt,
                example.rationale,
                example.example_type,
            ]
            if part
        ),
        "metadata": {
            "excerpt": example.excerpt,
            "rationale": example.rationale,
            "is_good_example": is_good,
            "is_borderline": is_borderline,
            "example_type": example.example_type,
            "source_periodical": example.source_periodical,
            "source_issue": example.source_issue,
            "page_number": example.page_number,
        },
    }


def build_codebook_retrieval_docs(codebook: Codebook) -> list[dict[str, Any]]:
    docs: list[dict[str, Any]] = []

    for family_id, family in codebook.nonempty_families().items():
        for concept_id, concept in family.concepts.items():
            for i, trigger in enumerate(concept.trigger_variants):
                docs.append(
                    build_trigger_doc(
                        family_id=family_id,
                        concept_id=concept_id,
                        trigger_index=i,
                        trigger=trigger,
                    )
                )

            for i, example in enumerate(concept.few_shot_examples):
                docs.append(
                    build_example_doc(
                        family_id=family_id,
                        concept_id=concept_id,
                        example_index=i,
                        example=example,
                    )
                )

    return docs


def write_codebook_retrieval_docs(
    codebook_path: str | Path,
    output_path: str | Path,
) -> None:
    raw = read_json(codebook_path)
    codebook = Codebook.model_validate(raw)
    docs = build_codebook_retrieval_docs(codebook)
    write_jsonl(output_path, docs)