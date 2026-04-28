from __future__ import annotations

from typing import Any

from flamenco_frames.io_utils import read_json
from flamenco_frames.schemas.codebook import Codebook, Concept, Family


def load_codebook(codebook_path: str) -> Codebook:
    return Codebook.model_validate(read_json(codebook_path))


def get_candidate_family_ids(candidate_row: dict[str, Any]) -> list[str]:
    family_ids = set(candidate_row.get("candidate_families") or [])

    for candidate in candidate_row.get("candidates", []):
        family_id = candidate.get("family_id")
        if family_id:
            family_ids.add(family_id)

    return sorted(family_ids)


def example_to_prompt_dict(example: Any) -> dict[str, Any]:
    return {
        "excerpt": example.excerpt,
        "language": example.language,
        "rationale": example.rationale,
        "is_good_example": example.is_good_example,
        "is_borderline": example.is_borderline,
        "example_type": example.example_type,
        "source_periodical": example.source_periodical,
        "source_issue": example.source_issue,
        "page_number": example.page_number,
    }


def trigger_to_prompt_dict(trigger: Any) -> dict[str, Any]:
    return {
        "term": trigger.term,
        "language": trigger.language,
        "regex_or_seed": trigger.regex_or_seed,
        "translation_warning": trigger.translation_warning,
    }


def concept_to_prompt_dict(
    concept_id: str,
    concept: Concept,
    max_examples_per_concept: int = 4,
) -> dict[str, Any]:
    positive_examples = []
    trap_examples = []
    borderline_examples = []

    for example in concept.few_shot_examples:
        if example.is_good_example == "yes":
            positive_examples.append(example_to_prompt_dict(example))
        elif example.is_good_example == "no":
            trap_examples.append(example_to_prompt_dict(example))

        if example.is_borderline == "yes":
            borderline_examples.append(example_to_prompt_dict(example))

    return {
        "concept_id": concept_id,
        "label_en": concept.label_en,
        "label_es": concept.label_es,
        "definition": concept.definition,
        "use_when": concept.use_when,
        "do_not_use_when": concept.do_not_use_when,
        "related_concepts": concept.related_concepts,
        "bilingual_alignment_note": concept.bilingual_alignment_note,
        "trigger_variants": [
            trigger_to_prompt_dict(trigger)
            for trigger in concept.trigger_variants
        ],
        "positive_examples": positive_examples[:max_examples_per_concept],
        "trap_examples": trap_examples[:max_examples_per_concept],
        "borderline_examples": borderline_examples[:max_examples_per_concept],
    }


def family_to_prompt_dict(
    family_id: str,
    family: Family,
    candidate_concept_ids: set[str] | None = None,
    include_all_sibling_concepts: bool = True,
    max_examples_per_concept: int = 4,
) -> dict[str, Any]:
    candidate_concept_ids = candidate_concept_ids or set()

    concepts: dict[str, Any] = {}

    for concept_id, concept in family.concepts.items():
        if include_all_sibling_concepts or concept_id in candidate_concept_ids:
            concepts[concept_id] = concept_to_prompt_dict(
                concept_id=concept_id,
                concept=concept,
                max_examples_per_concept=max_examples_per_concept,
            )

    return {
        "family_id": family_id,
        "name_en": family.name_en,
        "name_es": family.name_es,
        "purpose": family.purpose,
        "use_when": family.use_when,
        "do_not_use_when": family.do_not_use_when,
        "notes": family.notes,
        "concepts": concepts,
    }


def build_family_context(
    codebook: Codebook,
    candidate_row: dict[str, Any],
    include_all_sibling_concepts: bool = True,
    max_examples_per_concept: int = 4,
) -> list[dict[str, Any]]:
    """
    Build family-level context for the prompt.

    If a candidate includes AUTH_01, the prompt should receive the full AUTH family,
    not just AUTH_01. This is important because codes are theoretically adjacent and
    boundaries are often decided by sibling concepts and negative guardrails.
    """
    family_ids = get_candidate_family_ids(candidate_row)

    candidate_concepts_by_family: dict[str, set[str]] = {}

    for candidate in candidate_row.get("candidates", []):
        family_id = candidate.get("family_id")
        concept_id = candidate.get("concept_id")

        if not family_id or not concept_id:
            continue

        candidate_concepts_by_family.setdefault(family_id, set()).add(concept_id)

    contexts: list[dict[str, Any]] = []

    for family_id in family_ids:
        family = codebook.families.get(family_id)

        if family is None:
            continue

        contexts.append(
            family_to_prompt_dict(
                family_id=family_id,
                family=family,
                candidate_concept_ids=candidate_concepts_by_family.get(family_id, set()),
                include_all_sibling_concepts=include_all_sibling_concepts,
                max_examples_per_concept=max_examples_per_concept,
            )
        )

    return contexts