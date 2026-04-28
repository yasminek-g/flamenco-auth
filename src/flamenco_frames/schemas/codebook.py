from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, model_validator


YesNo = Literal["yes", "no"]


class TriggerVariant(BaseModel):
    term: str
    language: str
    regex_or_seed: str
    translation_warning: str | None = None


class FewShotExample(BaseModel):
    excerpt: str
    language: str
    rationale: str
    is_good_example: YesNo
    is_borderline: YesNo
    example_type: str | None = None
    source_periodical: str | None = None
    source_json: str | None = None
    source_issue: str | None = None
    page_number: int | None = None
    provenance_match_method: str | None = None


class TheoreticalAnchor(BaseModel):
    excerpt: str | None = None
    language: str | None = None
    rationale: str | None = None
    is_good_example: YesNo | None = None
    is_borderline: YesNo | None = None
    literature_source: str | None = None
    summary: str | None = None


class Concept(BaseModel):
    label_en: str
    label_es: str
    definition: str
    use_when: str
    do_not_use_when: str
    related_concepts: str | None = None
    bilingual_alignment_note: str | None = None
    trigger_variants: list[TriggerVariant] = Field(default_factory=list)
    few_shot_examples: list[FewShotExample] = Field(default_factory=list)
    theoretical_anchors: list[TheoreticalAnchor] = Field(default_factory=list)

    @model_validator(mode="after")
    def warn_empty_operational_material(self) -> "Concept":
        if not self.trigger_variants and not self.few_shot_examples:
            raise ValueError(
                f"Concept {self.label_en!r} has neither trigger_variants nor few_shot_examples."
            )
        return self


class Family(BaseModel):
    name_en: str
    name_es: str
    purpose: str
    use_when: str
    do_not_use_when: str
    notes: str | None = None
    theoretical_grounding: list[dict] = Field(default_factory=list)
    concepts: dict[str, Concept] = Field(default_factory=dict)


class Codebook(BaseModel):
    project_name: str
    description: str | None = None
    families: dict[str, Family]
    version_note: str | None = None

    def nonempty_families(self) -> dict[str, Family]:
        return {
            family_id: family
            for family_id, family in self.families.items()
            if family.concepts
        }

    def empty_family_ids(self) -> list[str]:
        return [
            family_id
            for family_id, family in self.families.items()
            if not family.concepts
        ]