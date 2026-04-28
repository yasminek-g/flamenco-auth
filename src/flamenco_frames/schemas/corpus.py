from __future__ import annotations

from pydantic import BaseModel, Field


class AnnotationUnit(BaseModel):
    unit_id: str

    issue_id: str
    page_number: int | None = None
    logical_page: str | None = None

    article_id: str | None = None
    article_name: str | None = None
    article_type: str | None = None
    section_label: str | None = None
    listed_in_contents: bool | None = None
    contents_match: str | None = None
    subclass: str | None = None

    block_id: int | None = None
    global_block_id: int | None = None
    block_label: str | None = None
    block_role: str | None = None
    block_order: int | None = None
    block_bbox: list[int | float] | None = None

    text: str
    text_chars: int

    metadata: dict = Field(default_factory=dict)
    review_flags: list[str] = Field(default_factory=list)