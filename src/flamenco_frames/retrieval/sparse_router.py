from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from rank_bm25 import BM25Okapi

from flamenco_frames.io_utils import read_json, read_jsonl, write_jsonl
from flamenco_frames.schemas.codebook import Codebook


TOKEN_RE = re.compile(r"\w+", re.UNICODE)

SPANISH_STOPWORDS = {
    "el", "la", "los", "las", "un", "una", "unos", "unas",
    "de", "del", "a", "al", "en", "por", "para", "con", "sin",
    "y", "o", "u", "que", "se", "su", "sus", "es", "son", "ser",
    "como", "más", "mas", "pero", "no", "si", "sí",
    "nuestro", "nuestra", "nuestros", "nuestras",
    "este", "esta", "estos", "estas", "ese", "esa", "esos", "esas",
    "esto", "aquello", "aquella", "aquellos", "aquellas",
    "lo", "le", "les", "ha", "han", "he", "hemos", "fue", "era", "eran",
    "hace", "todo", "toda", "todos", "todas",
    "muy", "menos", "también", "tambien",
    "desde", "hasta", "sobre", "entre", "dentro", "fuera",
    "hay", "había", "habia", "hubo", "tener", "tiene", "tienen",
    "algo", "alguien",
}

ENGLISH_STOPWORDS = {
    "the", "a", "an", "of", "and", "or", "to", "in", "on", "for",
    "with", "without", "is", "are", "was", "were", "be", "been",
    "this", "that", "these", "those", "our", "your", "their",
    "as", "by", "from", "it", "its", "they", "them", "we", "you",
}

STOPWORDS = SPANISH_STOPWORDS | ENGLISH_STOPWORDS


def tokenize(text: str) -> list[str]:
    tokens = TOKEN_RE.findall(text.lower())
    return [
        token
        for token in tokens
        if token not in STOPWORDS and len(token) > 2
    ]


@dataclass
class RegexHit:
    family_id: str
    concept_id: str
    term: str
    language: str
    pattern: str
    original_pattern: str
    matched_text: str


@dataclass
class CandidateCode:
    family_id: str
    concept_id: str
    score: float
    sources: list[str]
    regex_hits: list[dict[str, Any]]
    bm25_hits: list[dict[str, Any]]


def looks_like_stem_pattern(pattern: str) -> bool:
    """
    Detect compact stem-style trigger patterns.

    Examples:
    - reconoc should match reconocido, reconocimiento, reconocer
    - autoriz should match autorizado, autorización
    - legitim should match legitimar, legitimidad

    But we should not stem-wrap phrase patterns or already boundary-aware regexes.
    """
    if " " in pattern or r"\s" in pattern:
        return False

    if any(
        token in pattern
        for token in [
            r"\b",
            "^",
            "$",
            ".*",
            ".+",
            "(?:",
            "(?=",
            "(?!",
            "(?<=",
            "(?<!",
        ]
    ):
        return False

    if "|" in pattern:
        return True

    if re.fullmatch(r"[A-Za-zÀ-ÿ\[\]\(\)\|\\íúéáóÍÚÉÁÓñÑ]+", pattern):
        return len(pattern) >= 4

    return False


def normalize_regex_pattern(pattern: str) -> str:
    """
    Normalize trigger regexes to reduce substring false positives while preserving
    stem-style matching.

    Prevents:
    - men matching inside tratamiento
    - public matching inside publicación

    Preserves:
    - reconoc matching reconocido/reconocimiento
    - autoriz matching autorizado/autorización
    """

    pattern = pattern.strip()

    already_boundary_or_phrase_aware = any(
        token in pattern
        for token in [
            r"\b",
            "^",
            "$",
            r"\s",
            r"\W",
            ".*",
            ".+",
            "(?:",
            "(?=",
            "(?!",
            "(?<=",
            "(?<!",
        ]
    )

    if already_boundary_or_phrase_aware:
        return pattern

    if looks_like_stem_pattern(pattern):
        parts = pattern.split("|")
        stem_parts = []

        for part in parts:
            part = part.strip()
            if not part:
                continue

            stem_parts.append(rf"{part}\w*")

        if stem_parts:
            return rf"\b(?:{'|'.join(stem_parts)})\b"

    return rf"\b(?:{pattern})\b"


def safe_compile(pattern: str) -> tuple[re.Pattern[str] | None, str]:
    normalized = normalize_regex_pattern(pattern)

    try:
        return re.compile(normalized, flags=re.IGNORECASE | re.UNICODE), normalized
    except re.error:
        return None, normalized


def normalize_language_filter(language: str | None) -> str | None:
    if language is None:
        return None

    language = language.strip().lower()

    if not language:
        return None

    return language


def build_regex_patterns(
    codebook: Codebook,
    language: str | None = None,
) -> list[dict[str, Any]]:
    language = normalize_language_filter(language)
    patterns: list[dict[str, Any]] = []

    for family_id, family in codebook.nonempty_families().items():
        for concept_id, concept in family.concepts.items():
            for trigger in concept.trigger_variants:
                trigger_language = str(trigger.language).strip().lower()

                if language and trigger_language != language:
                    continue

                compiled, normalized_pattern = safe_compile(trigger.regex_or_seed)

                if compiled is None:
                    continue

                patterns.append(
                    {
                        "family_id": family_id,
                        "concept_id": concept_id,
                        "term": trigger.term,
                        "language": trigger_language,
                        "original_pattern": trigger.regex_or_seed,
                        "normalized_pattern": normalized_pattern,
                        "compiled": compiled,
                    }
                )

    return patterns


def find_regex_hits(text: str, patterns: list[dict[str, Any]]) -> list[RegexHit]:
    hits: list[RegexHit] = []

    for item in patterns:
        compiled = item["compiled"]

        for match in compiled.finditer(text):
            hits.append(
                RegexHit(
                    family_id=item["family_id"],
                    concept_id=item["concept_id"],
                    term=item["term"],
                    language=item["language"],
                    pattern=item["normalized_pattern"],
                    original_pattern=item["original_pattern"],
                    matched_text=match.group(0),
                )
            )

    return hits


class BM25CodebookIndex:
    def __init__(
        self,
        retrieval_docs: list[dict[str, Any]],
        language: str | None = None,
    ) -> None:
        language = normalize_language_filter(language)

        if language:
            retrieval_docs = [
                doc
                for doc in retrieval_docs
                if str(doc.get("language") or "").strip().lower() == language
            ]

        self.docs = retrieval_docs
        self.tokenized_docs = [
            tokenize(doc["text_for_embedding"])
            for doc in retrieval_docs
        ]

        self.index = BM25Okapi(self.tokenized_docs)

    def search(self, text: str, top_k: int = 12) -> list[dict[str, Any]]:
        query_tokens = tokenize(text)

        if not query_tokens or not self.docs:
            return []

        scores = self.index.get_scores(query_tokens)

        ranked_indices = sorted(
            range(len(scores)),
            key=lambda i: scores[i],
            reverse=True,
        )[:top_k]

        results: list[dict[str, Any]] = []

        for i in ranked_indices:
            score = float(scores[i])
            if score <= 0:
                continue

            doc = self.docs[i]
            results.append(
                {
                    "doc_id": doc["doc_id"],
                    "doc_type": doc["doc_type"],
                    "family_id": doc["family_id"],
                    "concept_id": doc["concept_id"],
                    "language": doc.get("language"),
                    "score": score,
                    "text_for_embedding": doc["text_for_embedding"],
                    "metadata": doc.get("metadata", {}),
                }
            )

        return results


def bm25_contribution(hit: dict[str, Any], bm25_weight: float) -> float:
    doc_type = hit["doc_type"]
    raw_score = float(hit["score"])

    if doc_type == "positive_example":
        multiplier = 1.0
    elif doc_type == "trigger_variant":
        multiplier = 0.85
    elif doc_type == "trap_example":
        multiplier = 0.25
    else:
        multiplier = 0.5

    return bm25_weight * multiplier * raw_score


def combine_candidates(
    regex_hits: list[RegexHit],
    bm25_hits: list[dict[str, Any]],
    regex_weight: float = 2.0,
    bm25_weight: float = 1.0,
    final_k: int = 8,
    min_bm25_only_score: float = 3.0,
    min_regex_only_score: float = 4.0,
) -> list[CandidateCode]:
    grouped: dict[tuple[str, str], dict[str, Any]] = defaultdict(
        lambda: {
            "score": 0.0,
            "sources": set(),
            "regex_hits": [],
            "bm25_hits": [],
        }
    )

    seen_regex_hits: set[tuple[str, str, str, str]] = set()

    for hit in regex_hits:
        seen_key = (
            hit.family_id,
            hit.concept_id,
            hit.original_pattern,
            hit.matched_text.lower(),
        )

        if seen_key in seen_regex_hits:
            continue

        seen_regex_hits.add(seen_key)

        key = (hit.family_id, hit.concept_id)
        grouped[key]["score"] += regex_weight
        grouped[key]["sources"].add("regex")
        grouped[key]["regex_hits"].append(
            {
                "term": hit.term,
                "language": hit.language,
                "pattern": hit.pattern,
                "original_pattern": hit.original_pattern,
                "matched_text": hit.matched_text,
            }
        )

    for hit in bm25_hits:
        key = (hit["family_id"], hit["concept_id"])
        contribution = bm25_contribution(hit, bm25_weight=bm25_weight)

        grouped[key]["score"] += contribution
        grouped[key]["sources"].add("bm25")
        grouped[key]["bm25_hits"].append(
            {
                "doc_id": hit["doc_id"],
                "doc_type": hit["doc_type"],
                "score": hit["score"],
                "contribution": contribution,
                "language": hit.get("language"),
                "metadata": hit.get("metadata", {}),
            }
        )

    candidates: list[CandidateCode] = []

    for (family_id, concept_id), payload in grouped.items():
        sources = sorted(payload["sources"])
        score = float(payload["score"])

        if "regex" not in sources and score < min_bm25_only_score:
            continue

        if sources == ["regex"]:
            regex_hits_payload = payload["regex_hits"]
            unique_matches = {
                hit["matched_text"].lower()
                for hit in regex_hits_payload
            }

            if score < min_regex_only_score and len(unique_matches) < 2:
                continue

        candidates.append(
            CandidateCode(
                family_id=family_id,
                concept_id=concept_id,
                score=score,
                sources=sources,
                regex_hits=payload["regex_hits"],
                bm25_hits=payload["bm25_hits"],
            )
        )

    candidates.sort(key=lambda c: c.score, reverse=True)
    return candidates[:final_k]


def generate_candidates_for_unit(
    unit: dict[str, Any],
    regex_patterns: list[dict[str, Any]],
    bm25_index: BM25CodebookIndex,
    bm25_top_k: int = 12,
    final_k: int = 8,
    min_bm25_only_score: float = 3.0,
    min_regex_only_score: float = 4.0,
) -> dict[str, Any]:
    text = unit["text"]

    regex_hits = find_regex_hits(text, regex_patterns)
    bm25_hits = bm25_index.search(text, top_k=bm25_top_k)

    candidates = combine_candidates(
        regex_hits=regex_hits,
        bm25_hits=bm25_hits,
        final_k=final_k,
        min_bm25_only_score=min_bm25_only_score,
        min_regex_only_score=min_regex_only_score,
    )

    candidate_families = sorted({candidate.family_id for candidate in candidates})

    return {
        "unit_id": unit["unit_id"],
        "issue_id": unit["issue_id"],
        "article_id": unit.get("article_id"),
        "article_name": unit.get("article_name"),
        "page_number": unit.get("page_number"),
        "logical_page": unit.get("logical_page"),
        "text": text,
        "candidate_families": candidate_families,
        "candidates": [
            {
                "family_id": candidate.family_id,
                "concept_id": candidate.concept_id,
                "score": candidate.score,
                "sources": candidate.sources,
                "regex_hits": candidate.regex_hits,
                "bm25_hits": candidate.bm25_hits,
            }
            for candidate in candidates
        ],
    }


def generate_candidate_file(
    *,
    codebook_path: str,
    retrieval_docs_path: str,
    annotation_units_path: str,
    output_path: str,
    language: str | None = "es",
    bm25_top_k: int = 12,
    final_k: int = 8,
    min_bm25_only_score: float = 3.0,
    min_regex_only_score: float = 4.0,
) -> None:
    language = normalize_language_filter(language)

    codebook = Codebook.model_validate(read_json(codebook_path))
    units = read_jsonl(annotation_units_path)
    retrieval_docs = read_jsonl(retrieval_docs_path)

    regex_patterns = build_regex_patterns(codebook, language=language)
    bm25_index = BM25CodebookIndex(retrieval_docs, language=language)

    rows = [
        generate_candidates_for_unit(
            unit=unit,
            regex_patterns=regex_patterns,
            bm25_index=bm25_index,
            bm25_top_k=bm25_top_k,
            final_k=final_k,
            min_bm25_only_score=min_bm25_only_score,
            min_regex_only_score=min_regex_only_score,
        )
        for unit in units
    ]

    write_jsonl(output_path, rows)