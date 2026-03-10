#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
import json
import re
import shlex
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Iterable

from paddleocr import PaddleOCRVL


SUPPORTED_EXTENSIONS = {
    ".bmp",
    ".jpeg",
    ".jpg",
    ".pdf",
    ".png",
    ".tif",
    ".tiff",
    ".webp",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run PaddleOCR-VL on local documents, saving merged JSON, merged Markdown, "
            "and extracted Markdown images."
        )
    )
    parser.add_argument(
        "input_path",
        help="A single document path or a directory containing documents.",
    )
    parser.add_argument(
        "--output-root",
        default=None,
        help=(
            "Optional output root. If omitted, outputs are written next to each input file. "
            "If input_path is a directory and output-root is set, the input tree is preserved."
        ),
    )
    parser.add_argument(
        "--device",
        default="gpu:0",
        help="Inference device, e.g. gpu:0, cpu, gpu:1. Default: gpu:0.",
    )
    parser.add_argument(
        "--precision",
        default="fp16",
        help="Inference precision passed to PaddleOCRVL. Default: fp16.",
    )
    parser.add_argument(
        "--enable-hpi",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable PaddleOCR high-performance inference. Default: enabled.",
    )
    parser.add_argument(
        "--use-tensorrt",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Enable TensorRT subgraph acceleration when supported.",
    )
    parser.add_argument(
        "--use-doc-orientation-classify",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Enable document orientation classification.",
    )
    parser.add_argument(
        "--use-doc-unwarping",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Enable document unwarping.",
    )
    parser.add_argument(
        "--use-layout-detection",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable layout detection. Default: enabled.",
    )
    parser.add_argument(
        "--merge-tables",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Merge tables across pages when restructuring PDF output.",
    )
    parser.add_argument(
        "--relevel-titles",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Reconstruct multi-level headings when restructuring PDF output.",
    )
    parser.add_argument(
        "--vl-rec-backend",
        default=None,
        help="Optional VL recognition backend override, e.g. vllm.",
    )
    parser.add_argument(
        "--vl-rec-server-url",
        default=None,
        help="Optional server URL if using a service-backed VL recognition backend.",
    )
    parser.add_argument(
        "--vl-rec-max-concurrency",
        type=int,
        default=None,
        help="Optional max concurrency for service-backed VL inference.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of input documents to process.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Process only one batch containing this many documents.",
    )
    parser.add_argument(
        "--batch-index",
        type=int,
        default=0,
        help="Zero-based batch index to process when --batch-size is set. Default: 0.",
    )
    parser.add_argument(
        "--skip-existing",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip documents whose JSON and Markdown outputs already exist. Default: enabled.",
    )
    parser.add_argument(
        "--post-success-command",
        default=None,
        help=(
            "Optional shell command to run after each successful document. "
            "Placeholders: {input}, {output_dir}, {json}, {markdown}, {images_dir}."
        ),
    )
    parser.add_argument(
        "--cleanup-input-on-success",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Delete the source input file after successful OCR and post-success handling.",
    )
    parser.add_argument(
        "--cleanup-output-on-success",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Delete generated OCR outputs after a successful post-success command.",
    )
    return parser.parse_args()


def collect_documents(input_path: Path) -> list[Path]:
    if input_path.is_file():
        return [input_path]
    if not input_path.is_dir():
        raise FileNotFoundError(f"Input path does not exist: {input_path}")
    return sorted(
        path
        for path in input_path.rglob("*")
        if path.is_file() and path.suffix.lower() in SUPPORTED_EXTENSIONS
    )


def sanitize_path_component(value: str) -> str:
    cleaned = re.sub(r"[\\/:*?\"<>|]+", "_", value).strip()
    return cleaned or "item"


def sanitize_relative_image_path(raw_path: str, page_index: int) -> Path:
    original = Path(raw_path)
    safe_parts = [sanitize_path_component(part) for part in original.parts if part not in {"", "."}]
    if not safe_parts:
        safe_parts = [f"image-{page_index:04d}.png"]
    result = Path(f"page-{page_index:04d}")
    for part in safe_parts:
        result /= part
    return result


def rewrite_markdown_images(markdown_info: dict, page_index: int) -> tuple[dict, list[tuple[Path, object]]]:
    normalized = copy.deepcopy(markdown_info)
    markdown_text = normalized.get("markdown_texts", "") or ""
    image_items: list[tuple[Path, object]] = []

    for original_path, image in (markdown_info.get("markdown_images", {}) or {}).items():
        relative_target = Path("images") / sanitize_relative_image_path(original_path, page_index)
        markdown_text = markdown_text.replace(str(original_path), relative_target.as_posix())
        image_items.append((relative_target, image))

    normalized["markdown_texts"] = markdown_text
    normalized["markdown_images"] = {}
    return normalized, image_items


def save_markdown_images(output_dir: Path, image_items: Iterable[tuple[Path, object]]) -> None:
    for relative_path, image in image_items:
        destination = output_dir / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        image.save(destination)


def build_output_dir(input_file: Path, input_root: Path, output_root: Path | None) -> Path:
    if output_root is None:
        return input_file.parent
    if input_root.is_file():
        return output_root
    relative_parent = input_file.relative_to(input_root).parent
    return output_root / relative_parent


def build_output_paths(input_file: Path, input_root: Path, output_root: Path | None) -> tuple[Path, Path, Path, Path]:
    output_dir = build_output_dir(input_file, input_root, output_root)
    stem = sanitize_path_component(input_file.stem)
    json_path = output_dir / f"{stem}.json"
    markdown_path = output_dir / f"{stem}.md"
    images_dir = output_dir / "images"
    return output_dir, json_path, markdown_path, images_dir


def remove_empty_parents(start_dir: Path, stop_dir: Path) -> None:
    current = start_dir
    while current != stop_dir and current.exists():
        if any(current.iterdir()):
            break
        current.rmdir()
        current = current.parent


def run_post_success_command(
    command_template: str,
    *,
    input_file: Path,
    output_dir: Path,
    json_path: Path,
    markdown_path: Path,
    images_dir: Path,
) -> None:
    command = command_template.format(
        input=shlex.quote(str(input_file)),
        output_dir=shlex.quote(str(output_dir)),
        json=shlex.quote(str(json_path)),
        markdown=shlex.quote(str(markdown_path)),
        images_dir=shlex.quote(str(images_dir)),
    )
    subprocess.run(command, shell=True, check=True)


def build_pipeline(args: argparse.Namespace) -> PaddleOCRVL:
    kwargs = {
        "device": args.device,
        "enable_hpi": args.enable_hpi,
        "precision": args.precision,
        "use_tensorrt": args.use_tensorrt,
        "use_doc_orientation_classify": args.use_doc_orientation_classify,
        "use_doc_unwarping": args.use_doc_unwarping,
        "use_layout_detection": args.use_layout_detection,
    }
    if args.vl_rec_backend:
        kwargs["vl_rec_backend"] = args.vl_rec_backend
    if args.vl_rec_server_url:
        kwargs["vl_rec_server_url"] = args.vl_rec_server_url
    if args.vl_rec_max_concurrency is not None:
        kwargs["vl_rec_max_concurrency"] = args.vl_rec_max_concurrency
    return PaddleOCRVL(**kwargs)


def process_document(
    pipeline: PaddleOCRVL,
    input_file: Path,
    input_root: Path,
    output_root: Path | None,
    args: argparse.Namespace,
) -> bool:
    output_dir, json_path, markdown_path, images_dir = build_output_paths(
        input_file,
        input_root,
        output_root,
    )
    if args.skip_existing and json_path.exists() and markdown_path.exists():
        print(f"[skipped-existing] {input_file}", flush=True)
        return False

    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"[start] {input_file}", flush=True)
    raw_pages = list(pipeline.predict(input=str(input_file)))
    if not raw_pages:
        print(f"[empty] {input_file}", flush=True)
        return False

    pages_for_export = pipeline.restructure_pages(
        raw_pages,
        merge_tables=args.merge_tables,
        relevel_titles=args.relevel_titles,
        concatenate_pages=False,
    )

    merged_json = {
        "source_path": str(input_file),
        "page_count": len(raw_pages),
        "pages": [],
    }
    markdown_pages = []
    markdown_images: list[tuple[Path, object]] = []

    for page_number, result in enumerate(pages_for_export, start=1):
        merged_json["pages"].append(
            {
                "page_number": page_number,
                "result": result.json,
            }
        )
        normalized_markdown, page_images = rewrite_markdown_images(result.markdown, page_number)
        markdown_pages.append(normalized_markdown)
        markdown_images.extend(page_images)

    merged_markdown = pipeline.concatenate_markdown_pages(markdown_pages)

    with json_path.open("w", encoding="utf-8") as handle:
        json.dump(merged_json, handle, ensure_ascii=False, indent=2)

    with markdown_path.open("w", encoding="utf-8") as handle:
        handle.write(merged_markdown)

    save_markdown_images(output_dir, markdown_images)
    print(
        f"[done] {input_file} -> {json_path.name}, {markdown_path.name}, images={len(markdown_images)}",
        flush=True,
    )
    if args.post_success_command:
        run_post_success_command(
            args.post_success_command,
            input_file=input_file,
            output_dir=output_dir,
            json_path=json_path,
            markdown_path=markdown_path,
            images_dir=images_dir,
        )
        print(f"[post-success] {input_file}", flush=True)

    if args.cleanup_output_on_success:
        if json_path.exists():
            json_path.unlink()
        if markdown_path.exists():
            markdown_path.unlink()
        if images_dir.exists():
            shutil.rmtree(images_dir)
        stop_dir = output_root.resolve() if output_root else input_file.parent.resolve()
        remove_empty_parents(output_dir, stop_dir)
        print(f"[cleanup-output] {input_file}", flush=True)

    if args.cleanup_input_on_success and input_file.exists():
        input_file.unlink()
        print(f"[cleanup-input] {input_file}", flush=True)

    return True


def main() -> int:
    args = parse_args()
    input_path = Path(args.input_path).expanduser().resolve()
    output_root = (
        Path(args.output_root).expanduser().resolve()
        if args.output_root is not None
        else None
    )

    try:
        documents = collect_documents(input_path)
    except FileNotFoundError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if not documents:
        print("error: no supported documents found", file=sys.stderr)
        return 1

    if args.limit is not None and args.limit > 0:
        documents = documents[: args.limit]
    if args.batch_size is not None:
        if args.batch_size <= 0:
            print("error: --batch-size must be > 0", file=sys.stderr)
            return 1
        if args.batch_index < 0:
            print("error: --batch-index must be >= 0", file=sys.stderr)
            return 1
        start = args.batch_index * args.batch_size
        end = start + args.batch_size
        documents = documents[start:end]

    if not documents:
        print("error: no documents selected after filtering", file=sys.stderr)
        return 1

    print(
        f"Initializing PaddleOCRVL for {len(documents)} document(s) on {args.device} "
        f"with precision={args.precision}.",
        flush=True,
    )
    if args.batch_size is not None:
        print(
            f"Batch mode: batch_index={args.batch_index}, batch_size={args.batch_size}.",
            flush=True,
        )
    pipeline = build_pipeline(args)

    processed = 0
    for index, document in enumerate(documents, start=1):
        print(f"[document {index}/{len(documents)}]", flush=True)
        did_process = process_document(
            pipeline=pipeline,
            input_file=document,
            input_root=input_path,
            output_root=output_root,
            args=args,
        )
        if did_process:
            processed += 1

    print(f"Completed OCR run. processed={processed}, selected={len(documents)}.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
