#!/usr/bin/env python3
from __future__ import annotations

import argparse
import html
import os
import re
import sys
import time
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, unquote, urljoin, urlparse
from urllib.request import Request, urlopen


DEFAULT_START_URL = (
    "https://www.dipujaen.es/revistacandil/results.vm"
    "?q=parent:0000393913&t=%2Balpha&lang=es&view=cdl"
)
COLLECTION_DIRNAME = "candil"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (compatible; candil-downloader/1.0; "
    "+https://www.dipujaen.es/revistacandil/)"
)


@dataclass(frozen=True)
class DownloadLink:
    url: str
    filename: str
    label: str
    edition_slug: str
    previous_edition_slug: str
    legacy_filename: str


@dataclass(frozen=True)
class PageData:
    current_page: int | None
    total_pages: int | None
    total_results: int | None
    downloads: list[DownloadLink]
    next_page_url: str | None


class ResultsParser(HTMLParser):
    def __init__(self, page_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.page_url = page_url
        self.downloads: list[DownloadLink] = []
        self.next_page_url: str | None = None
        self.current_page: int | None = None
        self.total_pages: int | None = None
        self.total_results: int | None = None

        self._capture_results_text = False
        self._results_chunks: list[str] = []
        self._capture_label = False
        self._label_chunks: list[str] = []
        self._pending_download: dict[str, str] | None = None
        self._capture_record_name = False
        self._record_name_chunks: list[str] = []
        self._current_frame_id: str | None = None
        self._current_record_name: str | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = {key: value or "" for key, value in attrs}
        if tag == "div" and attr_map.get("class") == "results":
            self._capture_results_text = True
            self._results_chunks.clear()
            return

        if tag == "div" and attr_map.get("class") == "list-frame":
            frame_id = attr_map.get("id", "")
            self._current_frame_id = frame_id.removeprefix("frame-") or None
            self._current_record_name = None
            return

        if tag == "p" and attr_map.get("class") == "list-record-name":
            self._capture_record_name = True
            self._record_name_chunks.clear()
            return

        if tag != "a":
            return

        href = attr_map.get("href")
        anchor_id = attr_map.get("id", "")

        if anchor_id in {"top-next", "bottom-next"} and href and not self.next_page_url:
            self.next_page_url = urljoin(self.page_url, html.unescape(href))

        if not anchor_id.startswith("download-") or not href:
            return

        record_id = anchor_id.removeprefix("download-")
        download_url = urljoin(self.page_url, html.unescape(href))
        edition_slug = derive_edition_slug(self._current_record_name, record_id)
        previous_edition_slug = derive_edition_slug(
            self._current_record_name,
            record_id,
            year_first=False,
        )
        filename = f"{edition_slug}.pdf"
        legacy_filename = derive_filename(download_url, record_id)
        self._pending_download = {
            "url": download_url,
            "filename": filename,
            "edition_slug": edition_slug,
            "previous_edition_slug": previous_edition_slug,
            "legacy_filename": legacy_filename,
        }
        self._capture_label = True
        self._label_chunks.clear()

    def handle_data(self, data: str) -> None:
        if self._capture_results_text:
            self._results_chunks.append(data)
        if self._capture_label:
            self._label_chunks.append(data)
        if self._capture_record_name:
            self._record_name_chunks.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "div" and self._capture_results_text:
            self._capture_results_text = False
            self._parse_results_summary(" ".join(self._results_chunks))
            self._results_chunks.clear()
            return

        if tag == "p" and self._capture_record_name:
            self._capture_record_name = False
            self._current_record_name = " ".join(chunk.strip() for chunk in self._record_name_chunks).strip()
            self._record_name_chunks.clear()
            return

        if tag == "a" and self._capture_label and self._pending_download:
            label = " ".join(chunk.strip() for chunk in self._label_chunks).strip() or "Descargar"
            self.downloads.append(
                DownloadLink(
                    url=self._pending_download["url"],
                    filename=self._pending_download["filename"],
                    label=label,
                    edition_slug=self._pending_download["edition_slug"],
                    previous_edition_slug=self._pending_download["previous_edition_slug"],
                    legacy_filename=self._pending_download["legacy_filename"],
                )
            )
            self._capture_label = False
            self._label_chunks.clear()
            self._pending_download = None

    def _parse_results_summary(self, text: str) -> None:
        normalized = " ".join(text.split())
        page_match = re.search(r"Página\s+(\d+)\s+de\s+(\d+)", normalized)
        if page_match:
            self.current_page = int(page_match.group(1))
            self.total_pages = int(page_match.group(2))

        results_match = re.search(r"Resultados:\s+(\d+)", normalized)
        if results_match:
            self.total_results = int(results_match.group(1))


def derive_filename(download_url: str, fallback_id: str) -> str:
    parsed = urlparse(download_url)
    params = parse_qs(parsed.query)
    attachment = params.get("attachment", [""])[0]
    attachment = unquote(attachment).strip()
    if attachment:
        return safe_filename(attachment)
    return f"{fallback_id}.pdf"


def derive_edition_slug(record_name: str | None, fallback_id: str, year_first: bool = True) -> str:
    if record_name:
        match = re.search(r"(\d{1,2})/(\d{4})", record_name)
        if match:
            month = int(match.group(1))
            year = match.group(2)
            if year_first:
                return f"{year}-{month:02d}"
            return f"{month:02d}-{year}"
    return fallback_id


def safe_filename(name: str) -> str:
    sanitized = re.sub(r'[\\/:*?"<>|]+', "_", name).strip()
    return sanitized or "download.pdf"


def fetch_text(url: str, timeout: float, user_agent: str) -> str:
    request = Request(url, headers={"User-Agent": user_agent})
    with urlopen(request, timeout=timeout) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        return response.read().decode(charset, errors="replace")


def fetch_page(url: str, timeout: float, user_agent: str) -> PageData:
    parser = ResultsParser(url)
    parser.feed(fetch_text(url, timeout=timeout, user_agent=user_agent))
    return PageData(
        current_page=parser.current_page,
        total_pages=parser.total_pages,
        total_results=parser.total_results,
        downloads=parser.downloads,
        next_page_url=parser.next_page_url,
    )


def with_retries(action_name: str, retries: int, func):
    attempt = 0
    while True:
        try:
            return func()
        except (HTTPError, URLError, TimeoutError) as exc:
            attempt += 1
            if attempt > retries:
                raise
            print(
                f"  [retry {attempt}/{retries}] {action_name} failed: {exc}",
                flush=True,
            )
            time.sleep(min(5 * attempt, 15))


def download_file(
    link: DownloadLink,
    collection_dir: Path,
    legacy_root_dir: Path,
    timeout: float,
    user_agent: str,
    overwrite: bool,
    resume: bool,
) -> tuple[str, Path]:
    destination_dir = collection_dir / link.edition_slug
    destination = destination_dir / link.filename
    legacy_source = legacy_root_dir / link.legacy_filename
    previous_destination = collection_dir / link.previous_edition_slug / f"{link.previous_edition_slug}.pdf"

    destination_dir.mkdir(parents=True, exist_ok=True)
    if destination.exists() and resume and not overwrite:
        return "skipped", destination
    if previous_destination.exists() and previous_destination != destination and not overwrite:
        os.replace(previous_destination, destination)
        previous_parent = previous_destination.parent
        if previous_parent.exists() and not any(previous_parent.iterdir()):
            previous_parent.rmdir()
        return "moved", destination
    if legacy_source.exists() and not overwrite:
        os.replace(legacy_source, destination)
        return "moved", destination

    request = Request(link.url, headers={"User-Agent": user_agent})
    tmp_destination = destination.with_suffix(destination.suffix + ".part")
    try:
        with urlopen(request, timeout=timeout) as response, tmp_destination.open("wb") as handle:
            while True:
                chunk = response.read(1024 * 128)
                if not chunk:
                    break
                handle.write(chunk)
        os.replace(tmp_destination, destination)
    finally:
        if tmp_destination.exists():
            tmp_destination.unlink(missing_ok=True)

    return "downloaded", destination


def count_existing_pdfs(output_dir: Path) -> int:
    return sum(1 for path in output_dir.rglob("*.pdf") if path.is_file())


def iterate_pages(
    start_url: str,
    timeout: float,
    user_agent: str,
    delay: float,
    retries: int,
) -> Iterable[PageData]:
    next_url: str | None = start_url
    seen_urls: set[str] = set()

    while next_url:
        if next_url in seen_urls:
            raise RuntimeError(f"Pagination loop detected at {next_url}")
        seen_urls.add(next_url)

        page = with_retries(
            action_name=f"fetching page {next_url}",
            retries=retries,
            func=lambda: fetch_page(next_url, timeout=timeout, user_agent=user_agent),
        )
        yield page

        next_url = page.next_page_url
        if next_url and delay > 0:
            time.sleep(delay)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download all Candil PDFs from the paginated results listing on dipujaen.es."
        )
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory where downloaded files should be saved.",
    )
    parser.add_argument(
        "--start-url",
        default=DEFAULT_START_URL,
        help="Results page URL to start from.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="Network timeout in seconds. Default: 30.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.2,
        help="Delay between page requests in seconds. Default: 0.2.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Redownload files even if they already exist.",
    )
    parser.add_argument(
        "--resume",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip files that already exist in the output directory. Default: enabled.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List the files that would be downloaded without saving them.",
    )
    parser.add_argument(
        "--user-agent",
        default=DEFAULT_USER_AGENT,
        help="HTTP User-Agent header to send.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of unique files to process.",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Number of retries for page loads and file downloads. Default: 3.",
    )
    parser.add_argument(
        "--organize-only",
        action="store_true",
        help="Reorganize existing legacy downloads into the new Candil folder structure without downloading missing files.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    collection_dir = output_dir / COLLECTION_DIRNAME
    collection_dir.mkdir(parents=True, exist_ok=True)
    existing_pdfs = count_existing_pdfs(collection_dir)

    page_count = 0
    found_links = 0
    downloaded = 0
    skipped = 0
    seen_download_urls: set[str] = set()
    limit = args.limit if args.limit and args.limit > 0 else None

    print(
        f"Starting download into {output_dir} "
        f"with resume={'on' if args.resume else 'off'}, "
        f"overwrite={'on' if args.overwrite else 'off'}, "
        f"organize_only={'on' if args.organize_only else 'off'}.",
        flush=True,
    )
    if args.resume and existing_pdfs:
        print(
            f"Found {existing_pdfs} existing PDF(s); they will be skipped.",
            flush=True,
        )

    try:
        for page in iterate_pages(
            start_url=args.start_url,
            timeout=args.timeout,
            user_agent=args.user_agent,
            delay=args.delay,
            retries=args.retries,
        ):
            page_count += 1
            page_label = page.current_page if page.current_page is not None else page_count
            total_label = page.total_pages if page.total_pages is not None else "?"
            page_total_results = page.total_results if page.total_results is not None else "?"
            print(
                f"[page {page_label}/{total_label}] found {len(page.downloads)} download link(s); "
                f"site total is {page_total_results}.",
                flush=True,
            )

            for link in page.downloads:
                if link.url in seen_download_urls:
                    continue
                seen_download_urls.add(link.url)
                found_links += 1
                progress_total = limit if limit is not None else (page.total_results or "?")
                progress_prefix = f"[file {found_links}/{progress_total}]"

                if limit is not None and found_links > limit:
                    print(
                        f"Reached limit of {limit} unique file(s); stopping.",
                        flush=True,
                    )
                    print(
                        f"Completed: {page_count} page(s), {limit} unique file(s), "
                        f"{downloaded} downloaded, {skipped} skipped.",
                        flush=True,
                    )
                    return 0

                if args.dry_run:
                    print(
                        f"{progress_prefix} [dry-run] {COLLECTION_DIRNAME}/{link.edition_slug}/{link.filename}",
                        flush=True,
                    )
                    continue

                if args.organize_only:
                    legacy_source = output_dir / link.legacy_filename
                    destination = collection_dir / link.edition_slug / link.filename
                    previous_destination = (
                        collection_dir / link.previous_edition_slug / f"{link.previous_edition_slug}.pdf"
                    )
                    if destination.exists():
                        skipped += 1
                        print(f"{progress_prefix} [skipped] {destination}", flush=True)
                    elif previous_destination.exists() and previous_destination != destination:
                        destination.parent.mkdir(parents=True, exist_ok=True)
                        os.replace(previous_destination, destination)
                        previous_parent = previous_destination.parent
                        if previous_parent.exists() and not any(previous_parent.iterdir()):
                            previous_parent.rmdir()
                        downloaded += 1
                        print(f"{progress_prefix} [moved] {destination}", flush=True)
                    elif legacy_source.exists():
                        destination.parent.mkdir(parents=True, exist_ok=True)
                        os.replace(legacy_source, destination)
                        downloaded += 1
                        print(f"{progress_prefix} [moved] {destination}", flush=True)
                    else:
                        print(f"{progress_prefix} [missing] {link.legacy_filename}", flush=True)
                    if args.delay > 0:
                        time.sleep(args.delay)
                    continue

                status, path = with_retries(
                    action_name=f"downloading {link.filename}",
                    retries=args.retries,
                    func=lambda: download_file(
                        link=link,
                        collection_dir=collection_dir,
                        legacy_root_dir=output_dir,
                        timeout=args.timeout,
                        user_agent=args.user_agent,
                        overwrite=args.overwrite,
                        resume=args.resume,
                    ),
                )
                if status in {"downloaded", "moved"}:
                    downloaded += 1
                    print(f"{progress_prefix} [{status}] {path}", flush=True)
                else:
                    skipped += 1
                    print(f"{progress_prefix} [skipped] {path}", flush=True)

                if args.delay > 0:
                    time.sleep(args.delay)
    except (HTTPError, URLError, TimeoutError, RuntimeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(
        f"Completed: {page_count} page(s), {found_links} unique file(s), "
        f"{downloaded} downloaded, {skipped} skipped.",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
