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
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, unquote, urljoin, urlparse
from urllib.request import Request, urlopen


DEFAULT_START_URL = "https://www.juntadeandalucia.es/cultura/flamenco/content/la-nueva-albore%C3%A1"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (compatible; alborea-downloader/1.0; "
    "+https://www.juntadeandalucia.es/cultura/flamenco/)"
)
ISSUE_PATH_RE = re.compile(r"/content/la-nueva-albore%C3%A1-n%C2%BA-\d+$", re.IGNORECASE)


@dataclass(frozen=True)
class IssueLink:
    url: str
    label: str


@dataclass(frozen=True)
class DownloadLink:
    url: str
    filename: str
    label: str


class IssueIndexParser(HTMLParser):
    def __init__(self, page_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.page_url = page_url
        self.issues: list[IssueLink] = []
        self._capture_label = False
        self._label_chunks: list[str] = []
        self._pending_url: str | None = None
        self._seen_urls: set[str] = set()

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a":
            return
        attr_map = {key: value or "" for key, value in attrs}
        href = attr_map.get("href", "")
        full_url = urljoin(self.page_url, html.unescape(href))
        parsed = urlparse(full_url)
        if not ISSUE_PATH_RE.search(parsed.path):
            return
        if full_url in self._seen_urls:
            return
        self._pending_url = full_url
        self._capture_label = True
        self._label_chunks.clear()

    def handle_data(self, data: str) -> None:
        if self._capture_label:
            self._label_chunks.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag != "a" or not self._capture_label or not self._pending_url:
            return
        label = " ".join(chunk.strip() for chunk in self._label_chunks).strip()
        if label:
            self.issues.append(IssueLink(url=self._pending_url, label=label))
            self._seen_urls.add(self._pending_url)
        self._pending_url = None
        self._capture_label = False
        self._label_chunks.clear()


class IssuePageParser(HTMLParser):
    def __init__(self, page_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.page_url = page_url
        self.pdf_url: str | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "iframe" or self.pdf_url:
            return
        attr_map = {key: value or "" for key, value in attrs}
        classes = set(attr_map.get("class", "").split())
        if "pdf" not in classes:
            return

        data_src = html.unescape(attr_map.get("data-src", "")).strip()
        if data_src:
            self.pdf_url = urljoin(self.page_url, data_src)
            return

        viewer_src = html.unescape(attr_map.get("src", "")).strip()
        if not viewer_src:
            return

        viewer_url = urljoin(self.page_url, viewer_src)
        file_param = parse_qs(urlparse(viewer_url).query).get("file", [""])[0]
        if file_param:
            self.pdf_url = urljoin(self.page_url, unquote(file_param))


def safe_filename(name: str) -> str:
    sanitized = re.sub(r'[\\/:*?"<>|]+', "_", name).strip()
    return sanitized or "download.pdf"


def derive_filename(pdf_url: str, fallback_label: str) -> str:
    path_name = Path(unquote(urlparse(pdf_url).path)).name
    if path_name:
        return safe_filename(path_name)
    normalized = re.sub(r"\s+", "_", fallback_label.strip().lower())
    normalized = normalized.replace("º", "o")
    return safe_filename(f"{normalized}.pdf")


def fetch_text(url: str, timeout: float, user_agent: str) -> str:
    request = Request(url, headers={"User-Agent": user_agent})
    with urlopen(request, timeout=timeout) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        return response.read().decode(charset, errors="replace")


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


def fetch_issue_index(url: str, timeout: float, user_agent: str, retries: int) -> list[IssueLink]:
    parser = IssueIndexParser(url)
    parser.feed(
        with_retries(
            action_name=f"fetching issue index {url}",
            retries=retries,
            func=lambda: fetch_text(url, timeout=timeout, user_agent=user_agent),
        )
    )
    return parser.issues


def fetch_pdf_link(issue: IssueLink, timeout: float, user_agent: str, retries: int) -> DownloadLink:
    parser = IssuePageParser(issue.url)
    parser.feed(
        with_retries(
            action_name=f"fetching issue page {issue.url}",
            retries=retries,
            func=lambda: fetch_text(issue.url, timeout=timeout, user_agent=user_agent),
        )
    )
    if not parser.pdf_url:
        raise RuntimeError(f"No PDF viewer found on issue page: {issue.url}")
    return DownloadLink(
        url=parser.pdf_url,
        filename=derive_filename(parser.pdf_url, issue.label),
        label=issue.label,
    )


def download_file(
    link: DownloadLink,
    output_dir: Path,
    timeout: float,
    user_agent: str,
    overwrite: bool,
    resume: bool,
) -> tuple[str, Path]:
    destination = output_dir / link.filename
    if destination.exists() and resume and not overwrite:
        return "skipped", destination

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
    return sum(1 for path in output_dir.iterdir() if path.is_file() and path.suffix.lower() == ".pdf")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download all La Nueva Alborea PDFs from the Andalusian Flamenco site."
    )
    parser.add_argument("--output-dir", required=True, help="Directory where PDFs should be saved.")
    parser.add_argument(
        "--start-url",
        default=DEFAULT_START_URL,
        help="La Nueva Alborea index page URL.",
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
        help="Delay between requests in seconds. Default: 0.2.",
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
        help="List the PDFs that would be downloaded without saving them.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of issue PDFs to process.",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Number of retries for page loads and file downloads. Default: 3.",
    )
    parser.add_argument(
        "--user-agent",
        default=DEFAULT_USER_AGENT,
        help="HTTP User-Agent header to send.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    existing_pdfs = count_existing_pdfs(output_dir)
    limit = args.limit if args.limit and args.limit > 0 else None

    print(
        f"Starting La Nueva Alborea download into {output_dir} "
        f"with resume={'on' if args.resume else 'off'}, "
        f"overwrite={'on' if args.overwrite else 'off'}.",
        flush=True,
    )
    if args.resume and existing_pdfs:
        print(
            f"Found {existing_pdfs} existing PDF(s); they will be skipped.",
            flush=True,
        )

    try:
        issues = fetch_issue_index(
            url=args.start_url,
            timeout=args.timeout,
            user_agent=args.user_agent,
            retries=args.retries,
        )
        total_issues = len(issues)
        print(f"Found {total_issues} issue link(s) on the index page.", flush=True)

        downloaded = 0
        skipped = 0

        for index, issue in enumerate(issues, start=1):
            if limit is not None and index > limit:
                print(f"Reached limit of {limit} issue(s); stopping.", flush=True)
                break

            progress_total = limit if limit is not None else total_issues
            progress_prefix = f"[issue {index}/{progress_total}]"
            pdf_link = fetch_pdf_link(
                issue=issue,
                timeout=args.timeout,
                user_agent=args.user_agent,
                retries=args.retries,
            )

            if args.dry_run:
                print(
                    f"{progress_prefix} [dry-run] {issue.label} -> {pdf_link.filename}",
                    flush=True,
                )
                if args.delay > 0:
                    time.sleep(args.delay)
                continue

            status, path = with_retries(
                action_name=f"downloading {pdf_link.filename}",
                retries=args.retries,
                func=lambda: download_file(
                    link=pdf_link,
                    output_dir=output_dir,
                    timeout=args.timeout,
                    user_agent=args.user_agent,
                    overwrite=args.overwrite,
                    resume=args.resume,
                ),
            )
            if status == "downloaded":
                downloaded += 1
                print(f"{progress_prefix} [downloaded] {path.name}", flush=True)
            else:
                skipped += 1
                print(f"{progress_prefix} [skipped] {path.name}", flush=True)

            if args.delay > 0:
                time.sleep(args.delay)
    except (HTTPError, URLError, TimeoutError, RuntimeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(
        f"Completed La Nueva Alborea run: {downloaded} downloaded, {skipped} skipped.",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
