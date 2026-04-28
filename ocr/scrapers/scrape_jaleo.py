#!/usr/bin/env python3
from __future__ import annotations

import argparse
import html
import os
import re
import ssl
import sys
import time
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urljoin, urlparse
from urllib.request import Request, urlopen


DEFAULT_START_URL = "https://www.elitedynamics.com/jaleomagazine/index-jaleo_issues.htm"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (compatible; jaleo-downloader/1.0; "
    "+https://www.elitedynamics.com/jaleomagazine/)"
)


@dataclass(frozen=True)
class IssueDownload:
    issue_label: str
    issue_slug: str
    low_url: str | None
    high_url: str | None

    @property
    def preferred_url(self) -> str:
        return self.high_url or self.low_url or ""

    @property
    def quality(self) -> str:
        return "better" if self.high_url else "low"


class IssueTableParser(HTMLParser):
    def __init__(self, page_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.page_url = page_url
        self.issues: list[IssueDownload] = []

        self._in_row = False
        self._cell_index = -1
        self._current_label_parts: list[str] = []
        self._current_low_url: str | None = None
        self._current_high_url: str | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = {key: value or "" for key, value in attrs}
        if tag == "tr":
            self._in_row = True
            self._cell_index = -1
            self._current_label_parts = []
            self._current_low_url = None
            self._current_high_url = None
            return

        if not self._in_row:
            return

        if tag == "td":
            self._cell_index += 1
            return

        if tag != "a":
            return

        href = html.unescape(attr_map.get("href", "")).strip()
        if not href.lower().endswith(".pdf"):
            return

        full_url = urljoin(self.page_url, href)
        if self._cell_index == 1:
            self._current_low_url = full_url
        elif self._cell_index == 2:
            self._current_high_url = full_url

    def handle_data(self, data: str) -> None:
        if self._in_row and self._cell_index == 0:
            text = " ".join(data.split())
            if text:
                self._current_label_parts.append(text)

    def handle_endtag(self, tag: str) -> None:
        if tag != "tr" or not self._in_row:
            return

        self._in_row = False
        label = " ".join(self._current_label_parts).strip()
        if label == "Issue Date":
            return
        if not label:
            return
        if not self._current_low_url and not self._current_high_url:
            return

        preferred = self._current_high_url or self._current_low_url or ""
        issue_slug = derive_issue_slug(preferred, label)
        self.issues.append(
            IssueDownload(
                issue_label=label,
                issue_slug=issue_slug,
                low_url=self._current_low_url,
                high_url=self._current_high_url,
            )
        )


def safe_filename(name: str) -> str:
    sanitized = re.sub(r'[\\/:*?"<>|]+', "_", name).strip()
    return sanitized or "download.pdf"


def derive_issue_slug(download_url: str, label: str) -> str:
    path_name = Path(unquote(urlparse(download_url).path)).name
    match = re.search(r"JALEO-(\d{4})-(\d{2})", path_name, re.IGNORECASE)
    if match:
        return f"{match.group(1)}-{match.group(2)}"

    month_map = {
        "january": "01",
        "february": "02",
        "march": "03",
        "april": "04",
        "may": "05",
        "june": "06",
        "july": "07",
        "august": "08",
        "september": "09",
        "october": "10",
        "november": "11",
        "december": "12",
    }
    label_match = re.search(
        r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})",
        label,
        re.IGNORECASE,
    )
    if label_match:
        month = month_map[label_match.group(1).lower()]
        year = label_match.group(2)
        return f"{year}-{month}"

    fallback = re.sub(r"[^a-z0-9]+", "-", label.lower()).strip("-")
    return fallback or "jaleo-issue"


def fetch_text(url: str, timeout: float, user_agent: str) -> str:
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    request = Request(url, headers={"User-Agent": user_agent})
    with urlopen(request, timeout=timeout, context=context) as response:
        raw = response.read()
        charset = response.headers.get_content_charset() or "cp1252"
        return raw.decode(charset, errors="replace")


def with_retries(action_name: str, retries: int, func):
    attempt = 0
    while True:
        try:
            return func()
        except (HTTPError, URLError, TimeoutError, ssl.SSLError) as exc:
            attempt += 1
            if attempt > retries:
                raise
            print(
                f"  [retry {attempt}/{retries}] {action_name} failed: {exc}",
                flush=True,
            )
            time.sleep(min(5 * attempt, 15))


def fetch_issues(start_url: str, timeout: float, user_agent: str, retries: int) -> list[IssueDownload]:
    parser = IssueTableParser(start_url)
    parser.feed(
        with_retries(
            action_name=f"fetching issue index {start_url}",
            retries=retries,
            func=lambda: fetch_text(start_url, timeout=timeout, user_agent=user_agent),
        )
    )
    return parser.issues


def count_existing_pdfs(output_dir: Path) -> int:
    return sum(1 for path in output_dir.rglob("*.pdf"))


def download_file(
    issue: IssueDownload,
    output_dir: Path,
    timeout: float,
    user_agent: str,
    overwrite: bool,
    resume: bool,
) -> tuple[str, Path]:
    issue_dir = output_dir / issue.issue_slug
    issue_dir.mkdir(parents=True, exist_ok=True)
    destination = issue_dir / f"{issue.issue_slug}.pdf"

    if destination.exists() and resume and not overwrite:
        return "skipped", destination

    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    request = Request(issue.preferred_url, headers={"User-Agent": user_agent})
    tmp_destination = destination.with_suffix(destination.suffix + ".part")
    try:
        with urlopen(request, timeout=timeout, context=context) as response, tmp_destination.open("wb") as handle:
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download Jaleo magazine issues, preferring better-resolution PDFs when available."
    )
    parser.add_argument("--output-dir", required=True, help="Directory where issue folders should be saved.")
    parser.add_argument(
        "--start-url",
        default=DEFAULT_START_URL,
        help="Jaleo issue index page URL.",
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
        help="Delay between downloads in seconds. Default: 0.2.",
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
        help="List discovered issues without downloading files.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only process the first N issues after parsing.",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=5,
        help="Number of retries for failed requests. Default: 5.",
    )
    parser.add_argument(
        "--user-agent",
        default=DEFAULT_USER_AGENT,
        help="HTTP User-Agent to send with requests.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Output directory: {output_dir}", flush=True)
    print(f"Start URL: {args.start_url}", flush=True)
    print(f"Existing PDFs: {count_existing_pdfs(output_dir)}", flush=True)

    issues = fetch_issues(
        start_url=args.start_url,
        timeout=args.timeout,
        user_agent=args.user_agent,
        retries=args.retries,
    )
    if args.limit is not None:
        issues = issues[: args.limit]

    print(f"Discovered issues: {len(issues)}", flush=True)
    if args.dry_run:
        for index, issue in enumerate(issues, start=1):
            print(
                f"[{index}/{len(issues)}] {issue.issue_slug} [{issue.quality}] {issue.preferred_url}",
                flush=True,
            )
        return 0

    downloaded = 0
    skipped = 0
    for index, issue in enumerate(issues, start=1):
        print(
            f"[issue {index}/{len(issues)}] {issue.issue_slug} [{issue.quality}]",
            flush=True,
        )
        status, destination = with_retries(
            action_name=f"downloading {issue.preferred_url}",
            retries=args.retries,
            func=lambda issue=issue: download_file(
                issue=issue,
                output_dir=output_dir,
                timeout=args.timeout,
                user_agent=args.user_agent,
                overwrite=args.overwrite,
                resume=args.resume,
            ),
        )
        print(f"  {status}: {destination}", flush=True)
        if status == "downloaded":
            downloaded += 1
        else:
            skipped += 1
        if args.delay > 0:
            time.sleep(args.delay)

    print(
        f"Done. Downloaded {downloaded}, skipped {skipped}, total {len(issues)}.",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
