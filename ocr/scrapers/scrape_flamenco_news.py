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
from urllib.parse import urljoin
from urllib.request import Request, urlopen


DEFAULT_START_URL = "https://biginabox.com/page-design-and-posters/"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (compatible; flamenco-news-downloader/1.0; "
    "+https://biginabox.com/page-design-and-posters/)"
)

SEASON_ALIASES = {
    "spring": "spring",
    "summer": "summer",
    "autumn": "fall",
    "fall": "fall",
    "winter": "winter",
}


@dataclass(frozen=True)
class EditionLink:
    edition_slug: str
    source_label: str
    url: str


class FlamencoNewsPdfParser(HTMLParser):
    def __init__(self, page_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.page_url = page_url
        self.links: list[EditionLink] = []
        self._capture_anchor = False
        self._anchor_href: str | None = None
        self._anchor_text_parts: list[str] = []
        self._seen_editions: set[str] = set()

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a":
            return
        attr_map = {key: value or "" for key, value in attrs}
        href = html.unescape(attr_map.get("href", "")).strip()
        if not href.lower().endswith(".pdf"):
            return
        self._capture_anchor = True
        self._anchor_href = urljoin(self.page_url, href)
        self._anchor_text_parts.clear()

    def handle_data(self, data: str) -> None:
        if self._capture_anchor:
            self._anchor_text_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag != "a" or not self._capture_anchor or not self._anchor_href:
            return

        label = " ".join(" ".join(self._anchor_text_parts).split()).strip()
        self._capture_anchor = False
        self._anchor_text_parts.clear()
        href = self._anchor_href
        self._anchor_href = None

        edition_slug = derive_edition_slug(label)
        if not edition_slug:
            return
        if edition_slug in self._seen_editions:
            return

        self.links.append(EditionLink(edition_slug=edition_slug, source_label=label, url=href))
        self._seen_editions.add(edition_slug)


def derive_edition_slug(label: str) -> str | None:
    normalized = " ".join(label.split())
    match = re.search(
        r"FLAMENCO\s+NEWS\s+(SPRING|SUMMER|AUTUMN|FALL|WINTER)\s+(\d{4})",
        normalized,
        re.IGNORECASE,
    )
    if not match:
        return None
    season = SEASON_ALIASES[match.group(1).lower()]
    year = match.group(2)
    return f"{year}-{season}"


def fetch_text(url: str, timeout: float, user_agent: str) -> str:
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    request = Request(url, headers={"User-Agent": user_agent})
    with urlopen(request, timeout=timeout, context=context) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        return response.read().decode(charset, errors="replace")


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


def fetch_links(start_url: str, timeout: float, user_agent: str, retries: int) -> list[EditionLink]:
    parser = FlamencoNewsPdfParser(start_url)
    parser.feed(
        with_retries(
            action_name=f"fetching page {start_url}",
            retries=retries,
            func=lambda: fetch_text(start_url, timeout=timeout, user_agent=user_agent),
        )
    )
    return sorted(parser.links, key=lambda link: link.edition_slug)


def download_file(
    link: EditionLink,
    output_dir: Path,
    timeout: float,
    user_agent: str,
    overwrite: bool,
    resume: bool,
) -> tuple[str, Path]:
    destination = output_dir / f"{link.edition_slug}.pdf"
    if destination.exists() and resume and not overwrite:
        return "skipped", destination

    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    request = Request(link.url, headers={"User-Agent": user_agent})
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


def count_existing_pdfs(output_dir: Path) -> int:
    return sum(1 for path in output_dir.iterdir() if path.is_file() and path.suffix.lower() == ".pdf")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download Flamenco News PDFs from biginabox.com as YYYY-season.pdf."
    )
    parser.add_argument("--output-dir", required=True, help="Directory where YYYY-season.pdf files should be saved.")
    parser.add_argument("--start-url", default=DEFAULT_START_URL, help="Flamenco News index page URL.")
    parser.add_argument("--timeout", type=float, default=30.0, help="Network timeout in seconds. Default: 30.")
    parser.add_argument("--delay", type=float, default=0.2, help="Delay between downloads in seconds. Default: 0.2.")
    parser.add_argument("--overwrite", action="store_true", help="Redownload files even if they already exist.")
    parser.add_argument(
        "--resume",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip files that already exist in the output directory. Default: enabled.",
    )
    parser.add_argument("--dry-run", action="store_true", help="List discovered editions without downloading files.")
    parser.add_argument("--limit", type=int, default=None, help="Only process the first N editions.")
    parser.add_argument("--retries", type=int, default=5, help="Number of retries for failed requests. Default: 5.")
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT, help="HTTP User-Agent to send with requests.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Output directory: {output_dir}", flush=True)
    print(f"Start URL: {args.start_url}", flush=True)
    print(f"Existing PDFs: {count_existing_pdfs(output_dir)}", flush=True)

    links = fetch_links(
        start_url=args.start_url,
        timeout=args.timeout,
        user_agent=args.user_agent,
        retries=args.retries,
    )
    if args.limit is not None:
        links = links[: args.limit]

    print(f"Discovered editions: {len(links)}", flush=True)
    if args.dry_run:
        for index, link in enumerate(links, start=1):
            print(f"[{index}/{len(links)}] {link.edition_slug} -> {link.url}", flush=True)
        return 0

    downloaded = 0
    skipped = 0
    failed = 0
    for index, link in enumerate(links, start=1):
        print(f"[edition {index}/{len(links)}] {link.edition_slug}", flush=True)
        try:
            status, destination = with_retries(
                action_name=f"downloading {link.url}",
                retries=args.retries,
                func=lambda link=link: download_file(
                    link=link,
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
        except (HTTPError, URLError, TimeoutError, ssl.SSLError) as exc:
            failed += 1
            print(f"  failed: {link.url} ({exc})", flush=True)
        if args.delay > 0:
            time.sleep(args.delay)

    print(
        f"Done. Downloaded {downloaded}, skipped {skipped}, failed {failed}, total {len(links)}.",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
