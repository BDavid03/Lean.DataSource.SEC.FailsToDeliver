# helpers/extract.py
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin, urlparse

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.http import Response

try:
    from scrapy_playwright.page import PageMethod
except ImportError:
    raise RuntimeError("Install scrapy-playwright: pip install scrapy-playwright")

from helpers.paths import ZIP_DIR, DOWNLOADED_JSON, SEC_INDEX_URL


def _load_ledger(path: Path) -> set[str]:
    if not path.exists():
        return set()
    try:
        return set(json.loads(path.read_text(encoding="utf-8")))
    except Exception as e:
        logging.warning("Ledger read failed (%s). Starting with empty set.", e)
        return set()


def _save_ledger(path: Path, urls: set[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(sorted(urls), indent=2), encoding="utf-8")


def _contact_email() -> str:
    return os.getenv("SEC_CONTACT_EMAIL", "youremail@example.com")


class SECFtdSpider(scrapy.Spider):
    name = "sec_ftd"
    allowed_domains = ["sec.gov"]

    # class attrs (set by extract_new_zips)
    start_url: str = SEC_INDEX_URL
    downloads_dir: Path = ZIP_DIR

    # runtime
    ledger_path: Path
    last_run_path: Path
    ledger: set[str]
    saved_urls: list[str]
    saved_paths: list[str]              # << store filepaths too (as strings)
    limit: int | None

    UA = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36 "
        f"(contact: {_contact_email()})"
    )

    custom_settings = {
        "USER_AGENT": UA,
        "ROBOTSTXT_OBEY": True,
        "COOKIES_ENABLED": False,
        "LOG_LEVEL": "INFO",

        # be polite
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 0.5,
        "AUTOTHROTTLE_MAX_DELAY": 5.0,
        "DOWNLOAD_DELAY": 0.25,
        "CONCURRENT_REQUESTS": 12,

        "DEFAULT_REQUEST_HEADERS": {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        },

        # scrapy-playwright for the index page only
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        "PLAYWRIGHT_BROWSER_TYPE": "chromium",
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 30000,
        "PLAYWRIGHT_LAUNCH_OPTIONS": {"headless": True},
    }

    def __init__(
        self,
        ledger_path: Path,
        last_run_path: Path,
        ledger: Iterable[str] | None = None,
        limit: int | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.ledger_path = ledger_path
        self.last_run_path = last_run_path
        self.ledger = set(ledger or [])
        self.limit = limit
        self.saved_urls = []
        self.saved_paths = []

    async def start(self):
        self.downloads_dir.mkdir(parents=True, exist_ok=True)
        yield scrapy.Request(
            url=self.start_url,
            callback=self.parse_index,
            headers={"Referer": "https://www.sec.gov/"},
            meta={
                "playwright": True,
                "playwright_page_methods": [
                    PageMethod("wait_for_selector", 'a[href*=".zip"]', state="attached"),
                ],
            },
            dont_filter=True,
        )

    @staticmethod
    def _is_zip_href(href: str) -> bool:
        return bool(href) and ".zip" in href.lower() and not href.lower().endswith(".zip#")

    def _zip_filename(self, url: str) -> Path:
        name = Path(urlparse(url).path).name or "download.zip"
        return self.downloads_dir / name

    def _zip_request(self, url: str, referer: str) -> scrapy.Request:
        headers = {
            "User-Agent": self.UA,
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": referer,
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
        # Regular Scrapy download (no playwright) for the binary file:
        return scrapy.Request(
            url=url,
            callback=self.save_zip,
            headers=headers,
            meta={
                "handle_httpstatus_list": [200, 403, 404, 429],
                "download_timeout": 120,
                "zip_url": url,
                "referer": referer,
            },
            dont_filter=True,
        )

    def parse_index(self, response: Response):
        hrefs = set()
        hrefs.update(
            urljoin(response.url, h)
            for h in response.css('a[href*=".zip"]::attr(href)').getall()
            if self._is_zip_href(h)
        )
        hrefs.update(
            urljoin(response.url, h)
            for h in response.xpath('//a[contains(translate(@href,"ZIP","zip"),".zip")]/@href').getall()
            if self._is_zip_href(h)
        )
        hrefs = sorted(hrefs)
        self.log(f"Found {len(hrefs)} candidate zip URLs", level=logging.INFO)

        new_hrefs = [u for u in hrefs if u not in self.ledger]
        if self.limit is not None:
            new_hrefs = new_hrefs[: max(0, int(self.limit))]
        self.log(f"{len(new_hrefs)} new ZIP(s) to fetch.", level=logging.INFO)

        referer = str(response.url)
        for url in new_hrefs:
            yield self._zip_request(url, referer)

    def save_zip(self, response: Response):
        url = response.meta.get("zip_url", response.url)
        if response.status != 200:
            self.logger.warning("Zip fetch non-200 for %s (status=%s)", url, response.status)
            return
        out_path = self._zip_filename(url)
        try:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_bytes(response.body)
            self.logger.info("Saved %s (%d bytes)", out_path, len(response.body))
            self.saved_urls.append(url)
            self.saved_paths.append(str(out_path))
        except Exception as exc:
            self.logger.warning("Failed to save %s: %r", out_path, exc)

    def closed(self, reason):
        # Update ledger
        current = _load_ledger(self.ledger_path)
        updated = current.union(self.saved_urls)
        _save_ledger(self.ledger_path, updated)
        # Persist last run summary (urls + paths)
        try:
            self.last_run_path.parent.mkdir(parents=True, exist_ok=True)
            self.last_run_path.write_text(
                json.dumps({"saved_urls": self.saved_urls, "saved_paths": self.saved_paths}, indent=2),
                encoding="utf-8",
            )
        except Exception as e:
            self.logger.warning("Failed writing last-run file: %r", e)


def extract_new_zips(
    zip_dir: Path = ZIP_DIR,
    ledger_path: Path = DOWNLOADED_JSON,
    index_url: str = SEC_INDEX_URL,
    limit: int | None = None,
) -> tuple[list[Path], int]:
    """
    Return (new_zip_paths, new_count). Always returns a tuple, even if no new files.
    """
    zip_dir.mkdir(parents=True, exist_ok=True)
    already = _load_ledger(ledger_path)
    last_run_path = ledger_path.with_suffix(".last_run.json")

    # Configure spider class attrs
    SECFtdSpider.start_url = index_url
    SECFtdSpider.downloads_dir = zip_dir

    process = CrawlerProcess(settings={})
    process.crawl(
        SECFtdSpider,
        ledger_path=ledger_path,
        last_run_path=last_run_path,
        ledger=already,
        limit=limit,
    )
    process.start()

    # Read what was saved in this run
    saved_paths: list[Path] = []
    if last_run_path.exists():
        try:
            payload = json.loads(last_run_path.read_text(encoding="utf-8"))
            saved_paths = [Path(p) for p in payload.get("saved_paths", []) if p]
        except Exception:
            saved_paths = []

    return saved_paths, len(saved_paths)
