"""
No SecurityTicker code in universe without Integration, this really is my best given my level of technical ability at this time, current time sink (at least 40hr personally at this point), and continued contribution.

"""

#!/usr/bin/env python3
import os
import time
import uuid
import zipfile
import datetime as dt
from pathlib import Path
from typing import List, Dict, Iterable, Optional, Tuple, Set

import requests


CATALOG_HTML_URL = "https://catalog.data.gov/dataset/fails-to-deliver-data"
MARKER_PREFIX = "https://www.sec.gov/files/data/fails-deliver-data/"

DESTINATION_ROOT = Path("sec-fails-to-deliver")
MAP_FILE_DIR = Path("map_files")

USER_AGENT = os.environ.get(
    "SEC_USER_AGENT",
    "SecFtdPythonDownloader/1.0 (contact@example.com)"
)
MAX_RETRIES = 5
REQUEST_SLEEP_BASE = 1
SKIP_PROCESSED_DISTRIBUTIONS = True


class DistributionMetadata:
    def __init__(self, title: str, download_url: str, process_date: dt.date):
        self.title = title
        self.download_url = download_url
        self.process_date = process_date

    def __repr__(self):
        return f"<Distribution {self.title} {self.process_date} {self.download_url}>"


class FailRecord:
    def __init__(
        self,
        settlement_date: dt.date,
        cusip: str,
        symbol: str,
        quantity: int,
        reference_price: float,
    ):
        self.settlement_date = settlement_date
        self.cusip = cusip
        self.symbol = symbol
        self.quantity = quantity
        self.reference_price = reference_price


class FailsToDeliverUniverseDownloader:
    def __init__(
        self,
        destination_folder: Path,
        map_file_dir: Optional[Path] = None,
        session: Optional[requests.Session] = None,
    ):
        self.destination_folder = destination_folder
        self.universe_folder = destination_folder / "universe"
        self.tmp_folder = destination_folder / "tmp"

        self.destination_folder.mkdir(parents=True, exist_ok=True)
        self.universe_folder.mkdir(parents=True, exist_ok=True)
        self.tmp_folder.mkdir(parents=True, exist_ok=True)

        self.session = session or requests.Session()
        self.session.headers.update(
            {
                "User-Agent": USER_AGENT,
                "Accept": "*/*",
            }
        )

        self.allowed_tickers: Optional[Set[str]] = None
        if map_file_dir is not None:
            if map_file_dir.is_dir():
                tickers = self._load_tickers_from_map_files(map_file_dir)
                if tickers:
                    self.allowed_tickers = tickers
                    print(f"[INFO] Loaded {len(self.allowed_tickers)} tickers from {map_file_dir}")
                else:
                    print(f"[INFO] No tickers found in {map_file_dir}, no filtering applied")
            else:
                print(f"[INFO] Map file dir {map_file_dir} not found, no filtering applied")

    def run(self) -> bool:
        today = dt.datetime.utcnow().date()
        distributions = self._get_distribution_metadata()
        if not distributions:
            print("[ERROR] No distribution metadata discovered.")
            return False

        processed_any = False

        for dist in distributions:
            if dist.process_date > today:
                continue

            if SKIP_PROCESSED_DISTRIBUTIONS and self._already_processed(dist.process_date):
                print(f"[INFO] Skipping {dist.title} – already processed.")
                continue

            print(f"[INFO] Downloading {dist.title} ({dist.download_url})")
            try:
                blob = self._download_binary(dist.download_url)
                if not blob:
                    print(f"[WARN] Empty payload for {dist.download_url}")
                    continue

                processed_lines = self._process_distribution_archive(dist, blob)
                if processed_lines > 0:
                    processed_any = True
                    print(
                        f"[INFO] Processed {processed_lines:,} lines for {dist.title} "
                        f"(process_date={dist.process_date})"
                    )
                else:
                    print(f"[WARN] No valid rows for {dist.title}")
            except Exception as e:
                print(f"[ERROR] Failed processing {dist.title}: {e}")

        print("[INFO] Done.")
        return processed_any

    # ---------- map_files ----------
    def _load_tickers_from_map_files(self, directory: Path) -> Set[str]:
        tickers = set()
        for p in directory.iterdir():
            if not p.is_file():
                continue
            if p.suffix.lower() != ".csv":
                continue
            stem = p.stem.strip()
            if stem:
                tickers.add(stem.upper())
        return tickers

    # ---------- metadata discovery ----------
    def _get_distribution_metadata(self) -> List[DistributionMetadata]:
        html = self._http_get_text(CATALOG_HTML_URL)
        if not html:
            return []

        links = self._scrape_download_links(html)
        results: List[DistributionMetadata] = []

        for link in links:
            norm_url = self._normalize_download_url(link)
            file_name = self._try_get_file_name(norm_url)
            if not file_name:
                continue

            parsed = self._try_parse_distribution_file_name(file_name)
            if parsed is None:
                continue
            year, month, half = parsed

            process_date = self._get_processing_date(year, month, half)
            title = f"{dt.date(year, month, 1):%B %Y}, {'first' if half == 'a' else 'second'} half"
            results.append(DistributionMetadata(title, norm_url, process_date))

        results.sort(key=lambda r: r.process_date)
        print(f"[INFO] Found {len(results)} FTD distributions")
        return results

    def _http_get_text(self, url: str) -> str:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = self.session.get(url, timeout=300)
                if resp.status_code == 404:
                    print(f"[ERROR] 404 at {url}")
                    return ""
                resp.raise_for_status()
                return resp.text
            except Exception as e:
                print(f"[WARN] GET {url} failed (attempt {attempt}/{MAX_RETRIES}): {e}")
                time.sleep(REQUEST_SLEEP_BASE * attempt)
        print(f"[ERROR] Exhausted retries for {url}")
        return ""

    def _download_binary(self, url: str) -> bytes:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = self.session.get(url, timeout=300, stream=True)
                if resp.status_code == 404:
                    print(f"[ERROR] 404 at {url}")
                    return b""
                resp.raise_for_status()
                return resp.content
            except Exception as e:
                print(f"[WARN] Binary GET {url} failed (attempt {attempt}/{MAX_RETRIES}): {e}")
                time.sleep(REQUEST_SLEEP_BASE * attempt)
        raise RuntimeError(f"Binary request failed for {url}")

    def _scrape_download_links(self, html: str) -> List[str]:
        links = set()
        s = html
        marker = MARKER_PREFIX
        while True:
            idx = s.lower().find(marker)
            if idx == -1:
                break
            s = s[idx:]
            end = s.find('"')
            if end <= 0:
                break
            links.add(s[:end])
            s = s[end:]
        return sorted(links)

    def _normalize_download_url(self, url: str) -> str:
        if url.lower().startswith("http"):
            return url
        if not url.startswith("/"):
            url = "/" + url
        return f"https://www.sec.gov{url}"

    def _try_get_file_name(self, url: str) -> str:
        from urllib.parse import urlparse

        try:
            parsed = urlparse(url)
            return Path(parsed.path).stem
        except Exception:
            return Path(url).stem

    def _try_parse_distribution_file_name(self, file_name: str) -> Optional[Tuple[int, int, str]]:
        if not file_name:
            return None
        name = file_name.strip().lower()
        if not name.startswith("cnsfails"):
            return None
        token = name[len("cnsfails") :]
        if len(token) < 7:
            return None
        try:
            year = int(token[0:4])
            month = int(token[4:6])
        except ValueError:
            return None
        half = token[6]
        if half not in ("a", "b"):
            return None
        return year, month, half

    def _get_processing_date(self, year: int, month: int, half: str) -> dt.date:
        start = dt.date(year, month, 1)
        if half.lower() == "a":
            if month == 12:
                next_month = dt.date(year + 1, 1, 1)
            else:
                next_month = dt.date(year, month + 1, 1)
            return next_month - dt.timedelta(days=1)
        if month == 12:
            next_month = dt.date(year + 1, 1, 1)
        else:
            next_month = dt.date(year, month + 1, 1)
        return dt.date(next_month.year, next_month.month, 15)

    # ---------- processing ----------
    def _already_processed(self, process_date: dt.date) -> bool:
        name = process_date.strftime("%Y%m%d") + ".csv"
        path = self.universe_folder / name
        return path.exists()

    def _process_distribution_archive(self, dist: DistributionMetadata, blob: bytes) -> int:
        import io

        processed_lines = 0
        universe_lines: List[str] = []
        symbol_lines: Dict[str, List[str]] = {}

        with zipfile.ZipFile(io.BytesIO(blob)) as zf:
            infos = zf.infolist()
            if not infos:
                print(f"[ERROR] Empty archive for {dist.title}")
                return 0
            entry = infos[0]
            with zf.open(entry, "r") as f:
                first = True
                for raw in f:
                    line = raw.decode("utf-8", errors="ignore").rstrip("\r\n")
                    if first:
                        first = False
                        continue
                    record = self._try_parse_raw_line(line)
                    if record is None:
                        continue

                    ticker = self._normalize_ticker(record.symbol)
                    if not ticker:
                        continue

                    if self.allowed_tickers is not None and ticker not in self.allowed_tickers:
                        continue

                    processed_date_str = dist.process_date.strftime("%Y%m%d")
                    settlement_str = record.settlement_date.strftime("%Y%m%d")

                    data_line = ",".join(
                        [
                            processed_date_str,
                            settlement_str,
                            record.cusip,
                            str(record.quantity),
                            f"{record.reference_price:.6f}",
                        ]
                    )

                    symbol_lines.setdefault(ticker, []).append(data_line)

                    symbol_id = ticker
                    uni_line = ",".join(
                        [
                            symbol_id,
                            ticker,
                            record.cusip,
                            str(record.quantity),
                            settlement_str,
                            f"{record.reference_price:.6f}",
                        ]
                    )
                    universe_lines.append(uni_line)
                    processed_lines += 1

        for ticker, lines in symbol_lines.items():
            self._save_content_to_file(self.destination_folder, ticker.lower(), lines)

        if universe_lines:
            date_name = dist.process_date.strftime("%Y%m%d")
            self._save_content_to_file(self.universe_folder, date_name, universe_lines)

        return processed_lines

    def _try_parse_raw_line(self, line: str) -> Optional[FailRecord]:
        if not line or "|" not in line:
            return None
        parts = line.split("|")
        if len(parts) < 6:
            return None

        date_str = parts[0].strip()
        try:
            d = dt.datetime.strptime(date_str, "%Y%m%d").date()
        except ValueError:
            return None

        qty_str = parts[3].replace(",", "").strip()
        try:
            qty = int(qty_str)
        except ValueError:
            return None
        if qty <= 0:
            return None

        symbol = parts[2].strip()
        if not symbol:
            return None

        price = 0.0
        if len(parts) > 5:
            price_str = parts[5].replace(",", "").strip()
            if price_str:
                try:
                    price = float(price_str)
                except ValueError:
                    price = 0.0

        cusip = parts[1].strip()
        return FailRecord(d, cusip, symbol, qty, price)

    def _normalize_ticker(self, ticker: str) -> str:
        if not ticker:
            return ""

        lower = ticker.lower()
        if "defunct" in lower:
            for delim in ("-", "_"):
                idx = ticker.find(delim)
                if idx != -1:
                    ticker = ticker[:idx]
                    break

        ticker = ticker.strip()
        if not ticker:
            return ""

        out = []
        for ch in ticker:
            if ch.isalnum():
                out.append(ch.upper())
            elif ch in (".", "/", "-", "_"):
                out.append(".")
        cleaned = "".join(out).strip(".")
        return cleaned

    # ---------- file writing ----------
    def _save_content_to_file(
        self, destination_folder: Path, name: str, contents: Iterable[str]
    ) -> None:
        final_path = destination_folder / f"{name}.csv"
        new_lines = list(contents)
        if not new_lines:
            return

        all_lines = set()
        if final_path.exists():
            with final_path.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.rstrip("\n\r")
                    if line:
                        all_lines.add(line)

        for l in new_lines:
            if l:
                all_lines.add(l)

        if not all_lines:
            return

        if destination_folder.name.lower() == "universe":
            ordered = sorted(all_lines, key=lambda x: x.split(",")[0])
        else:
            def date_key(row: str) -> dt.date:
                first = row.split(",")[0]
                try:
                    return dt.datetime.strptime(first, "%Y%m%d").date()
                except Exception:
                    return dt.date(1900, 1, 1)

            ordered = sorted(all_lines, key=date_key)

        tmp_name = self.tmp_folder / f"{uuid.uuid4().hex}.tmp"
        with tmp_name.open("w", encoding="utf-8", newline="") as f:
            for row in ordered:
                f.write(row + "\n")
        tmp_name.replace(final_path)


def main():
    downloader = FailsToDeliverUniverseDownloader(
        destination_folder=DESTINATION_ROOT,
        map_file_dir=MAP_FILE_DIR,
    )
    downloader.run()


if __name__ == "__main__":
    main()
