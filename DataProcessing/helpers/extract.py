import json
from io import BytesIO
from pathlib import Path
import logging
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from zipfile import ZipFile

import requests
from bs4 import BeautifulSoup as BS

from paths import SEC_URL, HDRS, dirs, MAX_WORKERS

logging.basicConfig(level=logging.INFO, format="%(message)s")

RAW: Path = dirs["RAW"]
STATE: Path = dirs["STATE"]
SEEN: Path = STATE / "downloaded.json"

def extract() -> None:
    seen: set[str] = set(json.loads(SEEN.read_text("utf-8"))) if SEEN.exists() else set()

    with requests.Session() as sess:
        sess.headers.update(HDRS)

        # discover all .zip/.txt links (absolute)
        html = sess.get(SEC_URL, timeout=60).text
        soup = BS(html, "html.parser")
        urls = sorted({
            urljoin(SEC_URL, a["href"])
            for a in soup.find_all("a", href=True)
            if a["href"].lower().endswith((".zip", ".txt"))
        })

        new = [u for u in urls if u not in seen]
        logging.info(f"✅ {len(new)} new file(s) to download/extract")

        if new:
            futs = {}
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                for u in new:
                    futs[ex.submit(sess.get, u, timeout=300)] = u

                done = 0
                for fut in as_completed(futs):
                    url = futs[fut]
                    try:
                        r = fut.result()
                        r.raise_for_status()

                        if url.lower().endswith(".zip"):
                            with ZipFile(BytesIO(r.content)) as z:
                                z.extractall(RAW)
                        else:
                            (RAW / Path(url.split("?", 1)[0]).name).write_bytes(r.content)

                        seen.add(url)
                    except Exception as e:
                        logging.info(f"⚠️  {url} -> {e}")
                    finally:
                        done += 1
                        if done % 10 == 0 or done == len(futs):
                            logging.info(f"… {done}/{len(futs)} processed")

    SEEN.write_text(json.dumps(sorted(seen), indent=2), encoding="utf-8")







