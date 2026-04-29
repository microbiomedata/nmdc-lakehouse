#!/usr/bin/env python
"""Download URLs in parallel from a CSV manifest into a local cache directory.

Designed to run **outside** Jupyter so a multi-hour download is not subject to
notebook-kernel restarts. Files are written atomically (per-thread `.tmp`
suffix + atomic rename), so re-runs skip files already on disk.

The companion notebooks (e.g. `fetch_ko_ec_annotations.ipynb`,
`fetch_taxonomy_summaries.ipynb`) emit a `manifest.csv` next to their output
directory; pass that file in here.

Typical use:

    nohup python scripts/download_to_cache.py \\
        --manifest notebooks/loaded_ko_ec/manifest.csv \\
        --cache-dir notebooks/loaded_ko_ec/raw_cache \\
        --workers 8 \\
        > notebooks/loaded_ko_ec/download.log 2>&1 &

Then `tail -f notebooks/loaded_ko_ec/download.log` to watch progress, and
re-open the notebook to parse the cached files once it finishes.
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlparse

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--manifest", required=True, type=Path,
                   help="CSV file with at least a 'url' column")
    p.add_argument("--cache-dir", required=True, type=Path,
                   help="Cache root; files land at <cache-dir>/<url-path>")
    p.add_argument("--workers", type=int, default=8,
                   help="Parallel download workers (default: 8). Lower if upstream throttles.")
    p.add_argument("--timeout", type=int, default=120,
                   help="Per-request HTTP timeout in seconds (default: 120)")
    p.add_argument("--retries", type=int, default=3,
                   help="Retry attempts per URL on transient errors (default: 3)")
    p.add_argument("--progress-every", type=int, default=10,
                   help="Print a progress line every N completions (default: 10)")
    p.add_argument("--user-agent", default="nmdc-lakehouse/download_to_cache",
                   help="HTTP User-Agent header value")
    return p.parse_args()


def cache_path_for(cache_root: Path, url: str) -> Path:
    return cache_root / urlparse(url).path.lstrip("/")


def make_fetcher(session: requests.Session, retries: int, timeout: int):
    @retry(
        retry=retry_if_exception_type(requests.RequestException),
        stop=stop_after_attempt(retries),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def _fetch(url: str) -> str:
        r = session.get(url, timeout=timeout)
        r.raise_for_status()
        return r.text
    return _fetch


def download_one(url: str, cache_root: Path, fetch) -> tuple[str, str | None]:
    """Returns ('cached'|'downloaded'|'error', error_msg_or_None)."""
    try:
        path = cache_path_for(cache_root, url)
        if path.exists():
            return ("cached", None)
        text = fetch(url)
        path.parent.mkdir(parents=True, exist_ok=True)
        # Per-thread temp suffix avoids the race where two threads fetch the same URL
        # (the manifest can contain duplicate URLs) and one's atomic rename consumes
        # the other's tmp.
        tmp = path.with_suffix(f"{path.suffix}.{threading.get_ident()}.tmp")
        tmp.write_text(text)
        tmp.replace(path)
        return ("downloaded", None)
    except Exception as exc:
        return ("error", f"{url}: {exc}")


def main() -> int:
    args = parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stdout,
    )
    log = logging.getLogger("download_to_cache")

    if not args.manifest.exists():
        log.error(f"manifest not found: {args.manifest}")
        return 2

    args.cache_dir.mkdir(parents=True, exist_ok=True)

    urls: list[str] = []
    seen: set[str] = set()
    with open(args.manifest, newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None or "url" not in reader.fieldnames:
            log.error(f"manifest must have a 'url' column; found columns: {reader.fieldnames}")
            return 2
        for row in reader:
            url = (row["url"] or "").strip()
            if url and url not in seen:
                seen.add(url)
                urls.append(url)

    log.info(f"manifest: {len(urls):,} unique URLs from {args.manifest}")
    n_cached = sum(1 for u in urls if cache_path_for(args.cache_dir, u).exists())
    log.info(f"cache:    {n_cached:,} already on disk; will fetch {len(urls) - n_cached:,}")
    log.info(f"workers:  {args.workers}")

    session = requests.Session()
    session.headers.update({"User-Agent": args.user_agent})
    fetch = make_fetcher(session, args.retries, args.timeout)

    counts = {"cached": 0, "downloaded": 0, "error": 0}
    errors_sample: list[str] = []
    t0 = time.monotonic()

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = [ex.submit(download_one, u, args.cache_dir, fetch) for u in urls]
        for i, fut in enumerate(as_completed(futures), 1):
            status, msg = fut.result()
            counts[status] += 1
            if status == "error" and len(errors_sample) < 10:
                errors_sample.append(msg)
            if i == 1 or i % args.progress_every == 0 or i == len(urls):
                elapsed = time.monotonic() - t0
                rate    = i / elapsed if elapsed > 0 else 0
                eta_min = ((len(urls) - i) / rate) / 60 if rate > 0 else 0
                log.info(
                    f"  {i}/{len(urls)} ({rate:.1f}/s, eta {eta_min:.1f}m) "
                    f"cached={counts['cached']:,} dl={counts['downloaded']:,} err={counts['error']:,}"
                )

    elapsed = time.monotonic() - t0
    log.info(f"Done in {elapsed/60:.1f} min")
    log.info(f"  cached    : {counts['cached']:,}")
    log.info(f"  downloaded: {counts['downloaded']:,}")
    log.info(f"  errors    : {counts['error']:,}")
    if errors_sample:
        log.info("  sample errors:")
        for e in errors_sample:
            log.info(f"    {e}")
    return 0 if counts["error"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
