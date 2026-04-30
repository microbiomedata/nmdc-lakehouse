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
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential


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
    p.add_argument("--chunk-size", type=int, default=1024 * 1024,
                   help="Streaming download chunk size in bytes (default: 1 MiB)")
    return p.parse_args()


class TraversalError(ValueError):
    """Raised when a URL would resolve to a path outside the cache root."""


def cache_path_for(cache_root: Path, url: str) -> Path:
    """Map a URL to a local cache path, rejecting traversal tricks.

    The URL's path component is appended under `cache_root`. We then verify
    the resolved path stays within `cache_root` so a manifest with `..` or
    absolute-path segments cannot cause writes outside the cache directory.
    """
    raw_path = urlparse(url).path.lstrip("/")
    if not raw_path:
        raise TraversalError(f"URL has no path component: {url!r}")
    candidate = (cache_root / raw_path).resolve()
    root      = cache_root.resolve()
    if root != candidate and root not in candidate.parents:
        raise TraversalError(
            f"resolved path {candidate} escapes cache root {root} (url={url!r})"
        )
    return candidate


def _is_retryable(exc: BaseException) -> bool:
    """Retry on transport errors and 5xx/429, but not on permanent 4xx."""
    if isinstance(exc, requests.HTTPError):
        resp = exc.response
        if resp is None:
            return True
        status = resp.status_code
        if status == 429:
            return True
        if 400 <= status < 500:
            return False
        return True
    return isinstance(exc, requests.RequestException)


def make_fetcher(session_factory, retries: int, timeout: int, chunk_size: int):
    """Build a fetcher that streams a URL to a destination Path.

    `session_factory()` must return a thread-local `requests.Session` —
    Sessions are not thread-safe, so each worker gets its own.
    """
    @retry(
        retry=retry_if_exception(_is_retryable),
        stop=stop_after_attempt(retries),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def _fetch(url: str, dest: Path) -> None:
        session = session_factory()
        with session.get(url, timeout=timeout, stream=True) as r:
            r.raise_for_status()
            with open(dest, "wb") as fh:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    if chunk:
                        fh.write(chunk)

    return _fetch


def _classify_http_error(exc: BaseException) -> str:
    """Bucket an exception as 'missing' (4xx other than 429) or 'error'."""
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        status = exc.response.status_code
        if 400 <= status < 500 and status != 429:
            return "missing"
    return "error"


def download_one(url: str, cache_root: Path, fetch) -> tuple[str, str | None]:
    """Returns ('cached'|'downloaded'|'missing'|'error', error_msg_or_None)."""
    try:
        path = cache_path_for(cache_root, url)
    except TraversalError as exc:
        return ("error", f"{url}: {exc}")
    if path.exists():
        return ("cached", None)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Per-thread temp suffix avoids the race where two threads fetch the same URL
    # (the manifest can contain duplicate URLs) and one's atomic rename consumes
    # the other's tmp.
    tmp = path.with_suffix(f"{path.suffix}.{threading.get_ident()}.tmp")
    try:
        fetch(url, tmp)
        tmp.replace(path)
        return ("downloaded", None)
    except Exception as exc:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        return (_classify_http_error(exc), f"{url}: {exc}")


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
    n_cached = 0
    for u in urls:
        try:
            if cache_path_for(args.cache_dir, u).exists():
                n_cached += 1
        except TraversalError:
            pass
    log.info(f"cache:    {n_cached:,} already on disk; will fetch {len(urls) - n_cached:,}")
    log.info(f"workers:  {args.workers}")

    # Per-thread Sessions — `requests.Session` is not thread-safe, so each
    # worker gets its own via `threading.local()`. The User-Agent is set
    # once when the Session is first created on a given thread.
    tls = threading.local()

    def session_factory() -> requests.Session:
        s = getattr(tls, "session", None)
        if s is None:
            s = requests.Session()
            s.headers.update({"User-Agent": args.user_agent})
            tls.session = s
        return s

    fetch = make_fetcher(session_factory, args.retries, args.timeout, args.chunk_size)

    counts = {"cached": 0, "downloaded": 0, "missing": 0, "error": 0}
    errors_sample: list[str] = []
    missing_sample: list[str] = []
    t0 = time.monotonic()

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = [ex.submit(download_one, u, args.cache_dir, fetch) for u in urls]
        for i, fut in enumerate(as_completed(futures), 1):
            status, msg = fut.result()
            counts[status] += 1
            if status == "error" and len(errors_sample) < 10:
                errors_sample.append(msg)
            elif status == "missing" and len(missing_sample) < 10:
                missing_sample.append(msg)
            if i == 1 or i % args.progress_every == 0 or i == len(urls):
                elapsed = time.monotonic() - t0
                rate    = i / elapsed if elapsed > 0 else 0
                eta_min = ((len(urls) - i) / rate) / 60 if rate > 0 else 0
                log.info(
                    f"  {i}/{len(urls)} ({rate:.1f}/s, eta {eta_min:.1f}m) "
                    f"cached={counts['cached']:,} dl={counts['downloaded']:,} "
                    f"missing={counts['missing']:,} err={counts['error']:,}"
                )

    elapsed = time.monotonic() - t0
    log.info(f"Done in {elapsed/60:.1f} min")
    log.info(f"  cached    : {counts['cached']:,}")
    log.info(f"  downloaded: {counts['downloaded']:,}")
    log.info(f"  missing   : {counts['missing']:,}  (4xx, not retried)")
    log.info(f"  errors    : {counts['error']:,}")
    if missing_sample:
        log.info("  sample missing:")
        for m in missing_sample:
            log.info(f"    {m}")
    if errors_sample:
        log.info("  sample errors:")
        for e in errors_sample:
            log.info(f"    {e}")
    # 4xx misses are expected (e.g. NMDC data_object_set has known-bad URLs)
    # and do NOT cause a non-zero exit. Only true errors do.
    return 0 if counts["error"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
