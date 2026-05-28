"""
Madrid CRTM Real-Time Arrivals Collector
=========================================
Polls the CRTM GetStopsTimes API for all stops across all public transport
modes in the Madrid metropolitan area (Metro, Cercanías, EMT buses, light
rail, interurban buses).

Designed to run 6× daily via GitHub Actions and commit results to:
  https://github.com/nicolas-izquierdo/madrid-transport-times

Each run writes one gzipped CSV to data/YYYY-MM/YYYY-MM-DD_HHMM.csv.gz.
Stop metadata is cached locally and refreshed weekly.

Author contact: paizquie@clio.uc3m.es
"""

import csv
import gzip
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MADRID_TZ = ZoneInfo("Europe/Madrid")
CRTM_API = "https://www.crtm.es/widgets/api"
HEADERS = {
    "User-Agent": (
        "MadridTransportResearch/1.0 "
        "(+https://github.com/nicolas-izquierdo/madrid-transport-times; "
        "academic research, UC3M)"
    ),
    "Accept": "application/json",
}

# CRTM mode identifiers used in the API
MODES = {
    "metro": 4,
    "cercanias": 5,
    "emt": 6,
    "interurban": 8,
    "light_rail": 10,
}

HERE = Path(__file__).parent
CACHE_FILE = HERE / "stops_cache.json"
DATA_DIR = HERE / "data"
LOG_DIR = HERE / "logs"

CACHE_MAX_AGE_DAYS = 7
MAX_WORKERS = 8          # parallel API calls — stay polite to CRTM server
REQUEST_TIMEOUT = 15     # seconds per HTTP request
RETRY_ATTEMPTS = 2
INTER_MODE_SLEEP = 1.0   # seconds between mode fetches during cache build

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging() -> None:
    LOG_DIR.mkdir(exist_ok=True)
    log_path = LOG_DIR / f"{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.log"
    fmt = "%(asctime)s %(levelname)-8s %(message)s"
    handlers: list[logging.Handler] = [
        logging.FileHandler(log_path, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ]
    logging.basicConfig(level=logging.INFO, format=fmt, handlers=handlers)


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _get(session: requests.Session, endpoint: str, params: dict) -> dict | None:
    url = f"{CRTM_API}/{endpoint}"
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            r = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as exc:
            logging.debug("HTTP %s for %s (attempt %d)", exc.response.status_code, url, attempt)
        except Exception as exc:
            logging.debug("Request error %s for %s (attempt %d)", exc, url, attempt)
        if attempt < RETRY_ATTEMPTS:
            time.sleep(1)
    return None


# ---------------------------------------------------------------------------
# Stop cache — built from CRTM API line/stop endpoints
# ---------------------------------------------------------------------------

def _fetch_lines(mode_id: int, session: requests.Session) -> list[dict]:
    data = _get(session, "GetLinesInformation.php", {"codMode": mode_id})
    if not data:
        return []
    lines = data.get("linesInformation", {}).get("line", [])
    if isinstance(lines, dict):
        lines = [lines]
    return lines or []


def _fetch_stops_for_line(line_code: str, session: requests.Session) -> list[dict]:
    data = _get(session, "GetStops.php", {"codLine": line_code})
    if not data:
        return []
    stops = data.get("stops", {}).get("stop", [])
    if isinstance(stops, dict):
        stops = [stops]
    return stops or []


def build_stop_cache(session: requests.Session) -> dict:
    """
    Enumerate all stops across all modes via the CRTM API.
    Returns {stop_code: {name, mode, lines: [{code, name}]}}.
    """
    stops: dict[str, dict] = {}
    for mode_name, mode_id in MODES.items():
        logging.info("  Fetching lines for mode: %s (id=%d)", mode_name, mode_id)
        lines = _fetch_lines(mode_id, session)
        logging.info("    Found %d lines", len(lines))
        for line in lines:
            line_code = line.get("codLine", "")
            line_name = line.get("headerName") or line.get("codLine", "")
            if not line_code:
                continue
            line_stops = _fetch_stops_for_line(line_code, session)
            for stop in line_stops:
                code = stop.get("codStop") or stop.get("codStopObt", "")
                if not code:
                    continue
                if code not in stops:
                    stops[code] = {
                        "name": stop.get("name", ""),
                        "mode": mode_name,
                        "lines": [],
                    }
                stops[code]["lines"].append({"code": line_code, "name": line_name})
        time.sleep(INTER_MODE_SLEEP)

    logging.info("Stop cache built: %d unique stops", len(stops))
    return stops


def load_stop_cache(session: requests.Session) -> dict:
    if CACHE_FILE.exists():
        age_days = (time.time() - CACHE_FILE.stat().st_mtime) / 86400
        if age_days < CACHE_MAX_AGE_DAYS:
            cached = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
            logging.info("Loaded stop cache: %d stops (%.1f days old)", len(cached), age_days)
            return cached
    logging.info("Building stop cache (first run or cache expired)...")
    stops = build_stop_cache(session)
    if stops:
        CACHE_FILE.write_text(
            json.dumps(stops, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    return stops


# ---------------------------------------------------------------------------
# Real-time arrival polling
# ---------------------------------------------------------------------------

def _poll_stop(
    stop_code: str,
    stop_info: dict,
    session: requests.Session,
    collected_at: datetime,
) -> list[dict]:
    data = _get(
        session,
        "GetStopsTimes.php",
        {"codStop": stop_code, "type": "arrival", "orderBy": "departure", "nextArrivals": "3"},
    )
    if not data:
        return []

    times_block = data.get("stopTimes", {}).get("times", {})
    arrivals = times_block.get("Time", [])
    if isinstance(arrivals, dict):
        arrivals = [arrivals]
    if not arrivals:
        return []

    rows = []
    for arr in arrivals:
        rows.append({
            "collected_at":       collected_at.isoformat(),
            "collected_at_local": collected_at.astimezone(MADRID_TZ).isoformat(),
            "stop_code":          stop_code,
            "stop_name":          stop_info.get("name", ""),
            "mode":               stop_info.get("mode", ""),
            "line_code":          arr.get("line", ""),
            "line_name":          arr.get("codLine", ""),
            "destination":        arr.get("destination", ""),
            "arrival_time":       arr.get("time", ""),
            "minutes_to_arrival": arr.get("busTimeLeft", ""),
        })
    return rows


def collect_all(stops: dict) -> list[dict]:
    collected_at = datetime.now(timezone.utc)
    all_rows: list[dict] = []
    n_failed = 0

    with requests.Session() as session:
        session.headers.update(HEADERS)
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {
                pool.submit(_poll_stop, code, info, session, collected_at): code
                for code, info in stops.items()
            }
            total = len(futures)
            for i, future in enumerate(as_completed(futures), 1):
                rows = future.result()
                if rows:
                    all_rows.extend(rows)
                else:
                    n_failed += 1
                if i % 200 == 0 or i == total:
                    logging.info(
                        "  Progress: %d/%d stops polled — %d arrival records so far",
                        i, total, len(all_rows),
                    )

    logging.info(
        "Collection complete: %d arrival records from %d stops (%d stops returned no data)",
        len(all_rows), total - n_failed, n_failed,
    )
    return all_rows


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

FIELDNAMES = [
    "collected_at",
    "collected_at_local",
    "stop_code",
    "stop_name",
    "mode",
    "line_code",
    "line_name",
    "destination",
    "arrival_time",
    "minutes_to_arrival",
]


def save_results(rows: list[dict], now: datetime) -> Path | None:
    if not rows:
        logging.warning("No rows to save — skipping file write")
        return None

    month_dir = DATA_DIR / now.strftime("%Y-%m")
    month_dir.mkdir(parents=True, exist_ok=True)
    out_path = month_dir / f"{now.strftime('%Y-%m-%d_%H%M')}.csv.gz"

    with gzip.open(out_path, "wt", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    size_kb = out_path.stat().st_size / 1024
    logging.info("Saved %d rows → %s (%.1f KB compressed)", len(rows), out_path, size_kb)
    return out_path


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    setup_logging()
    logging.info("=" * 60)
    logging.info("Madrid CRTM Arrivals Collection — %s UTC", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"))
    logging.info("=" * 60)

    with requests.Session() as session:
        session.headers.update(HEADERS)
        stops = load_stop_cache(session)

    if not stops:
        logging.error("Stop cache is empty — cannot proceed")
        sys.exit(1)

    rows = collect_all(stops)
    now = datetime.now(timezone.utc)
    save_results(rows, now)

    logging.info("Done.")


if __name__ == "__main__":
    main()
