"""
Madrid CRTM Real-Time Arrivals Collector
=========================================
Polls the CRTM GetStopsTimes API for all stops across all public transport
modes in the Madrid metropolitan area (Metro, Cercanías, EMT buses, light
rail, interurban buses).

Stop discovery uses the CRTM spatial CSV files already in the project
data/ folder — no dependency on the unreliable CRTM line-listing API.

Designed to run 6× daily via GitHub Actions and commit results to:
  https://github.com/nicolas-izquierdo/madrid-transport-times

Each run writes one gzipped CSV to data/YYYY-MM/YYYY-MM-DD_HHMM.csv.gz.

Author contact: paizquie@clio.uc3m.es
"""

import csv
import gzip
import json
import logging
import math
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

MADRID_TZ   = ZoneInfo("Europe/Madrid")
CRTM_API    = "https://www.crtm.es/widgets/api/GetStopsTimes.php"
HEADERS     = {
    "User-Agent": (
        "MadridTransportResearch/1.0 "
        "(+https://github.com/nicolas-izquierdo/madrid-transport-times; "
        "academic research, UC3M)"
    ),
    "Accept": "application/json",
}

HERE        = Path(__file__).parent
DATA_DIR    = HERE / "data"
LOG_DIR     = HERE / "logs"
CACHE_FILE  = HERE / "stops_cache.json"

# Path to the project data/ folder (one level up from times_API/)
PROJECT_DATA = HERE.parent / "data"

# Maps mode name → (CSV file relative to PROJECT_DATA, id_column, name_column)
# Files with many duplicates per stop (stops_by_route) need deduplication.
STOP_SOURCES = {
    "metro": (
        "M4_metro/red/csv/M4_stations.csv",
        "IDESTACION", "DENOMINACION",
    ),
    "cercanias": (
        "M5_cercanias/red/csv/M5_stations.csv",
        "IDESTACION", "DENOMINACION",
    ),
    "emt": (
        "M6_emt/csv/M6_stops_by_route.csv",   # deduplicated by IDFESTACION
        "IDFESTACION", "DENOMINACION",
    ),
    "interurban": (
        "M8_interurban/csv/M8_stops_by_route.csv",
        "IDFESTACION", "DENOMINACION",
    ),
    "light_rail": (
        "M10_light_rail/red/csv/M10_stations.csv",
        "IDESTACION", "DENOMINACION",
    ),
}

CACHE_MAX_AGE_DAYS = 7
MAX_WORKERS        = 25     # parallel API calls; CRTM is I/O-bound, ~25 is safe
REQUEST_TIMEOUT    = 12     # seconds; fail fast and move on
RETRY_ATTEMPTS     = 2
RETRY_SLEEP        = 1      # seconds between retries

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging() -> None:
    LOG_DIR.mkdir(exist_ok=True)
    log_path = LOG_DIR / f"{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.log"
    fmt = "%(asctime)s %(levelname)-8s %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


# ---------------------------------------------------------------------------
# Stop cache — built from local CSV files (reliable; no API dependency)
# ---------------------------------------------------------------------------

def build_stop_cache() -> dict:
    """
    Read all stop IDs and names from the project's CSV spatial files.
    Returns {stop_code: {name, mode}}.
    """
    stops: dict[str, dict] = {}

    for mode, (rel_path, id_col, name_col) in STOP_SOURCES.items():
        csv_path = PROJECT_DATA / rel_path
        if not csv_path.exists():
            logging.warning("Stop CSV not found: %s — skipping mode %s", csv_path, mode)
            continue

        seen: set[str] = set()
        count = 0
        with open(csv_path, encoding="utf-8-sig", errors="replace") as fh:
            for row in csv.DictReader(fh):
                code = row.get(id_col, "").strip()
                name = row.get(name_col, "").strip()
                if not code or code in seen:
                    continue
                seen.add(code)
                stops[code] = {"name": name, "mode": mode}
                count += 1

        logging.info("  %-12s %4d unique stops  (%s)", mode, count, csv_path.name)

    logging.info("Stop cache total: %d stops across %d modes", len(stops), len(STOP_SOURCES))
    return stops


def load_stop_cache() -> dict:
    if CACHE_FILE.exists():
        age_days = (time.time() - CACHE_FILE.stat().st_mtime) / 86400
        # In CI (GitHub Actions) PROJECT_DATA won't exist — always use the cache
        project_data_available = PROJECT_DATA.exists()
        if age_days < CACHE_MAX_AGE_DAYS or not project_data_available:
            cached = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
            logging.info(
                "Loaded stop cache: %d stops (%.1f days old%s)",
                len(cached), age_days,
                ", CI mode — no local data" if not project_data_available else "",
            )
            return cached
        logging.info("Stop cache expired (%.1f days) — rebuilding from CSV files", age_days)
    else:
        logging.info("No stop cache — building from CSV files")

    stops = build_stop_cache()
    if stops:
        CACHE_FILE.write_text(json.dumps(stops, ensure_ascii=False, indent=2), encoding="utf-8")
    return stops


# ---------------------------------------------------------------------------
# Real-time arrival polling
# ---------------------------------------------------------------------------

def _minutes_until(arrival_iso: str, collected_at: datetime) -> int | None:
    """Compute minutes between collection time and predicted arrival."""
    try:
        arr = datetime.fromisoformat(arrival_iso)
        if arr.tzinfo is None:
            arr = arr.replace(tzinfo=MADRID_TZ)
        delta = (arr - collected_at).total_seconds()
        return math.floor(delta / 60)
    except Exception:
        return None


def _poll_stop(
    stop_code: str,
    stop_info: dict,
    session: requests.Session,
    collected_at: datetime,
) -> list[dict]:
    params = {
        "codStop":     stop_code,
        "type":        "arrival",
        "orderBy":     "departure",
        "nextArrivals": "3",
    }
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            r = session.get(CRTM_API, params=params, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            data = r.json()
            break
        except Exception as exc:
            if attempt < RETRY_ATTEMPTS:
                time.sleep(RETRY_SLEEP)
            else:
                logging.debug("Failed stop %s after %d attempts: %s", stop_code, attempt, exc)
                return []

    times_block = data.get("stopTimes", {}).get("times", {})
    arrivals = times_block.get("Time", [])
    if isinstance(arrivals, dict):
        arrivals = [arrivals]
    if not arrivals:
        return []

    rows = []
    for arr in arrivals:
        line_obj  = arr.get("line", {}) if isinstance(arr.get("line"), dict) else {}
        arrival_t = arr.get("time", "")
        rows.append({
            "collected_at":       collected_at.isoformat(),
            "collected_at_local": collected_at.astimezone(MADRID_TZ).isoformat(),
            "stop_code":          stop_code,
            "stop_name":          stop_info.get("name", ""),
            "mode":               stop_info.get("mode", ""),
            "line_code":          line_obj.get("codLine", ""),
            "line_name":          line_obj.get("shortDescription", ""),
            "destination":        arr.get("destination", ""),
            "arrival_time":       arrival_t,
            "minutes_to_arrival": _minutes_until(arrival_t, collected_at),
        })
    return rows


def collect_all(stops: dict) -> list[dict]:
    collected_at = datetime.now(timezone.utc)
    all_rows: list[dict] = []
    n_empty = 0

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
                    n_empty += 1
                if i % 300 == 0 or i == total:
                    logging.info(
                        "  %d/%d stops polled — %d arrival records, %d stops with no data",
                        i, total, len(all_rows), n_empty,
                    )

    logging.info(
        "Collection complete: %d records from %d stops (%d stops returned no data)",
        len(all_rows), total - n_empty, n_empty,
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
    logging.info("Saved %d rows -> %s (%.1f KB gzipped)", len(rows), out_path, size_kb)
    return out_path


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    setup_logging()
    logging.info("=" * 60)
    logging.info(
        "Madrid CRTM Arrivals Collection — %s UTC",
        datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"),
    )
    logging.info("=" * 60)

    stops = load_stop_cache()
    if not stops:
        logging.error("Stop list is empty — cannot proceed")
        sys.exit(1)

    rows = collect_all(stops)
    now  = datetime.now(timezone.utc)
    save_results(rows, now)
    logging.info("Done.")


if __name__ == "__main__":
    main()
