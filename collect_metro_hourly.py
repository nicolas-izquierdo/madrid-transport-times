"""
Madrid Metro Real-Time Arrivals — Hourly High-Frequency Collector
=================================================================
Polls all Metro de Madrid stations every minute for ~55 minutes,
then writes a single hourly Parquet file directly to Hugging Face
Datasets. Zero data stored in the GitHub repository.

Design principles (inspired by subwaydata.nyc and mta-bus-archive):
  - Immutable, append-only files per hour.
  - Clear partition scheme: metro/YYYY-MM/YYYY-MM-DD_HH00.parquet
  - poll_index field tracks which 1-min interval produced each row.
  - Graceful degradation: API failures are logged, never crash the run.
  - Zstandard compression via Parquet (best ratio + fast decompression).
  - Checkpoint pushes every CHECKPOINT_EVERY polls — if the job dies,
    the last checkpoint is already on HF (max data loss = CHECKPOINT_EVERY min).
    Each checkpoint overwrites the same file; HF git history preserves all
    intermediate versions automatically.

Runs as a GitHub Actions job (cron: every hour). Each job:
  1. Loops 55 × 1-minute collection rounds (55 min of data)
  2. Pushes a checkpoint Parquet to HF every CHECKPOINT_EVERY polls
  3. Final push at the end — one clean file per hour on HF

Required env var (GitHub Actions secret):
  HF_TOKEN — Hugging Face write-access token

Author: paizquie@clio.uc3m.es
"""

import json
import logging
import math
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from huggingface_hub import HfApi

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MADRID_TZ    = ZoneInfo("Europe/Madrid")
CRTM_API     = "https://www.crtm.es/widgets/api/GetStopsTimes.php"
HEADERS      = {
    "User-Agent": (
        "MadridTransportResearch/1.0 "
        "(+https://github.com/nicolas-izquierdo/madrid-transport-times; "
        "academic research, UC3M)"
    )
}

HERE         = Path(__file__).parent
CACHE_FILE   = HERE / "stops_cache.json"
LOG_DIR      = HERE / "logs"

HF_REPO      = "nicolas-izquierdo/madrid-transport-times"
HF_REPO_TYPE = "dataset"

# Collection parameters
POLL_INTERVAL_S  = 60    # 1 minute between poll starts (gold standard — subwaydata.nyc)
N_POLLS          = 55    # 55 × 1 min = 55 min of data per hourly job
MAX_WORKERS      = 20    # parallel API calls for ~293 Metro stops
REQUEST_TIMEOUT  = 12    # seconds; fail fast, retry once
RETRY_ATTEMPTS   = 2
CHECKPOINT_EVERY = 10    # push to HF every N polls (overwrites same file); max data loss = N min

# Parquet schema — explicit types for storage efficiency
# Using dictionary encoding for low-cardinality string columns
SCHEMA = pa.schema([
    pa.field("collected_at",       pa.timestamp("us", tz="UTC"),
             metadata={"description": "UTC timestamp when API was polled"}),
    pa.field("stop_code",          pa.string(),
             metadata={"description": "CRTM stop identifier, e.g. 4_38"}),
    pa.field("stop_name",          pa.string()),
    pa.field("line_code",          pa.string(),
             metadata={"description": "CRTM line code, e.g. 4__2___"}),
    pa.field("line_name",          pa.string(),
             metadata={"description": "Short line number, e.g. 2, 6, 10"}),
    pa.field("destination",        pa.string()),
    pa.field("arrival_time",       pa.timestamp("us", tz="UTC"),
             metadata={"description": "Predicted arrival time (UTC)"}),
    pa.field("minutes_to_arrival", pa.int16(),
             metadata={"description": "Minutes until predicted arrival at poll time"}),
    pa.field("poll_index",         pa.int8(),
             metadata={"description": "Which 1-min interval (0-54) within this hourly run"}),
])


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
# Stop loading — Metro only
# ---------------------------------------------------------------------------

def load_metro_stops() -> dict:
    if not CACHE_FILE.exists():
        logging.error("stops_cache.json not found at %s", CACHE_FILE)
        sys.exit(1)
    all_stops = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
    metro = {k: v for k, v in all_stops.items() if v.get("mode") == "metro"}
    logging.info("Metro stops loaded: %d", len(metro))
    return metro


# ---------------------------------------------------------------------------
# Single-stop polling
# ---------------------------------------------------------------------------

def _minutes_until(arrival_iso: str, collected_at: datetime) -> int:
    try:
        arr = datetime.fromisoformat(arrival_iso)
        if arr.tzinfo is None:
            arr = arr.replace(tzinfo=MADRID_TZ)
        return math.floor((arr - collected_at).total_seconds() / 60)
    except Exception:
        return -1


def _parse_arrival_utc(arrival_iso: str) -> datetime | None:
    try:
        arr = datetime.fromisoformat(arrival_iso)
        if arr.tzinfo is None:
            arr = arr.replace(tzinfo=MADRID_TZ)
        return arr.astimezone(timezone.utc)
    except Exception:
        return None


def _poll_stop(
    stop_code: str,
    stop_name: str,
    session: requests.Session,
    collected_at: datetime,
    poll_index: int,
) -> list[dict]:
    params = {
        "codStop":     stop_code,
        "type":        "arrival",
        "orderBy":     "departure",
        "nextArrivals": "3",
    }
    data = None
    for attempt in range(RETRY_ATTEMPTS):
        try:
            r = session.get(CRTM_API, params=params, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            data = r.json()
            break
        except Exception as exc:
            if attempt < RETRY_ATTEMPTS - 1:
                time.sleep(1)
            else:
                logging.debug("Stop %s failed: %s", stop_code, exc)
                return []

    arrivals = data.get("stopTimes", {}).get("times", {}).get("Time", [])
    if isinstance(arrivals, dict):
        arrivals = [arrivals]
    if not arrivals:
        return []

    rows = []
    for arr in arrivals:
        line_obj = arr.get("line", {}) if isinstance(arr.get("line"), dict) else {}
        arrival_raw = arr.get("time", "")
        arrival_utc = _parse_arrival_utc(arrival_raw)
        if arrival_utc is None:
            continue
        rows.append({
            "collected_at":       collected_at,
            "stop_code":          stop_code,
            "stop_name":          stop_name,
            "line_code":          line_obj.get("codLine", ""),
            "line_name":          line_obj.get("shortDescription", ""),
            "destination":        arr.get("destination", ""),
            "arrival_time":       arrival_utc,
            "minutes_to_arrival": _minutes_until(arrival_raw, collected_at),
            "poll_index":         poll_index,
        })
    return rows


# ---------------------------------------------------------------------------
# One collection round (all Metro stops in parallel)
# ---------------------------------------------------------------------------

def collect_round(
    stops: dict,
    session: requests.Session,
    poll_index: int,
) -> list[dict]:
    collected_at = datetime.now(timezone.utc)
    all_rows: list[dict] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(_poll_stop, code, info["name"], session, collected_at, poll_index): code
            for code, info in stops.items()
        }
        n_empty = 0
        for future in as_completed(futures):
            rows = future.result()
            if rows:
                all_rows.extend(rows)
            else:
                n_empty += 1

    logging.info(
        "  Poll %d/%d: %d records from %d stops (%d no data)",
        poll_index + 1, N_POLLS, len(all_rows), len(stops) - n_empty, n_empty,
    )
    return all_rows


# ---------------------------------------------------------------------------
# Parquet serialisation
# ---------------------------------------------------------------------------

def to_parquet_bytes(rows: list[dict], job_start: datetime, hour_start: datetime) -> bytes:
    arrays = {field.name: [] for field in SCHEMA}
    for row in rows:
        for field in SCHEMA:
            arrays[field.name].append(row.get(field.name))

    table = pa.table(
        {name: pa.array(vals, type=SCHEMA.field(name).type)
         for name, vals in arrays.items()},
        schema=SCHEMA,
    )

    # File-level metadata: lets downstream users verify actual vs. scheduled window.
    # Readable without loading rows: pq.read_schema("file.parquet").metadata
    file_meta = {
        b"actual_job_start_utc": job_start.isoformat().encode(),
        b"scheduled_slot_utc":   hour_start.isoformat().encode(),
        b"n_rows":               str(len(rows)).encode(),
        b"poll_interval_s":      str(POLL_INTERVAL_S).encode(),
        b"n_polls_target":       str(N_POLLS).encode(),
    }
    existing_meta = table.schema.metadata or {}
    table = table.replace_schema_metadata({**existing_meta, **file_meta})

    buf = BytesIO()
    pq.write_table(
        table,
        buf,
        compression="zstd",
        compression_level=9,
        use_dictionary=True,      # dictionary-encode low-cardinality strings
        write_statistics=True,
        row_group_size=50_000,
    )
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Hugging Face upload — with exponential-backoff retry
# ---------------------------------------------------------------------------

HF_PUSH_RETRIES    = 3
HF_PUSH_BACKOFF_S  = 30   # wait 30s, then 60s before final attempt

def push_to_hf(parquet_bytes: bytes, hour_dt: datetime, n_rows: int) -> None:
    token = os.environ.get("HF_TOKEN")
    if not token:
        logging.error("HF_TOKEN env var not set — cannot push to Hugging Face")
        sys.exit(1)

    api = HfApi(token=token)
    date_str = hour_dt.strftime("%Y-%m-%d")
    hour_str = hour_dt.strftime("%H")
    path_in_repo = f"metro/{hour_dt.strftime('%Y-%m')}/{date_str}_{hour_str}00.parquet"
    commit_msg   = f"data: metro {date_str} {hour_str}:00 UTC ({n_rows:,} rows)"

    logging.info("Pushing to HF: %s/%s", HF_REPO, path_in_repo)
    for attempt in range(HF_PUSH_RETRIES):
        try:
            api.upload_file(
                path_or_fileobj=parquet_bytes,
                path_in_repo=path_in_repo,
                repo_id=HF_REPO,
                repo_type=HF_REPO_TYPE,
                commit_message=commit_msg,
            )
            logging.info("Pushed successfully (%d KB)", len(parquet_bytes) // 1024)
            return
        except Exception as exc:
            if attempt < HF_PUSH_RETRIES - 1:
                wait = HF_PUSH_BACKOFF_S * (2 ** attempt)
                logging.warning(
                    "HF push failed (attempt %d/%d): %s — retrying in %ds",
                    attempt + 1, HF_PUSH_RETRIES, exc, wait,
                )
                time.sleep(wait)
            else:
                logging.error(
                    "HF push failed after %d attempts: %s — checkpoint data may be lost",
                    HF_PUSH_RETRIES, exc,
                )
                raise


# ---------------------------------------------------------------------------
# Main loop — checkpoint push every CHECKPOINT_EVERY polls
# ---------------------------------------------------------------------------

def main() -> None:
    setup_logging()
    job_start  = datetime.now(timezone.utc)
    hour_start = job_start.replace(minute=0, second=0, microsecond=0)

    delay_min = int((job_start - hour_start).total_seconds() / 60)
    if delay_min > 5:
        logging.warning(
            "Job started %d min after scheduled slot — file label %s may not match "
            "actual collection window (actual start %s UTC)",
            delay_min,
            hour_start.strftime("%Y-%m-%d %H:00"),
            job_start.strftime("%H:%M"),
        )

    logging.info("=" * 60)
    logging.info(
        "Metro hourly collection — slot %s UTC (actual start %s UTC)",
        hour_start.strftime("%Y-%m-%d %H:00"),
        job_start.strftime("%H:%M"),
    )
    logging.info(
        "Plan: %d polls × %d-sec interval — checkpoint every %d polls (max loss: %d min)",
        N_POLLS, POLL_INTERVAL_S, CHECKPOINT_EVERY, CHECKPOINT_EVERY,
    )
    logging.info("=" * 60)

    stops = load_metro_stops()
    all_rows: list[dict] = []

    with requests.Session() as session:
        session.headers.update(HEADERS)
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS
        )
        session.mount("https://", adapter)

        for poll_index in range(N_POLLS):
            round_start = time.monotonic()
            rows = collect_round(stops, session, poll_index)
            all_rows.extend(rows)

            is_last       = poll_index == N_POLLS - 1
            is_checkpoint = (poll_index + 1) % CHECKPOINT_EVERY == 0

            if (is_checkpoint or is_last) and all_rows:
                logging.info(
                    "  Checkpoint at poll %d/%d — serialising %d rows → HF...",
                    poll_index + 1, N_POLLS, len(all_rows),
                )
                parquet_bytes = to_parquet_bytes(all_rows, job_start=job_start, hour_start=hour_start)
                push_to_hf(parquet_bytes, hour_start, len(all_rows))

            if not is_last:
                elapsed = time.monotonic() - round_start
                wait = max(0.0, POLL_INTERVAL_S - elapsed)
                logging.info("  Waiting %.0f s until next poll...", wait)
                time.sleep(wait)

    logging.info("Done. %d total rows from %d polls.", len(all_rows), N_POLLS)


if __name__ == "__main__":
    main()
