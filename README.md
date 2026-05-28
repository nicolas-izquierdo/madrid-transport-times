# madrid-transport-times

**1-minute resolution real-time arrival data for all Metro de Madrid stations.**  
The first public continuous archive of Metro de Madrid operational performance data.

Collected every minute via the CRTM public API. Permanent archive on Hugging Face:  
→ **[huggingface.co/datasets/nicolas-izquierdo/madrid-transport-times](https://huggingface.co/datasets/nicolas-izquierdo/madrid-transport-times)**

---

## What this is

Every minute, the script polls the CRTM `GetStopsTimes` API for all 293 Metro de Madrid
stations — the same real-time GPS-tracked predictions shown on station countdown displays.
One hourly Parquet file is written per hour (55 polls × ~1,750 rows ≈ 96,000 rows/hour).

**Collection started:** May 2026  
**Polling interval:** 60 seconds (matches [subwaydata.nyc](https://subwaydata.nyc) standard)  
**Lines:** L1–L12, ML1–ML3 (all 293 stations)  
**Storage:** Hugging Face Datasets — `metro/YYYY-MM/YYYY-MM-DD_HH00.parquet`

---

## Repository contents

```
collect_metro_hourly.py       — collection + HF push script
stops_cache.json              — 293 Metro station codes (refreshed weekly)
.github/workflows/
  collect_metro.yml           — hourly GitHub Actions job (cron :02 * * * *)
hf_dataset_card.md            — Hugging Face dataset README
```

---

## Schema

| Column | Type | Description |
|---|---|---|
| `collected_at` | timestamp UTC | When the API was polled |
| `stop_code` | string | CRTM stop ID, e.g. `4_38` |
| `stop_name` | string | Station name, e.g. `NOVICIADO` |
| `line_code` | string | CRTM internal line code |
| `line_name` | string | Line number: `1`, `2` … `12` |
| `destination` | string | Terminal destination |
| `arrival_time` | timestamp UTC | Predicted arrival |
| `minutes_to_arrival` | int16 | Minutes until arrival at poll time |
| `poll_index` | int8 | Which 1-min round (0–54) within the hourly file |

---

## Loading the data

```python
# Python — one hour
import pandas as pd
df = pd.read_parquet(
    "hf://datasets/nicolas-izquierdo/madrid-transport-times/metro/2026-06/2026-06-01_0800.parquet"
)

# Python — full month via DuckDB
import duckdb
df = duckdb.sql("""
    SELECT * FROM read_parquet(
        'hf://datasets/nicolas-izquierdo/madrid-transport-times/metro/2026-06/*.parquet'
    )
""").df()
```

```r
# R — one file
library(arrow)
df <- read_parquet("https://huggingface.co/datasets/nicolas-izquierdo/madrid-transport-times/resolve/main/metro/2026-06/2026-06-01_0800.parquet")
```

---

## Inspired by

- [subwaydata.nyc](https://github.com/jamespfennell/subwaydata.nyc) — NYC subway continuous archive
- [mta-bus-archive](https://github.com/Bus-Data-NYC/mta-bus-archive) — NYC bus GTFS-RT archive

---

## Licence

Code: MIT. Data: CC BY 4.0.  
Cite as: Izquierdo, N. (2026). *Madrid Metro Real-Time Arrivals*. UC3M.  
[huggingface.co/datasets/nicolas-izquierdo/madrid-transport-times](https://huggingface.co/datasets/nicolas-izquierdo/madrid-transport-times)
