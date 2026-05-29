---
license: cc-by-4.0
task_categories:
  - other
language:
  - es
tags:
  - transport
  - madrid
  - metro
  - real-time
  - transit
  - gtfs
  - urban-mobility
  - spain
pretty_name: Madrid Metro Real-Time Arrivals
size_categories:
  - 10M<n<100M
---

# Madrid Metro Real-Time Arrivals

Continuously collected real-time arrival predictions for all Metro de Madrid
stations, polled every minute from the CRTM public API.

**This is the first public historical archive of Metro de Madrid real-time
operational data.** No equivalent dataset exists for any European metro system.

---

## What this dataset is

Each row is one predicted vehicle arrival at one station, as returned by the
CRTM `GetStopsTimes` API at the moment of polling. This is the same
information shown on the countdown displays inside Metro stations — GPS-tracked,
real-time predictions, not nominal schedule times.

**Modes covered:** Metro de Madrid only (all 13 lines, ~293 stations).  
**Collection started:** May 2026.  
**Update frequency:** Every 1 minute — 55 polls per hourly job (24 hourly Parquet files per day).  
**Collection method:** GitHub Actions polling the CRTM public REST API.

---

## File structure

```
metro/
└── YYYY-MM/
    └── YYYY-MM-DD_HH00.parquet    ← one file per hour, UTC
```

Each Parquet file contains approximately 55 poll rounds × ~1,750 rows = ~96,000 rows.
Compression: Zstandard, ~1–2 MB per file.

---

## Schema

| Column | Type | Description |
|---|---|---|
| `collected_at` | timestamp (UTC) | When the API was polled |
| `stop_code` | string | CRTM stop ID, e.g. `4_38` |
| `stop_name` | string | Station name, e.g. `NOVICIADO` |
| `line_code` | string | CRTM internal line code |
| `line_name` | string | Line number, e.g. `2`, `6`, `10` |
| `destination` | string | Terminal destination of this service |
| `arrival_time` | timestamp (UTC) | Predicted arrival time |
| `minutes_to_arrival` | int16 | Minutes until arrival at poll time |
| `poll_index` | int8 | Which 1-min round (0–54) within the hourly file |

---

## Loading the data

### Python

```python
import pandas as pd

# One hour
df = pd.read_parquet(
    "hf://datasets/nicolas-izquierdo/madrid-transport-times/metro/2026-05/2026-05-29_0800.parquet"
)

# Full month — lazy load with DuckDB
import duckdb
conn = duckdb.connect()
df = conn.execute("""
    SELECT * FROM read_parquet(
        'hf://datasets/nicolas-izquierdo/madrid-transport-times/metro/2026-06/*.parquet'
    )
    WHERE line_name = '6'
""").df()
```

### R

```r
library(arrow)
library(duckdb)
library(dplyr)

# One file
df <- read_parquet(
  "https://huggingface.co/datasets/nicolas-izquierdo/madrid-transport-times/resolve/main/metro/2026-05/2026-05-29_0800.parquet"
)

# Compute headway irregularity by station (across multiple days)
# Load several files, group by stop_code + line_name + hour-of-day,
# compute sd(minutes_to_arrival) as the irregularity measure.
```

---

## What you can measure with this data

- **Headway irregularity**: standard deviation of inter-arrival times by station/line/hour
- **Service frequency vs. schedule**: compare observed headways to GTFS scheduled headways
- **Night service patterns**: which stations/lines have earliest/latest service
- **Spatial equity**: map irregularity against neighbourhood socioeconomic variables
- **Temporal trends**: service quality changes over months/years

---

## Limitations

- API returns **predicted** arrivals, not confirmed actual arrivals.
  For trains ≤5 min away predictions are highly accurate; at 15–30 min less so.
- Night gaps (~01:00–06:00 Madrid time) produce sparse records.
- Collection began May 2026; no historical depth before this date exists anywhere.
- CRTM API is a public but undocumented endpoint; availability is not guaranteed.
  Check the commit history for collection gaps.

---

## Citation

If you use this dataset in published research, please cite:

```
@dataset{izquierdo2026madrid,
  title   = {Madrid Metro Real-Time Arrivals},
  author  = {Izquierdo, Nicolás},
  year    = {2026},
  url     = {https://huggingface.co/datasets/nicolas-izquierdo/madrid-transport-times},
  note    = {Continuously collected from CRTM API. UC3M.}
}
```

---

## Related resources

- [Code repository](https://github.com/nicolas-izquierdo/madrid-transport-times)
- [CRTM open data portal](https://datos.crtm.es)
- [CRTM GTFS archive on Transitland](https://www.transit.land/feeds/f-ezjm-consorcioregionaldetransportesdemadrid)
- [subwaydata.nyc](https://subwaydata.nyc) — inspiration (NYC subway equivalent)
