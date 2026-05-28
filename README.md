# madrid-transport-times

Real-time arrival prediction data for all public transport modes in the Madrid
metropolitan area, collected continuously from the CRTM (Consorcio Regional de
Transportes de Madrid) API.

**Part of a research project on Madrid public transport performance.**  
Contact: paizquie@clio.uc3m.es

---

## What this dataset contains

Predicted vehicle arrival times, polled from the CRTM public API
(`GetStopsTimes.php`) for all stops across five modes:

| Mode | Coverage |
|---|---|
| Metro de Madrid | All lines and stations |
| Cercanías Renfe (Madrid nucleus) | All stops |
| EMT urban buses | All stops |
| Interurban buses (CRTM concessions) | All stops |
| Light rail (ML1/ML2/ML3) | All stops |

**Collection schedule:** 6× per day at 00:00, 04:00, 08:00, 12:00, 16:00, 20:00 UTC  
(01:00, 05:00/09:00, 09:00/13:00, 13:00/17:00, 17:00/21:00, 21:00/01:00 Madrid time)

**Collection started:** 2026-05

---

## File structure

```
data/
└── YYYY-MM/
    └── YYYY-MM-DD_HHMM.csv.gz    ← one file per collection run
stops_cache.json                  ← stop metadata, refreshed weekly
```

---

## CSV schema

Each row is one predicted arrival at one stop, as returned by the CRTM API.

| Column | Type | Description |
|---|---|---|
| `collected_at` | ISO 8601 UTC | Timestamp when the API was polled |
| `collected_at_local` | ISO 8601 +01/+02 | Same timestamp in Europe/Madrid time |
| `stop_code` | string | CRTM stop identifier (e.g. `8_65`) |
| `stop_name` | string | Stop name as returned by CRTM |
| `mode` | string | `metro`, `cercanias`, `emt`, `interurban`, `light_rail` |
| `line_code` | string | CRTM line code |
| `line_name` | string | Line name / number |
| `destination` | string | Terminal destination of this service |
| `arrival_time` | string | Predicted arrival time (ISO 8601 or HH:MM) |
| `minutes_to_arrival` | integer | Minutes until predicted arrival |

---

## Loading the data

### R

```r
library(tidyverse)

# Load a single run
df <- read_csv(gzcon(file("data/2026-05/2026-05-28_0800.csv.gz", "rb")))

# Load all runs in a month
files <- list.files("data/2026-05", pattern = "\\.csv\\.gz$", full.names = TRUE)
df <- map_dfr(files, \(f) read_csv(gzcon(file(f, "rb"))))

# Compute actual headway per stop/line
# (difference between consecutive collected_at timestamps for same stop+line)
headways <- df |>
  arrange(stop_code, line_code, collected_at) |>
  group_by(stop_code, line_code) |>
  mutate(actual_headway_min = as.numeric(difftime(
    collected_at, lag(collected_at), units = "mins"
  )))
```

### Python

```python
import pandas as pd
import glob

# Load all runs
files = glob.glob("data/**/*.csv.gz", recursive=True)
df = pd.concat([pd.read_csv(f, compression="gzip") for f in files])
df["collected_at"] = pd.to_datetime(df["collected_at"], utc=True)
```

---

## Deriving actual vs scheduled headways

This dataset captures *predicted* arrival times. To compute *actual headway
irregularity* relative to the scheduled GTFS timetable:

1. Download GTFS snapshots for the same period from
   [Transitland](https://www.transit.land/feeds/f-ezjm-consorcioregionaldetransportesdemadrid)
2. Match `stop_code` to GTFS `stop_id`
3. Use `tidytransit` or `gtfstools` in R to extract scheduled headways
4. Compare against observed `minutes_to_arrival` distributions

---

## Important caveats

- This dataset starts in **May 2026**. No historical depth exists before
  collection began; the CRTM API has no public historical archive.
- The API returns *predicted* arrivals (typically 0–20 min ahead), not
  confirmed actual arrival timestamps. Prediction accuracy varies by mode
  and operator.
- Night service gaps (approximately 01:00–06:00 Madrid time) will show
  sparse or empty records.
- Stop cache is refreshed weekly; network changes between refreshes may
  cause missed stops.
- CRTM API availability is not guaranteed; collection runs may fail silently
  if the API is down. Check commit history for gaps.

---

## Related resources

- [CRTM open data portal](https://datos.crtm.es)
- [datos.madrid.es — EMT datasets](https://datos.madrid.es)
- [Transitland GTFS archive](https://www.transit.land/feeds/f-ezjm-consorcioregionaldetransportesdemadrid)
- [Mobility Database — EMT Madrid](https://mobilitydatabase.org/feeds/gtfs/mdb-793)

---

## Licence

Data collected from a public API. Code: MIT.  
If you use this dataset in published research, please cite the source project.
