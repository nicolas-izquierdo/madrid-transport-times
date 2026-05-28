# madrid-transport-times

Real-time arrival prediction data for all public transport modes in the Madrid
metropolitan area, collected continuously from the CRTM (Consorcio Regional de
Transportes de Madrid) API.

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

## Licence

Data collected from a public API. Code: MIT.  
If you use this dataset in published research, please cite the source project.
