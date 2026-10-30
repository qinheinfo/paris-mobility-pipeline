# Design Decisions

## 1. Deduplicate by comparing content, not by `last_reported`

**Context:** The API is polled every minute, but most stations do not change between polls. About 80% of raw records are exact duplicates.

**Option considered:** Treat `(station_id, last_reported)` as a unique key.

**Problem found in live data:** For station 214134714, bikes changed 5 → 4 → 5 while `last_reported` stayed at 13:26:16. Using it as a dedup key would drop real rental events.

**Decision:** Keep a record only if bikes, docks, `is_renting` or `is_returning` differ from the previous record of the same station (SQL `LAG` window function).

**Result:** 41,040 bronze rows → 7,947 silver rows (80.6% removed), with no real change lost.

## 2. Keep bronze raw, clean in silver

**Decision:** The bronze table stores Kafka messages as-is (plus Kafka partition and offset). All cleaning (renaming, type casting, dedup) happens in silver.

**Why:** If a cleaning rule turns out to be wrong, silver can be rebuilt from bronze. Raw data is never lost.

## 3. Deduplicate in a batch job, not in the streaming job

**Decision:** The streaming job only appends to bronze. Deduplication runs as a separate batch job (`build_silver.py`).

**Why:**
- Comparing with the previous record requires remembering the last state of every station. In streaming, that means managing state in memory, which is more complex and harder to recover after a crash.
- A batch job is simple to rerun and idempotent (`CREATE OR REPLACE`).

**Trade-off:** Silver is not real-time; it is as fresh as the last batch run.

## 4. Store all timestamps in UTC

**Why:** Paris switches between UTC+1 and UTC+2. UTC avoids duplicated or missing hours at daylight-saving changes.

## 5. Compute time-based averages from bronze, not silver

**Why:** Silver keeps only change events, so each row no longer represents one minute. Averaging silver rows would give a 1-minute state and a 50-minute state the same weight. Bronze has one snapshot per minute, so a simple average is time-weighted.

## 6. Exclude out-of-service periods from availability metrics

**Finding:** 17 stations had zero bikes in every sample, but 15 of them had `is_renting = 0` throughout, meaning they were out of service, not in high demand.

**Decision:** Availability metrics (average bikes, empty-station rate) only use rows where `is_renting = 1`. Only 2 of the 17 stations were genuinely empty while in service.