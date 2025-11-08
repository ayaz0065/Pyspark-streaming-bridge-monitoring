# Short Report — Bridge Monitoring Streaming Pipeline

## Overview
We implemented a PySpark Structured Streaming pipeline using the Medallion pattern (**Bronze→Silver→Gold**). Three file‑based streams simulate bridge sensors (temperature, vibration, tilt). The **Gold** layer computes 1‑minute tumbling aggregates with a **2‑minute watermark** and joins metrics across sensors per bridge and window.

## Checkpointing & Fault Tolerance
Each streaming sink writes to a dedicated directory under `checkpoints/`. On restart, Spark restores offsets and state, providing exactly‑once semantics for file sources/sinks. We also persist per‑batch metrics under `monitoring/metrics` to track throughput and rejections.

## Data Quality (QA)
- **Required fields**: `event_time`, `value`, `bridge_id` must be present.
- **Range rules**: temperature ∈ [‑40,80], vibration ≥ 0, tilt ∈ [0,90].
- **Enrichment**: stream‑static join with `metadata/bridges.csv`; unmatched records are rejected.
- **Rejected handling**: invalid records routed to `bronze/rejected` or `silver/rejected` with `reject_reason`.

## Watermark & Windows
We apply `withWatermark(event_time_ts, '2 minutes')` and 1‑minute tumbling windows. Events later than the watermark horizon are dropped. This balances late‑data acceptance with bounded state and predictable latency.

## Expected Latency
With file micro‑batches and 5‑second generator cadence, end‑to‑end latency to Gold is typically **~1–2 minutes** (window length + processing delay).

## Failure Handling
- If a process fails, relaunch the corresponding script; checkpoints resume processing.
- Use `--run-seconds` for controlled test runs; metrics and outputs verify progress.
- On schema issues, ensure the `.schema(...)` is set for streaming Parquet reads.

## Validation & Monitoring
Use the notebook or PySpark shell to:
```python
spark.read.parquet(f"{BASE}/gold/bridge_metrics").show()
spark.read.parquet(f"{BASE}/monitoring/metrics").groupBy('layer','sensor','status').sum('rows').show()
```

## Notes
- Scripts run independently; start Bronze → Silver → Gold in order.
- Generator supports `--deterministic` for reproducible tests.
