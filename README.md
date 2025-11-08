# Bridge Monitoring — PySpark Structured Streaming (Bronze→Silver→Gold)

This repo implements an end‑to‑end streaming ETL that simulates IoT sensors (temperature, vibration, tilt) and computes **1‑minute window** metrics with **2‑minute watermark**, using **file‑based streaming** sources. Includes data‑quality checks, stream‑static join, stream‑stream joins, checkpoints, and per‑batch metrics.

## Repo Structure
```
bridge-monitoring-pyspark/
├─ data_generator.py
├─ bronze_ingest.py
├─ silver_enrichment.py
├─ gold_aggregation.py
├─ notebooks/
│  └─ demo.ipynb
├─ metadata/
│  └─ bridges.csv
├─ scripts/
│  └─ run_all.sh
├─ checkpoints/ (runtime)
├─ monitoring/  (runtime)
├─ bronze/      (runtime)
├─ silver/      (runtime)
└─ gold/        (runtime)
```

## Prerequisites
- Python 3.8+
- Apache Spark 3.x (PySpark) with Structured Streaming
- Optional: Run on **Google Colab** with Drive mounted

## Quick Start (Local or Colab Terminal)
Set a base folder path where runtime data will be written:
```bash
export BASE="/content/drive/MyDrive/bridge-monitoring"   # or any local path
```

### 1) Generate Streaming Data
```bash
python data_generator.py --base "$BASE" --rate-per-5s 3 --deterministic --duration 300
```

### 2) Start Bronze Ingestion
```bash
python bronze_ingest.py --base "$BASE" --run-seconds 0   # 0 = run until Ctrl+C
```

### 3) Start Silver Enrichment (new terminal)
```bash
python silver_enrichment.py --base "$BASE" --run-seconds 0
```

### 4) Start Gold Aggregations (new terminal)
```bash
python gold_aggregation.py --base "$BASE" --run-seconds 0
```

### 5) Validate Outputs (from PySpark shell or notebook)
```python
spark.read.parquet(f"{BASE}/gold/bridge_metrics").orderBy('window_start','bridge_id').show(20, truncate=False)

spark.read.parquet(f"{BASE}/monitoring/metrics").groupBy('layer','sensor','status').sum('rows').show()
```

## Design Choices
- **Medallion:** Immutable Bronze; validated/enriched Silver; analytics‑ready Gold.
- **Watermark:** `2 minutes` to tolerate late events while bounding state.
- **Windows:** `1 minute` tumbling, aligned across sensors for joins.
- **DQ checks:** Non‑null timestamp/value, sensor‑range rules, metadata join required; rejected records routed to `bronze/` and `silver/` rejected sinks.
- **Checkpoints:** Per‑sink directories under `checkpoints/` to ensure exactly‑once semantics and recovery.
- **Monitoring:** `foreachBatch` writes per‑batch counts to `monitoring/metrics` (Parquet).

## Troubleshooting
- **Schema must be specified**: Streaming Parquet reads require `.schema(...)` if directories may be empty.
- **Gateway exited**: Ensure Java installed and `JAVA_HOME` set before creating Spark session (Colab: OpenJDK‑11).
- **Re‑runs**: Stop previous streaming queries before restarting scripts.
