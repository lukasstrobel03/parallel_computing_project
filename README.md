# ComPar 26/27 – Project 1: Large-Scale Analytics with Spark

Performance study of a PySpark pipeline on the NYC TLC Yellow Taxi Trip Records.
Tasks: M1 (cleaning), M2 (temporal demand), M3 (zone statistics),
O1 (fare anomalies), O2 (origin-destination flows), O3 (demand features).

> Items in `[...]` must be filled in before submission.

## 1. Repository layout

```
.
├── docker-compose.yml     # Spark master, worker, Jupyter
├── Dockerfile             # Jupyter image (PySpark client)
├── notebooks/             # mounted at /project/workspace
├── src/                   # helper code, e.g. spark_profiler.py (mounted at /project/src)
├── data/                  # NOT in the repository, see section 2 (mounted at /project/data)
├── output/                # results and plans (mounted at /project/output)
└── report/                # LaTeX report
```

## 2. Data

The dataset is not part of the repository. Download it locally into `data/`.

- Source: NYC TLC Trip Record Data, Yellow Taxi, Parquet
  `https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page`
- File pattern: `yellow_tripdata_YYYY-MM.parquet`, one file per month
- Files used: `[yellow_tripdata_2025-01.parquet ... yellow_tripdata_2025-12.parquet]`
  (`[+ 2026 files if used]`)
- Download date: `[YYYY-MM-DD]`
- Exact download URL pattern: `[e.g. https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet]`
- Zone lookup: `taxi_zone_lookup.csv` (265 rows), `[download URL]`

Expected layout:

```
data/
├── taxi_zone_lookup.csv
├── tripdata_2025/yellow_tripdata_2025-01.parquet ... yellow_tripdata_2025-12.parquet
└── tripdata_2026/yellow_tripdata_2026-01.parquet ...   (optional)
```

Inside the containers the data is available at `/project/data`.

## 3. Environment

| Item | Value |
|---|---|
| Spark version | `[run spark.version in the notebook]` |
| Python version | `[run python --version in the Jupyter container]` |
| Docker image (master, worker) | `spark:python3` |
| Cluster mode | Spark standalone (`spark://spark-master:7077`) |
| Workers | 1 |
| Cores per worker | 2 (`--cores 2`) |
| Memory per worker | 2 GB (`--memory 2G`) |
| Executors | `[number and cores per executor from the Executors tab of the Spark UI]` |
| Driver | Jupyter container, `local` Python process, `[driver memory]` |
| Host machine | `[CPU model, cores, RAM, OS]` |
| Docker resource limits | `[CPUs / memory assigned to Docker]` |

## 4. Start the cluster

Requirements: Docker with Docker Compose.

```powershell
docker compose -p parallel_computing_project up -d --build
```

Check that the three containers run and that the worker runs as `root`
(needed so that executors can write to the mounted `output/` folder):

```powershell
docker compose -p parallel_computing_project ps
docker compose -p parallel_computing_project exec spark-worker whoami
```

Web interfaces:

| Service | URL |
|---|---|
| Jupyter Lab | http://localhost:8888 (token: see `JUPYTER_TOKEN` in `docker-compose.yml`) |
| Spark Master UI | http://localhost:8080 |
| Spark Application UI | http://localhost:4040 |

Stop and remove:

```powershell
docker compose -p parallel_computing_project down
```

## 5. Run the pipeline

1. Open Jupyter Lab, then the notebook `[notebooks/<name>.ipynb]`.
2. Run **Kernel -> Restart Kernel and Run All Cells**. The cells run in this order:
   1. Setup: SparkSession, profiler import, configuration
   2. M1: ingestion, cleaning, per-year statistics
   3. M2: temporal demand, written to `output/demand_by_borough`
   4. M3: zone statistics, written to `output/zone_monthly_stats`
   5. O1, O2, O3: baseline and optimised variants
   6. Comparison cells (`profiler.compare(...)`)

Input path and scale are set in the first cells:

```python
docker_data_path = "/project/data"
year_dir = "tripdata_2025"      # "tripdata_2025" | "tripdata_2026"
scope    = "year"               # "file" | "year" | "all"
spec_file = ""                  # single file for smoke tests
```

### Equivalent `spark-submit` command

The experiments in this report ran from the notebook. The assignment asks for a
command-line equivalent; if the pipeline is also provided as a script, it is started with:

```bash
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.sql.adaptive.enabled=false \
  --conf spark.sql.autoBroadcastJoinThreshold=-1 \
  [--executor-memory 2G --total-executor-cores 2] \
  [src/pipeline.py --input /project/data --scale year --fraction 1.0]
```

`[Replace with the real script path and parameters, or delete this block if no script exists.]`

## 6. Spark configuration

Everything not listed is at the Spark default.

| Property | Baseline | Optimised |
|---|---|---|
| `spark.sql.adaptive.enabled` | `false` | `[true / false, see report]` |
| `spark.sql.autoBroadcastJoinThreshold` | `-1` (disabled) | `-1`, explicit `F.broadcast(...)` is used |
| `spark.sql.shuffle.partitions` | `200` (default) | `[8]` |
| `[any other, e.g. spark.executor.memory]` | `[value]` | `[value]` |

Set in the notebook (`set_mode(False)` for the baseline). Check with:

```python
print(spark.version)
print(spark.conf.get("spark.sql.adaptive.enabled"))
print(spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))
print(spark.conf.get("spark.sql.shuffle.partitions"))
```

## 7. Measurement method

- Profiler: `src/spark_profiler.py` (based on the Lab 3 profiler). Each block
  `with profiler.profile("name"):` records wall time, number of jobs, stages and tasks, input/output bytes,
  shuffle read/write bytes, and memory/disk spill. The values come from the REST API of the
  Spark UI. `profiler.compare(a, b)` shows two runs with the percentage change.
- Run names used in the report: `[task-2_temporal_demand_analysis, task-3_zone_stats, O1-fare-anomaly-baseline,
  O2-origin-destination-baseline, o3_demand_forecasting_features_before / _after, ...]`
- Execution plans are saved as `output/plan_<name>.txt` (`explain(mode="formatted")`).
- One change per measurement. Each run is repeated `[N]` times, the table reports `[median / second run]`.
  A warm-up run precedes the measured runs to reduce cold file cache effects.
- Spark UI screenshots in the report come from `http://localhost:4040`. The UI data is lost when the
  session ends, so `profiler.report()` is saved to `output/baseline_report.csv`.

## 8. Results and output

| Path | Content |
|---|---|
| `output/demand_by_borough/` | M2, Parquet partitioned by `pickup_borough` |
| `output/zone_monthly_stats/` | M3 |
| `output/o3_before/`, `output/o3_after/` | O3 baseline and optimised |
| `output/plan_*.txt` | physical plans |
| `output/baseline_report.csv` | profiler results |

Correctness check: baseline and optimised output must have the same row count and the same number of flagged
buckets (`high_demand`); the notebook prints both.

## 9. DI cluster

`The pipeline was run on the normal configuration, that we used for our labs as well. We didn't change anything. So we used 3 workers with 2 cores and 2 GB of memory. [Describe how the pipeline was run on the DI cluster: nodes, cores and memory per node, spark-submit
command, input path, and where the results are. Delete this section if the cluster was not available.]`

## 11. Authors and AI usage

`[Names]`. AI assistance (Claude) was used for `[code drafts, debugging, report text]`; see the
Acknowledgements section of the report.
