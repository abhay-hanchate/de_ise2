# Case Study 2 — Data Processing using Spark DataFrames

**Topic:** Load a dataset, run queries and aggregations · **Syllabus unit:** 5.2 DataFrames · **Tools:** PySpark, Pandas

This plan covers all 7 practical steps, all 5 evaluation criteria and all 3 bonus items.

---

## 1. Key decisions (summary)

| Decision | Choice | Why |
|---|---|---|
| Dataset | **NYC Taxi trips (TLC)** + Taxi Zone lookup + **Open-Meteo weather API** | Real public data with millions of rows, so Spark actually matters. Gives us **CSV, JSON and an API** in one project, plus a natural join. |
| Language / engine | Python + **PySpark DataFrame API** (Spark SQL for a few queries) | Matches the syllabus (5.2 DataFrames) and the tools column. |
| Where Pandas fits | (a) baseline to compare against Spark, (b) plotting small aggregated results with `toPandas()` | Uses the second listed tool and gives a strong performance story. |
| Frontend? | **No custom web frontend.** Use Jupyter notebooks + Spark UI screenshots. Optional: a small **Streamlit** dashboard for the demo. | No criterion scores a frontend. A React/Flask app costs days and adds no marks. Streamlit takes about a day, is pure Python, and makes the presentation better. |
| Run modes | `local[1]` → `local[*]` → **Standalone cluster (Docker Compose)** → **Kubernetes (kind/minikube)** | The same job in 3+ modes covers the "compare cluster modes" and "Kubernetes" bonuses. |
| Storage formats | Raw CSV/JSON → cleaned **Parquet** (partitioned) | Lets us show the CSV vs Parquet speed-up as an optimization. |

---

## 2. Dataset plan

| Source | Format | Size | Use |
|---|---|---|---|
| NYC Yellow Taxi trips, 3–6 months | **CSV** (TLC Parquet converted once to CSV for the raw landing zone) | ~3M rows/month → **10–20M rows, ~2–4 GB** | Main fact table |
| Taxi Zone Lookup | **CSV** (265 rows) | tiny | Dimension table, used to show a **broadcast join** |
| Open-Meteo Historical Weather API (free, no key) | **JSON over HTTP** | 1 row/hour for NYC | Enrichment: "does rain change trips/tips?" Covers the **real-time API bonus** |
| *(stretch)* Open-Meteo current/forecast API polled every N minutes | JSON | small | **Structured Streaming** demo (file-source stream) |

We will also keep a **small sample** (~100k rows) for quick development and the Pandas comparison.

---

## 3. Repository structure

```
de_ise2/
├── README.md                  # how to set up & run everything
├── PLAN.md                    # this file
├── requirements.txt           # pyspark, pandas, pyarrow, requests, matplotlib, streamlit
├── .gitignore                 # data/, spark-warehouse/, *.parquet, etc.
├── config/
│   └── settings.yaml          # paths, months to download, spark confs
├── scripts/
│   ├── download_data.py       # fetch TLC files + zone CSV
│   └── fetch_weather.py       # call Open-Meteo API → JSON
├── src/
│   ├── spark_session.py       # one builder, configurable master/confs
│   ├── schemas.py             # explicit StructType schemas (no inferSchema)
│   ├── ingest.py              # read CSV / JSON / API
│   ├── clean.py               # data-quality rules, derived columns
│   ├── queries.py             # the analytical queries (DataFrame API + SQL)
│   └── pipeline.py            # entry point: ingest → clean → query → write
├── benchmarks/
│   ├── run_benchmarks.py      # each optimization: baseline vs optimized
│   ├── pandas_vs_spark.py     # same query at growing data sizes
│   ├── cluster_modes.sh       # same job across local / standalone / k8s
│   └── results/*.csv          # raw timings (committed)
├── notebooks/
│   ├── 01_setup_and_ingest.ipynb
│   ├── 02_transformations_actions.ipynb
│   ├── 03_queries_aggregations.ipynb
│   ├── 04_optimization.ipynb
│   └── 05_performance_analysis.ipynb
├── dashboard/app.py           # optional Streamlit app (reads results only)
├── docker/
│   ├── Dockerfile             # spark + our code
│   └── docker-compose.yml     # 1 master + 2 workers (+ Jupyter)
├── k8s/
│   ├── spark-rbac.yaml
│   └── submit_k8s.sh          # spark-submit --master k8s://...
└── docs/
    ├── screenshots/           # Spark UI, outputs, dashboards
    ├── architecture.png
    └── RollNo_Name_CaseStudy.pdf   # final report (and slides)
```

---

## 4. The 7 practical steps

### Step 1 — Setup environment
- Python 3.11, Java 17, PySpark 3.5.x (pinned in `requirements.txt`), Pandas, PyArrow.
- `spark_session.py` reads the master URL and confs from env/config, so the **same code** runs in every mode.
- Docker Compose standalone cluster: `spark-master`, `spark-worker-1`, `spark-worker-2`, Jupyter.
- Screenshot evidence: versions, `docker ps`, Spark Master UI (:8080) showing workers.

### Step 2 — Load dataset (CSV / JSON / API)
- CSV trips and zones read with **explicit schemas**. We also time `inferSchema=True` against the explicit schema as a small optimization.
- Weather: `fetch_weather.py` calls the API and saves raw JSON. Spark reads it with `spark.read.json` and flattens the nested arrays with `arrays_zip` + `explode`.
- Show `printSchema()`, `count()`, `show()`, `describe()`.

### Step 3 — Distributed processing
- Show partitions: `df.rdd.getNumPartitions()` and record counts per partition (`spark_partition_id()`).
- Show the job split into stages and tasks across executors in the Spark UI (:4040).
- Run on the multi-worker cluster and screenshot the Executors tab.

### Step 4 — Transformations and actions
Label every operation as **narrow vs wide** and **transformation vs action**, and show **lazy evaluation** (nothing runs until an action; prove it with timings and the UI).

Cleaning and derived columns (`clean.py`):
- Remove bad rows: negative or zero fares, zero distance, trips with drop-off before pick-up, passenger_count outliers, dates outside the selected months.
- Add columns: `trip_duration_min`, `speed_mph`, `pickup_hour`, `day_of_week`, `is_weekend`, `tip_pct`, `is_airport`.
- Deduplicate, handle nulls (`fillna`/`dropna`), cast types.
- Keep a **data-quality report** of rows dropped per rule.

Analytical queries (`queries.py`), each written with the DataFrame API and a few repeated in Spark SQL:

| # | Question | Spark features shown |
|---|---|---|
| Q1 | Trips and revenue by hour of day | `groupBy`, `agg`, `orderBy` |
| Q2 | Top 10 pickup zones by revenue | **join** with zones, `limit` |
| Q3 | Average fare per mile by borough | join + multi-column agg |
| Q4 | Tip % by payment type and time of day | `when/otherwise`, conditional agg |
| Q5 | Day-of-week × hour heatmap | `pivot` |
| Q6 | Top 3 zones per borough | **Window** `rank()` / `dense_rank()` |
| Q7 | Daily revenue, 7-day moving average, day-over-day change | Window `avg over rowsBetween`, `lag` |
| Q8 | Trip duration percentiles | `percentile_approx` / `approxQuantile` |
| Q9 | Airport vs city trips: fare, distance, tip | `filter`, `union`, compare |
| Q10 | **Weather impact**: trips and tips on rainy vs dry hours | join with the API data |
| Q11 | Same query in SQL: `createOrReplaceTempView` + `spark.sql` | DataFrame API vs SQL equivalence |
| Q12 | A simple UDF, then the same logic with built-in functions | Why UDFs are slow (feeds Step 5) |

Outputs are written as Parquet/CSV to `data/output/`. Small results go to Pandas for charts.

### Step 5 — Optimize performance (cache, partition, shuffle)
Each experiment runs a **baseline vs optimized** pair. We record timings and `explain()` plans and take a Spark UI screenshot.

| # | Optimization | Baseline → Optimized |
|---|---|---|
| O1 | **Caching** | Re-run 5 queries on the cleaned DF without vs with `cache()`/`persist(MEMORY_AND_DISK)`; show the Storage tab |
| O2 | **File format** | Query on CSV vs Parquet (columnar storage, column pruning, predicate pushdown in `explain`) |
| O3 | **Partitioned writes** | `partitionBy("year","month")` and partition pruning on a month filter |
| O4 | **Shuffle partitions** | `spark.sql.shuffle.partitions` = 200 (default) vs 8/16/64; find the best value for our data size |
| O5 | **Broadcast join** | Sort-merge join vs `broadcast(zones)`; the plan changes from `SortMergeJoin` to `BroadcastHashJoin` |
| O6 | **AQE** | `spark.sql.adaptive.enabled` false vs true (partitions merged, skew handled) |
| O7 | **repartition vs coalesce** | Before writing: show shuffle vs no shuffle |
| O8 | **Python UDF vs built-ins** | Q12 timing (optionally a Pandas UDF too) |
| O9 | **Explicit schema vs inferSchema** | Load time |

### Step 6 — Capture results
- `docs/screenshots/` named by step: `step3_executors.png`, `O5_broadcast_plan.png`, …
- Spark UI: Jobs, Stages (shuffle read/write), Storage (cached DF), SQL tab (physical plan), Executors.
- Query outputs: `show()` output plus a matplotlib chart per query.
- All timings saved to `benchmarks/results/*.csv`.

### Step 7 — Analyze performance
Method: warm-up run, then **3 timed runs, report the median**. Force full execution with `df.write.format("noop")` so I/O does not distort timings. Record machine specs and the Spark config with every result.

Analyses:
1. **Optimization impact:** a bar chart of baseline vs optimized for O1–O9, with % speed-up.
2. **Pandas vs Spark:** the same aggregation at 100k / 1M / 5M / full rows. Pandas wins on small data and runs out of memory or slows down on large data. We explain the crossover point.
3. **Scalability:** `local[1]`, `local[2]`, `local[4]`, `local[*]`, standalone with 1 vs 2 workers, Kubernetes. Runtime vs cores chart.
4. Write-up: where the time goes (shuffle bytes, spill, GC from the Stages tab) and why each optimization helped or did not help.

---

## 5. Bonus items

| Bonus | How we cover it | Effort |
|---|---|---|
| **Real-time datasets / APIs** | Open-Meteo API ingestion (required path). Stretch: a poller writes JSON files every minute and **Spark Structured Streaming** reads the folder with a windowed aggregation | Low (API), Medium (streaming) |
| **Kubernetes deployment** | `kind` or `minikube` cluster. Build our Docker image and run `spark-submit --master k8s://… --deploy-mode cluster`, with driver and executor pods. Screenshots: `kubectl get pods`, logs, Spark UI via port-forward | Medium–High, so do it last |
| **Compare cluster modes** | `cluster_modes.sh` runs the same `pipeline.py` on local[1], local[*], Standalone (Docker, 1 and 2 workers) and K8s. Results table and chart | Medium |

---

## 6. Frontend: the decision

**No full frontend (React, Flask or similar).** The case study is graded on Spark concepts, implementation, optimization, documentation and presentation. A web app doesn't score in any of them.

What we use instead:
1. **Jupyter notebooks** (primary). They read top to bottom like a lab record, keep the outputs inline, and are easy to show live.
2. **Spark UI**, built in. It is the best proof of distributed processing.
3. **Optional Streamlit dashboard** (`dashboard/app.py`, about 1 day). It only reads the precomputed outputs and benchmark CSVs, and has 3 pages:
   - *Insights*: query results and charts (hourly demand, top zones, heatmap, weather impact).
   - *Performance*: the optimization speed-up chart, Pandas vs Spark, the cluster-mode comparison.
   - *Pipeline*: architecture diagram and data-quality report.

   Build it only after the core work is done. It adds polish for the presentation.

---

## 7. Mapping to evaluation criteria

| Criterion | What demonstrates it |
|---|---|
| **Concept clarity** | Notebook 02: lazy evaluation, narrow vs wide, transformation vs action, DAG/stages, `explain()`; architecture diagram |
| **Implementation quality** | Modular `src/`, explicit schemas, config-driven Spark session, data-quality rules, 12 varied queries, a reproducible README |
| **Performance optimization** | O1–O9 with measured before/after numbers and physical plans |
| **Documentation** | README (setup + run), comments, report PDF with screenshots, results tables |
| **Presentation** | 10–12 slides + live demo (notebook or Streamlit + Spark UI) |

---

## 8. Execution phases

| Phase | Work | Output |
|---|---|---|
| 0 | Repo skeleton, requirements, `.gitignore`, Spark session helper, download scripts | Runs `spark.range(10).show()` |
| 1 | Ingest CSV + JSON + API, schemas, cleaning, data-quality report | Clean Parquet in `data/processed/` |
| 2 | Queries Q1–Q12 + charts | Notebook 03, outputs |
| 3 | Optimizations O1–O9 + benchmark harness | `benchmarks/results/*.csv`, notebook 04 |
| 4 | Pandas vs Spark + Docker standalone cluster + cluster-mode comparison | Notebook 05, charts |
| 5 | **Bonus:** Kubernetes deployment, Structured Streaming | `k8s/`, screenshots |
| 6 | *(optional)* Streamlit dashboard | `dashboard/app.py` |
| 7 | Report + slides, file named `RollNo_Name_CaseStudy` | `docs/` |

Phases 0–3 are the **must-have core**. Phases 4–5 earn the bonus marks. Phase 6 is polish.

**Suggested split for a team of 3–4 people:** (A) ingest + cleaning + queries, (B) optimizations + benchmarks + Pandas comparison, (C) Docker/K8s + cluster modes, (D) dashboard + report + slides. Everyone captures screenshots for their own part.

---

## 9. Risks and mitigations

| Risk | Mitigation |
|---|---|
| Laptop RAM too small for the full data | Make the number of months configurable. Develop on the 100k sample and run the full benchmarks once. |
| Kubernetes setup eats time | Do it last. Standalone Docker already counts as a cluster-mode comparison. |
| Timings vary from run to run | Warm-up + median of 3, `noop` sink, same machine, record the config |
| API rate limits or downtime | Cache the raw JSON in `data/raw/`; the pipeline reads from the cache |
| Plagiarism check | Our own dataset combination, our own questions, our own measured numbers and write-up |

---

## 10. Information still needed from the team
- Submission **deadline**, and whether this is **individual or group** work. This sets how much bonus work is realistic.
- **Roll number(s) and name(s)** for the `RollNo_Name_CaseStudy` file name.
- Machine specs (RAM/cores), so we can size the dataset.
- Whether the evaluator expects a specific report format (PDF/DOCX/notebook).
