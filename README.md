# Data Processing using Spark DataFrames: NYC Taxi Case Study

Case Study 2 · Unit 5.2 DataFrames · Tools: PySpark, Pandas

| Name | Roll No |
|---|---|
| Varun Murugan | 124A8118 |
| Rankin Yanu | 124A8124 |
| Navin Nadar | 225A8134 |
| Abhay Hanchate | 225A8129 |

We process about 3 million NYC taxi trips (January 2024) with Spark DataFrames. The notebook:
- joins the trips with a zone lookup (CSV) and hourly weather from a REST API (JSON);
- answers 7 business questions;
- measures 5 performance optimizations;
- compares Spark with Pandas and with different numbers of cores.

## Run it (Google Colab)

1. Open the notebook in Colab:
   https://colab.research.google.com/github/abhay-hanchate/de_ise2/blob/claude/beautiful-keller-ee4vsx/notebooks/Spark_DataFrames_CaseStudy.ipynb
   (If the repo is private, use *File → Upload notebook* in Colab and choose `notebooks/Spark_DataFrames_CaseStudy.ipynb`.)
2. *Runtime → Run all*. The notebook installs PySpark if needed and downloads all 3 datasets itself.
   A full run takes a few minutes.
3. Take the screenshots where the notebook shows a 📸 marker.
4. The last cell downloads `case_study_outputs.zip` with all charts and result tables.

To run locally instead: `pip install -r requirements.txt` (needs Java 17), then open the notebook in Jupyter.
The data files are already in `data/raw/`.

## What the notebook covers

| Step | Content |
|---|---|
| 1. Setup | Spark session (`local[*]`), versions, Spark UI |
| 2. Load | Parquet trips, CSV zones (explicit schema), nested JSON weather from the Open-Meteo API (`explode` + `arrays_zip`) |
| 3. Distributed processing | Partitions, rows per partition, `repartition`, shuffle stages |
| 4. Transformations and actions | Lazy evaluation demo, narrow vs wide, data-quality report, Q1–Q7: `groupBy`, Spark SQL, joins, window `dense_rank`, `pivot`, weather join |
| 5. Optimization | O1 cache, O2 broadcast join, O3 shuffle partitions, O4 AQE, O5 CSV vs Parquet (median of 3 runs) |
| 6. Capture results | Cleaned Parquet output, result CSVs, chart PNGs |
| 7. Analysis | Speed-up chart, Pandas vs Spark at 0.1M–3M rows, scaling with 1/2/all cores, conclusions |

## Layout

```
notebooks/Spark_DataFrames_CaseStudy.ipynb   the whole case study
data/raw/                                    input data (trips parquet, zones csv, weather json)
PLAN.md                                      project plan
```
