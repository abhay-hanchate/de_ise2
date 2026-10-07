# Smart City NYC: Urban Data Processing using Spark DataFrames

Case Study 2 · Unit 5.2 DataFrames · Tools: PySpark, Pandas

| Name | Roll No |
|---|---|
| Varun Murugan | 124A8118 |
| Rankin Yanu | 124A8124 |
| Navin Nadar | 225A8134 |
| Abhay Hanchate | 225A8129 |

A smart city collects data from vehicles, citizens, sensors and road networks. We load six open New York City
data sources into Spark DataFrames, clean and combine them, and answer 10 questions a city operations team
would ask. Then we optimize the processing and measure the speed-ups.

## Data sources

| Domain | Data | Format / source |
|---|---|---|
| 🚕 Mobility | Yellow taxi trips, Jan 2024 (~3M rows) | Parquet, NYC TLC |
| 🚕 Mobility | Taxi zone lookup | CSV, NYC TLC |
| 🏛️ Citizen services | 311 service requests, Jan 2024 (~250k rows) | CSV pages from the NYC Open Data (Socrata) API |
| 🌫️ Environment | Hourly air quality (PM2.5, NO₂, CO) | JSON, Open-Meteo Air Quality API |
| 🌦️ Environment | Hourly weather (temperature, rain) | JSON, Open-Meteo Weather API |
| 🚦 Live traffic | Real-time road speeds | JSON, NYC DOT live API (fresh on every run) |

## Run it (Google Colab)

1. Open the notebook in Colab:
   https://colab.research.google.com/github/abhay-hanchate/de_ise2/blob/claude/beautiful-keller-ee4vsx/notebooks/Spark_DataFrames_CaseStudy.ipynb
   (If the repo is private, use *File → Upload notebook* in Colab and choose `notebooks/Spark_DataFrames_CaseStudy.ipynb`.)
2. *Runtime → Run all*. The notebook installs PySpark if needed and downloads every dataset itself.
   A full run takes about 5 minutes, most of it the 311 download.
3. Take the screenshots where the notebook shows a 📸 marker.
4. The last cell downloads `case_study_outputs.zip` with all charts and result tables.

## What the notebook covers

| Step | Content |
|---|---|
| 1. Setup | Spark session (`local[*]`), versions, Spark UI |
| 2. Load | Parquet, CSV with explicit schemas, a folder of CSV pages, nested API JSON (`explode` + `arrays_zip`), live JSON |
| 3. Distributed processing | Partitions, rows per partition, `repartition`, shuffle stages |
| 4. Transformations and actions | Lazy evaluation, narrow vs wide, data-quality reports, dedup, newest-reading-per-sensor window |
| 4. Queries | Q1 hourly demand (+ Spark SQL) · Q2 mobility hubs (join) · Q3 weekly heatmap (pivot) · Q4 top complaints · Q5 agency resolution time (percentile) · Q6 top complaints per borough (window) · Q7 taxi traffic vs air pollution (join + corr) · Q8 temperature vs heating complaints · Q9 live road speeds · Q10 borough scorecard from 4 sources |
| 5. Optimization | O1 cache, O2 broadcast join, O3 shuffle partitions, O4 AQE, O5 CSV vs Parquet (median of 3 runs) |
| 6. Capture results | Cleaned Parquet output, result CSVs, chart PNGs |
| 7. Analysis | Speed-up chart, Pandas vs Spark at 0.1M–3M rows, scaling with 1/2/all cores, smart city insights, conclusions |

## Layout

```
notebooks/Spark_DataFrames_CaseStudy.ipynb   the whole case study
data/raw/                                    taxi, zones and weather files (the notebook downloads the rest)
PLAN.md                                      project plan
```
