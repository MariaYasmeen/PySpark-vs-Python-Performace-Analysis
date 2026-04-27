# Energy Access Survey | Spark ETL + Benchmarking + Visual Analytics  

Interactive, reproducible analysis of the **Pakistan Energy Access Household Panel Survey** using **PySpark** (DataFrame + Spark SQL), with a lightweight benchmarking section and a set of **cleaned, non-null visual analyses exported as JPGs**.

---

## Table of Contents
- [Project Overview](#project-overview)
- [Key Outputs](#key-outputs)
- [Project Structure](#project-structure)
- [How to Run](#how-to-run)
- [Data Quality Rules](#data-quality-rules)
- [Visualizations](#visualizations)
- [Benchmarking](#benchmarking)
- [Troubleshooting](#troubleshooting)

---

## Project Overview
This project:
- Loads a large household survey CSV into a Spark DataFrame.
- Performs ETL (cleaning, typing, filtering) and writes aggregated outputs.
- Validates Spark SQL results against Spark DataFrame results.
- Generates multiple unique analyses and exports plots as **JPG only** (no PNG output).

---

## Key Outputs
- ETL exports:
  - `etl_output/connected_counts_by_region.csv`
  - `etl_output/connected_counts_by_region.jsonl`
- Plots (JPG only):
  - `etl_output/plots/jpg/*.jpg`

---

## Project Structure
```text
energy_project/
├─ energy_project.ipynb
├─ pakistanenergyaccesshouseholdpanelsurveydata.csv
├─ etl_output/
│  ├─ connected_counts_by_region.csv
│  ├─ connected_counts_by_region.jsonl
│  └─ plots/
│     └─ jpg/
│        ├─ top_regions_connected.jpg
│        ├─ overall_connection_status.jpg
│        ├─ pct_connected_by_cultural_belt.jpg
│        ├─ heatmap_culturalbelt_vs_area_pct_connected.jpg
│        └─ ...
└─ README.md
```

---

## How to Run

### Option A — Run in VS Code Notebook
1. Open [energy_project.ipynb](energy_project.ipynb) in VS Code (Notebook Editor).
2. Select a Python kernel with PySpark installed.
3. Run all cells.

Plots render in-cell and are saved as JPGs under:
`etl_output/plots/jpg/`

### Option B — Run from Terminal (Reproducible)
From the project root:
```bash
python -m jupyter nbconvert --execute --to notebook --inplace --ExecutePreprocessor.timeout=900 energy_project.ipynb
```

---

## Data Quality Rules
All analyses and plots are designed to **avoid null / empty / UNKNOWN labels**:
- String categories are trimmed and filtered to remove empty strings and `"UNKNOWN"`.
- Connection status is derived and `"Unknown/NA"` is excluded from grouped analyses.
- Numeric charts use safe numeric casting and drop non-numeric / null values.

---

## Visualizations
All plots are saved as **JPG only** to `etl_output/plots/jpg/`.

<details>
<summary><strong>Connection Coverage</strong></summary>

- Top connected regions: [top_regions_connected.jpg](etl_output/plots/jpg/top_regions_connected.jpg)
- Top connected districts: [top_districts_connected.jpg](etl_output/plots/jpg/top_districts_connected.jpg)
- Overall connection split: [overall_connection_status.jpg](etl_output/plots/jpg/overall_connection_status.jpg)

</details>

<details>
<summary><strong>Equity & Demographics</strong></summary>

- Connection by gender (stacked): [connection_by_gender.jpg](etl_output/plots/jpg/connection_by_gender.jpg)
- Connection by SEC (stacked): [connection_by_sec.jpg](etl_output/plots/jpg/connection_by_sec.jpg)
- Connection by area type (Peri Urban/Rural): [connection_by_area_type.jpg](etl_output/plots/jpg/connection_by_area_type.jpg)
- Connection by head education: [connection_by_head_education.jpg](etl_output/plots/jpg/connection_by_head_education.jpg)

</details>

<details>
<summary><strong>Connection Rate (% Connected)</strong></summary>

- By cultural belt: [pct_connected_by_cultural_belt.jpg](etl_output/plots/jpg/pct_connected_by_cultural_belt.jpg)
- By housing type: [pct_connected_by_housing_type.jpg](etl_output/plots/jpg/pct_connected_by_housing_type.jpg)
- By rent vs own: [pct_connected_by_rent_vs_own.jpg](etl_output/plots/jpg/pct_connected_by_rent_vs_own.jpg)

</details>

<details>
<summary><strong>Numeric Distributions & Relationships</strong></summary>

- Household size distribution: [household_size_hist.jpg](etl_output/plots/jpg/household_size_hist.jpg)
- Monthly electricity spend distribution: [monthly_electricity_spend_hist.jpg](etl_output/plots/jpg/monthly_electricity_spend_hist.jpg)
- Distance to nearest connected household distribution: [distance_to_nearest_connected_km_hist.jpg](etl_output/plots/jpg/distance_to_nearest_connected_km_hist.jpg)
- Distance by connection status (boxplot): [distance_km_box_by_status.jpg](etl_output/plots/jpg/distance_km_box_by_status.jpg)
- Spend by connection status (boxplot): [monthly_spend_box_by_status.jpg](etl_output/plots/jpg/monthly_spend_box_by_status.jpg)
- Spend vs distance (scatter): [spend_vs_distance_scatter_by_status.jpg](etl_output/plots/jpg/spend_vs_distance_scatter_by_status.jpg)

</details>

<details>
<summary><strong>2D Study (Heatmap)</strong></summary>

- Cultural Belt × Peri Urban/Rural (% connected): [heatmap_culturalbelt_vs_area_pct_connected.jpg](etl_output/plots/jpg/heatmap_culturalbelt_vs_area_pct_connected.jpg)

</details>

<details>
<summary><strong>Energy Source & Barriers</strong></summary>

- Primary electricity source (top): [primary_electricity_source_top.jpg](etl_output/plots/jpg/primary_electricity_source_top.jpg)
- Reasons not connected (top): [reasons_not_connected_top.jpg](etl_output/plots/jpg/reasons_not_connected_top.jpg)

</details>

---

## Benchmarking
Benchmarking compares:
- Spark DataFrame aggregation
- Spark SQL aggregation
- Pure Python CSV scanning (baseline)

Performance plots:
- Speedup vs Python: [performance_speedup_vs_python.jpg](etl_output/plots/jpg/performance_speedup_vs_python.jpg)

---

## Troubleshooting

<details>
<summary><strong>Graphs don’t render in VS Code</strong></summary>

- Open the notebook in **VS Code Notebook Editor** (not plain text mode).
- Ensure you are running a Python kernel that has `matplotlib` installed.

</details>

<details>
<summary><strong>Spark UI port warning (4040 already in use)</strong></summary>

Spark will auto-fallback to another port (e.g., 4041). This is not an error.

</details>

<details>
<summary><strong>Windows ZMQ RuntimeWarning</strong></summary>

This warning is common on Windows when executing notebooks via nbconvert; it does not prevent execution.

</details>
