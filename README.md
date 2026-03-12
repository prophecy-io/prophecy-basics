# prophecy-basics

## Development: conda environment `gem_testing`

Use the **gem_testing** conda environment for dbt with DuckDB and BigQuery (avoids PyArrow-related segfault on macOS).

**Create the environment (one-time):**
```bash
conda env create -f environment.yml -n gem_testing
```

**Activate and run:**
```bash
conda activate gem_testing
dbt compile                              # default profile (DuckDB)
dbt compile --target bigquery --select 'test_data_masking_*'   # BigQuery SQL (requires BQ profile)
```

**Requirements:** Python 3.11, `dbt-core`, `dbt-duckdb`, `dbt-bigquery`, and `pyarrow>=15,<17` (pinned to avoid import crash). See `environment.yml` and `requirements.txt`.