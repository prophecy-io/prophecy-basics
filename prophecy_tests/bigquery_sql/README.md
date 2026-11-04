# BigQuery SQL Tests

This directory contains tests for BigQuery SQL macros.

## Test Approach

These tests **call macros directly** from the `macros/` directory using dbt, without creating any models.
Source rows are inline CTEs rather than temporary tables, because each `run_query` call is a separate
BigQuery job and temporary tables do not outlive it.

## Setup

```bash
cd prophecy_tests/bigquery_sql

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dbt and dependencies
pip install -r requirements.txt
```

## Configuration

Add a `prophecy-default` profile with a BigQuery target, for example:

```yaml
prophecy-default:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth            # or service-account with keyfile
      project: "{{ env_var('BIGQUERY_PROJECT') }}"
      dataset: "{{ env_var('BIGQUERY_DATASET') }}"
      threads: 1
```

## Running Tests

```bash
# From project root
mkdir -p tests/bigquery_sql
find prophecy_tests/bigquery_sql -mindepth 1 -maxdepth 1 -type d ! -name "venv" -exec cp -r {} tests/bigquery_sql/ \;

cd prophecy_tests/bigquery_sql
source venv/bin/activate
dbt test --project-dir ../..
```

## Status
⚠️ Not yet wired into CI; run manually against a BigQuery project
