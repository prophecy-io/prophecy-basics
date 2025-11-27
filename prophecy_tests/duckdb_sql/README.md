# DuckDB SQL Tests

This directory contains configuration, setup, and SQL test files for DuckDB macros.

## 📁 Directory Structure

```
prophecy_tests/duckdb_sql/     # Configuration & test files
├── profiles.yml               # DuckDB connection profile
├── requirements.txt           # dbt dependencies
├── venv/                      # Virtual environment
├── CountRecords/              # Tests for CountRecords macro
│   ├── test_count_records.sql
│   ├── test_count_non_null_records.sql
│   └── test_count_distinct_records.sql
└── README.md
```

## ⚠️ Important: Local Testing Requirements

**SQL test files MUST be moved to `tests/` directory before running `dbt test`** because:
- `dbt test` only discovers test files in the `tests/` folder (configured in `dbt_project.yml`)
- The project's `dbt_project.yml` cannot be modified to add `prophecy_tests/` as a test path
- The `tests/` folder cannot be committed to git

**Before testing locally, you must manually move test files to `tests/duckdb_sql/`**

## Test Approach

These tests **call macros directly** from the `macros/` directory using dbt, without creating any models.

Example: Testing `macros/CountRecords.sql` directly with test data.

## Setup

```bash
cd prophecy_tests/duckdb_sql

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dbt and dependencies
pip install -r requirements.txt
```

## Configuration

### Local Testing
The `profiles.yml` in this directory configures DuckDB with an in-memory database for fast, local testing without any external dependencies.

### CI/CD
In GitHub Actions, the profile is created dynamically in `~/.dbt/profiles.yml` - no physical file needed in the repo!

## Running Tests Locally

**⚠️ IMPORTANT**: Before running tests, you must manually move SQL files to the `tests/` directory:

```bash
# From project root

# Step 1: Copy all test folders to tests/ directory (required for dbt test)
# Note: Excludes venv to avoid conflicts with dbt packages
mkdir -p tests/duckdb_sql
find prophecy_tests/duckdb_sql -mindepth 1 -maxdepth 1 -type d ! -name "venv" -exec cp -r {} tests/duckdb_sql/ \;

# Step 2: Navigate to test directory
cd prophecy_tests/duckdb_sql

# Step 3: Setup environment (first time only)
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Step 4: Run tests
dbt test --project-dir ../.. --profiles-dir .
```

**Complete one-liner from project root:**
```bash
mkdir -p tests/duckdb_sql && find prophecy_tests/duckdb_sql -mindepth 1 -maxdepth 1 -type d ! -name "venv" -exec cp -r {} tests/duckdb_sql/ \; && cd prophecy_tests/duckdb_sql && source venv/bin/activate && dbt test --project-dir ../.. --profiles-dir .
```

## Writing New Tests

1. **Create SQL test files in `prophecy_tests/duckdb_sql/<MacroName>/`**
2. **Tests directly invoke macros** like:

```sql
-- Example: prophecy_tests/duckdb_sql/CountRecords/test_count_records.sql
WITH test_data AS (
    SELECT 1 AS id, 'Alice' AS name
),
expected_result AS (
    SELECT 1 AS total_records
),
actual_result AS (
    SELECT {{ prophecy_basics.CountRecords('test_data', [], 'count_total_records') }} AS total_records
)
SELECT * FROM actual_result
WHERE total_records != (SELECT total_records FROM expected_result)
```

3. **No models needed** - just call macros with inline test data
4. **Tests pass if they return 0 rows** (no discrepancies found)
5. **Before running**: Copy files to `tests/duckdb_sql/<MacroName>/` (see Running Tests above)

## Advantages

- 🚀 **Fast**: In-memory database
- 💾 **Lightweight**: No external database needed
- 🔧 **Easy**: Works out of the box
- 📊 **Full SQL Support**: Comprehensive SQL dialect

## Status
✅ **Active** - CountRecords macro tests implemented and passing (3/3 tests)
