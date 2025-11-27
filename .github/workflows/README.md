# GitHub Actions Workflows

## Test Workflow (`test.yml`)

This workflow provides comprehensive CI/CD testing for the Prophecy Basics project.

### Triggers

The workflow runs on:
- **Push**: to `main`, `develop`, or `deb/**` branches
- **Pull Request**: to `main` or `develop` branches
- **Manual Dispatch**: via GitHub Actions UI

### Workflow Structure

```
determine-tests (Job 1)
    ↓
├─→ test-pyspark (Job 2)
├─→ test-snowflake-sql (Job 3)
├─→ test-databricks-sql (Job 4)
└─→ test-duckdb-sql (Job 5)
    ↓
test-summary (Job 6)
```

### Jobs

#### 1. `determine-tests`
- Determines which test suites to run based on:
  - Manual input selection
  - Available test files
  - Changed files (auto-detection)
- Sets output flags for each test suite

#### 2. `test-pyspark` 🔥
- Runs PySpark/Databricks tests
- Requirements:
  - Python 3.11
  - Java 11 (for Spark)
- Features:
  - Dependency caching
  - Coverage reporting
  - HTML test reports
  - JUnit XML results

#### 3. `test-snowflake-sql` ❄️
- Runs Snowflake SQL tests
- Currently skipped (no tests available yet)
- Ready for implementation

#### 4. `test-databricks-sql` 🧱
- Runs Databricks SQL tests
- Currently skipped (no tests available yet)
- Ready for implementation

#### 5. `test-duckdb-sql` 🦆
- Runs DuckDB SQL tests
- Currently skipped (no tests available yet)
- Ready for implementation

#### 6. `test-summary` 📊
- Aggregates results from all test jobs
- Creates GitHub step summary with visual table
- Downloads and displays all artifacts

### Manual Workflow Dispatch

To run specific tests manually:

1. Navigate to **Actions** tab
2. Select **Prophecy Basics Test Suite**
3. Click **Run workflow**
4. Select test type from dropdown:
   - `pyspark` - Only PySpark tests
   - `snowflake_sql` - Only Snowflake tests
   - `databricks_sql` - Only Databricks SQL tests
   - `duckdb_sql` - Only DuckDB tests
   - `all` - All available tests

### Artifacts

Each test job uploads the following artifacts:

- **Test Results**: JUnit XML and HTML reports
- **Coverage Reports**: XML and HTML coverage data (for PySpark)

Artifacts are retained for 30 days.

### Environment Variables

```yaml
PYTHON_VERSION: '3.11'  # Python version for all tests
```

### Caching

The workflow uses caching to speed up runs:
- pip dependencies (per test suite)
- PySpark dependencies

### Test Summary

After each run, a formatted summary is displayed in the GitHub UI showing:
- Status of each test suite (success/failure/skipped)
- Trigger type (push/PR/manual)
- Selected test type (for manual runs)

### Examples

#### Auto-run on Push
```bash
git push origin deb/my-feature
# Workflow runs automatically with pyspark tests
```

#### Manual Run - Specific Test
1. Actions → Prophecy Basics Test Suite → Run workflow
2. Select `pyspark`
3. Click "Run workflow"

#### Manual Run - All Tests
1. Actions → Prophecy Basics Test Suite → Run workflow
2. Select `all`
3. Click "Run workflow"

### Extending the Workflow

To add a new test suite:

1. Create test directory: `prophecy_tests/new_suite/`
2. Add `requirements.txt` and test files
3. The workflow will auto-detect the new suite
4. Update `determine-tests` job if needed

### Troubleshooting

#### Tests Skipped
- Check if test files exist in the test directory
- Verify `has_tests()` function in `determine-tests` job

#### Job Failures
- Check test results artifact for details
- Review job logs in GitHub Actions

#### Cache Issues
- Clear cache: Actions → Caches → Delete
- Workflow will rebuild cache on next run

### Best Practices

1. **Branch Protection**: Require test workflow to pass before merging
2. **Parallel Testing**: Jobs run in parallel for faster feedback
3. **Artifact Review**: Download artifacts to review detailed test reports
4. **Coverage Monitoring**: Track coverage trends over time
5. **Manual Testing**: Use manual dispatch for quick validation

## Adding More Workflows

To add new workflows (e.g., for deployment, linting):

1. Create new `.yml` file in this directory
2. Follow GitHub Actions syntax
3. Reference existing workflows for patterns
4. Update this README with documentation

