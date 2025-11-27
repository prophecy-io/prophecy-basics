# Quick Start Guide - Prophecy Basics Testing

## 🚀 Run Tests in 30 Seconds

### Local Testing

```bash
# Navigate to test directory
cd prophecy_tests

# Run PySpark tests (currently active)
python run_tests.py pyspark
```

### Manual CI/CD Trigger

1. Go to **GitHub** → **Actions** tab
2. Click **Prophecy Basics Test Suite**
3. Click **Run workflow** button
4. Select test type: `pyspark`, `snowflake_sql`, `databricks_sql`, `duckdb_sql`, or `all`
5. Click **Run workflow**

## 📂 Test Structure

```
prophecy_tests/
├── pyspark/              ✅ Active - 5 test files
├── snowflake_sql/        ⚠️  Coming Soon
├── databricks_sql/       ⚠️  Coming Soon
├── duckdb_sql/          ⚠️  Coming Soon
└── run_tests.py         ← Use this to run tests
```

## 🎯 Common Commands

```bash
# Basic test run
python run_tests.py pyspark

# With coverage report
python run_tests.py pyspark --coverage

# Verbose output
python run_tests.py pyspark --verbose

# Generate HTML report
python run_tests.py pyspark --html

# All options combined
python run_tests.py pyspark --coverage --verbose --html

# Run all available test suites
python run_tests.py all
```

## 🔧 First Time Setup (Optional)

If you want to run tests in a virtual environment:

```bash
cd prophecy_tests/pyspark

# Create virtual environment
python -m venv venv

# Activate it
source venv/bin/activate  # macOS/Linux
# or
venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt

# Add gems to path
export PYTHONPATH=../../gems:$PYTHONPATH

# Run tests
pytest -v
```

## ✅ What's Tested

### Currently Active Tests
**PySpark (pytest)** ✅
- DataCleansing - 18 test cases
- DataEncoderDecoder - Multiple scenarios
- DataMasking - Various masking strategies
- JSONParse - JSON parsing and extraction
- TextToColumns - Text splitting operations

### Coming Soon
**SQL Platforms (dbt unit tests)** ⚠️
- Snowflake SQL macro tests
- Databricks SQL macro tests  
- DuckDB SQL macro tests

## 🔧 Test Frameworks

This project uses **two different testing approaches**:

1. **pytest** (for PySpark): Tests Python gem implementations
2. **dbt unit tests** (for SQL): Tests SQL macros using dbt's unit testing framework

The test runner automatically handles both types!

## 🐛 Troubleshooting

### "Java not found" error
```bash
# Install Java 11 (required for PySpark)
brew install openjdk@11  # macOS
```

### "Module not found" error
```bash
# Make sure you're in the correct directory
cd prophecy_tests

# Or add gems to PYTHONPATH
export PYTHONPATH=/path/to/prophecy-basics/gems:$PYTHONPATH
```

### Tests fail locally but pass in CI
```bash
# Clean pytest cache
rm -rf .pytest_cache __pycache__

# Re-run tests
python run_tests.py pyspark
```

## 📖 More Information

- **Detailed Testing Guide**: [README.md](README.md)
- **Contributing Guide**: [CONTRIBUTING.md](CONTRIBUTING.md)
- **CI/CD Documentation**: [../.github/workflows/README.md](../.github/workflows/README.md)

## 💡 Pro Tips

1. **Fast Feedback**: Run tests locally before pushing
2. **Parallel Testing**: Use `--parallel` flag for faster execution
3. **Focused Testing**: Run specific test files with `pytest test_file.py`
4. **Coverage Monitoring**: Use `--coverage` to track test coverage
5. **CI Artifacts**: Download test reports from GitHub Actions runs

## 🎉 Next Steps

1. Run your first test: `python run_tests.py pyspark`
2. Check the results
3. Add new tests following [CONTRIBUTING.md](CONTRIBUTING.md)
4. Push changes and watch CI/CD in action!

---

**Need Help?** Check the main [README.md](README.md) or create an issue.

