#!/bin/bash

###############################################################################
# Test Runner Script for Prophecy Basics
# 
# This script provides a convenient way to run different test suites locally
# or in CI/CD environments.
#
# Usage:
#   ./run_tests.sh [test_type] [options]
#
# Examples:
#   ./run_tests.sh pyspark              # Run PySpark tests
#   ./run_tests.sh all                  # Run all available tests
#   ./run_tests.sh pyspark --coverage   # Run with coverage
#   ./run_tests.sh pyspark --verbose    # Run with verbose output
###############################################################################

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
TEST_TYPE="${1:-pyspark}"
COVERAGE=false
VERBOSE=false
PARALLEL=false
HTML_REPORT=false

# Parse arguments
shift || true
while [[ $# -gt 0 ]]; do
    case $1 in
        --coverage|-c)
            COVERAGE=true
            shift
            ;;
        --verbose|-v)
            VERBOSE=true
            shift
            ;;
        --parallel|-p)
            PARALLEL=true
            shift
            ;;
        --html|-h)
            HTML_REPORT=true
            shift
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Prophecy Basics Test Runner${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Function to check if a test suite has tests
has_tests() {
    local test_dir="$SCRIPT_DIR/$1"
    if [ -d "$test_dir" ]; then
        if find "$test_dir" -name "test_*.py" -type f | grep -q .; then
            return 0
        fi
    fi
    return 1
}

# Function to check if suite uses dbt unit tests
is_dbt_suite() {
    local suite_name=$1
    case $suite_name in
        snowflake_sql|databricks_sql|duckdb_sql)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

# Function to run dbt unit tests
run_dbt_tests() {
    local suite_name=$1
    local test_dir="$SCRIPT_DIR/$suite_name"
    
    echo -e "${YELLOW}Running dbt unit tests for $suite_name...${NC}"
    echo ""
    
    # Create virtual environment if it doesn't exist
    if [[ ! -d "$test_dir/venv" ]]; then
        echo -e "${GREEN}Creating virtual environment...${NC}"
        python3 -m venv "$test_dir/venv"
    fi
    
    # Activate virtual environment and install packages
    source "$test_dir/venv/bin/activate"
    
    echo -e "${GREEN}Installing dbt dependencies...${NC}"
    pip install -q -r "$test_dir/requirements.txt"
    echo ""
    
    # Copy profiles.yml to the test directory if it exists
    if [ -f "$test_dir/profiles.yml" ]; then
        echo -e "${GREEN}Using profiles.yml from $test_dir${NC}"
        cp "$test_dir/profiles.yml" "$PROJECT_ROOT/profiles.yml"
    fi
    
    # Run dbt unit tests
    cd "$PROJECT_ROOT"
    echo -e "${GREEN}Running: dbt test --select test_type:unit${NC}"
    echo ""
    
    if dbt test --select test_type:unit; then
        echo ""
        echo -e "${GREEN}✓ $suite_name dbt tests passed${NC}"
        deactivate
        return 0
    else
        echo ""
        echo -e "${RED}✗ $suite_name dbt tests failed${NC}"
        deactivate
        return 1
    fi
}

# Function to run pytest tests
run_pytest_tests() {
    local suite_name=$1
    local test_dir="$SCRIPT_DIR/$suite_name"
    
    # Check if requirements.txt exists
    if [ -f "$test_dir/requirements.txt" ]; then
        echo -e "${GREEN}Installing dependencies from $test_dir/requirements.txt${NC}"
        pip install -q -r "$test_dir/requirements.txt"
        echo ""
    fi
    
    # Build pytest command
    local pytest_cmd="pytest"
    local pytest_args=()
    
    # Add verbosity
    if [ "$VERBOSE" = true ]; then
        pytest_args+=("-vv")
    else
        pytest_args+=("-v")
    fi
    
    pytest_args+=("--tb=short")
    
    # Add coverage
    if [ "$COVERAGE" = true ]; then
        pytest_args+=("--cov=." "--cov-report=term" "--cov-report=xml")
    fi
    
    # Add HTML report
    if [ "$HTML_REPORT" = true ]; then
        pytest_args+=("--html=../../test-results/${suite_name}-report.html" "--self-contained-html")
    fi
    
    # Add parallel execution
    if [ "$PARALLEL" = true ]; then
        pytest_args+=("-n" "auto")
    fi
    
    # Add gems to PYTHONPATH for pyspark tests
    if [ "$suite_name" = "pyspark" ]; then
        export PYTHONPATH="$PROJECT_ROOT/gems:$PYTHONPATH"
        echo -e "${GREEN}Added gems to PYTHONPATH: $PROJECT_ROOT/gems${NC}"
    fi
    
    # Run tests
    cd "$test_dir"
    echo -e "${GREEN}Running: $pytest_cmd ${pytest_args[*]}${NC}"
    echo ""
    
    if $pytest_cmd "${pytest_args[@]}"; then
        echo ""
        echo -e "${GREEN}✓ $suite_name tests passed${NC}"
        return 0
    else
        echo ""
        echo -e "${RED}✗ $suite_name tests failed${NC}"
        return 1
    fi
}

# Function to run tests for a specific suite
run_test_suite() {
    local suite_name=$1
    local test_dir="$SCRIPT_DIR/$suite_name"
    
    echo -e "${YELLOW}Running $suite_name tests...${NC}"
    echo ""
    
    if [ ! -d "$test_dir" ]; then
        echo -e "${RED}Error: Test directory $test_dir does not exist${NC}"
        return 1
    fi
    
    if ! has_tests "$suite_name" && ! is_dbt_suite "$suite_name"; then
        echo -e "${YELLOW}Warning: No test files found in $test_dir${NC}"
        return 0
    fi
    
    # Determine test type and run accordingly
    if is_dbt_suite "$suite_name"; then
        run_dbt_tests "$suite_name"
    else
        run_pytest_tests "$suite_name"
    fi
}

# Main execution
case $TEST_TYPE in
    pyspark)
        echo -e "${BLUE}Test Type: PySpark${NC}"
        echo ""
        run_test_suite "pyspark"
        ;;
    
    snowflake_sql|snowflake)
        echo -e "${BLUE}Test Type: Snowflake SQL${NC}"
        echo ""
        run_test_suite "snowflake_sql"
        ;;
    
    databricks_sql|databricks)
        echo -e "${BLUE}Test Type: Databricks SQL${NC}"
        echo ""
        run_test_suite "databricks_sql"
        ;;
    
    duckdb_sql|duckdb)
        echo -e "${BLUE}Test Type: DuckDB SQL${NC}"
        echo ""
        run_test_suite "duckdb_sql"
        ;;
    
    all)
        echo -e "${BLUE}Test Type: All${NC}"
        echo ""
        
        failed_suites=()
        
        for suite in pyspark snowflake_sql databricks_sql duckdb_sql; do
            if has_tests "$suite"; then
                echo ""
                echo -e "${BLUE}----------------------------------------${NC}"
                if ! run_test_suite "$suite"; then
                    failed_suites+=("$suite")
                fi
            fi
        done
        
        echo ""
        echo -e "${BLUE}========================================${NC}"
        echo -e "${BLUE}Test Summary${NC}"
        echo -e "${BLUE}========================================${NC}"
        
        if [ ${#failed_suites[@]} -eq 0 ]; then
            echo -e "${GREEN}✓ All test suites passed!${NC}"
            exit 0
        else
            echo -e "${RED}✗ Failed test suites: ${failed_suites[*]}${NC}"
            exit 1
        fi
        ;;
    
    *)
        echo -e "${RED}Unknown test type: $TEST_TYPE${NC}"
        echo ""
        echo "Available test types:"
        echo "  - pyspark"
        echo "  - snowflake_sql (snowflake)"
        echo "  - databricks_sql (databricks)"
        echo "  - duckdb_sql (duckdb)"
        echo "  - all"
        echo ""
        echo "Options:"
        echo "  --coverage, -c    Enable coverage reporting"
        echo "  --verbose, -v     Enable verbose output"
        echo "  --parallel, -p    Enable parallel test execution"
        echo "  --html, -h        Generate HTML test report"
        exit 1
        ;;
esac

echo ""
echo -e "${GREEN}Done!${NC}"

