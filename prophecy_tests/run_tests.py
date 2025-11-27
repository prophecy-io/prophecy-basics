#!/usr/bin/env python3
"""
Python Test Runner for Prophecy Basics

This script provides a cross-platform way to run different test suites.

Usage:
    python run_tests.py [test_type] [options]

Examples:
    python run_tests.py pyspark
    python run_tests.py all --coverage
    python run_tests.py pyspark --verbose --html
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import List, Optional


class Colors:
    """ANSI color codes for terminal output"""
    RED = '\033[0;31m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    BLUE = '\033[0;34m'
    NC = '\033[0m'  # No Color


class TestRunner:
    """Manages test execution for different test suites"""
    
    AVAILABLE_SUITES = ['pyspark', 'snowflake_sql', 'databricks_sql', 'duckdb_sql']
    DBT_SUITES = {'snowflake_sql', 'databricks_sql', 'duckdb_sql'}
    
    def __init__(self, script_dir: Path):
        self.script_dir = script_dir
        self.project_root = script_dir.parent
        self.failed_suites: List[str] = []
    
    def is_dbt_suite(self, suite_name: str) -> bool:
        """Check if suite uses dbt unit tests"""
        return suite_name in self.DBT_SUITES
    
    def print_header(self, message: str):
        """Print a formatted header"""
        print(f"{Colors.BLUE}{'=' * 60}{Colors.NC}")
        print(f"{Colors.BLUE}{message}{Colors.NC}")
        print(f"{Colors.BLUE}{'=' * 60}{Colors.NC}")
        print()
    
    def print_info(self, message: str):
        """Print an info message"""
        print(f"{Colors.GREEN}{message}{Colors.NC}")
    
    def print_warning(self, message: str):
        """Print a warning message"""
        print(f"{Colors.YELLOW}{message}{Colors.NC}")
    
    def print_error(self, message: str):
        """Print an error message"""
        print(f"{Colors.RED}{message}{Colors.NC}")
    
    def has_tests(self, suite_name: str) -> bool:
        """Check if a test suite has test files"""
        test_dir = self.script_dir / suite_name
        if not test_dir.is_dir():
            return False
        
        test_files = list(test_dir.glob("test_*.py"))
        return len(test_files) > 0
    
    def install_requirements(self, test_dir: Path) -> bool:
        """Install requirements for a test suite"""
        requirements_file = test_dir / "requirements.txt"
        if not requirements_file.exists():
            return True
        
        self.print_info(f"Installing dependencies from {requirements_file}")
        try:
            subprocess.run(
                [sys.executable, "-m", "pip", "install", "-q", "-r", str(requirements_file)],
                check=True
            )
            print()
            return True
        except subprocess.CalledProcessError as e:
            self.print_error(f"Failed to install requirements: {e}")
            return False
    
    def run_dbt_tests(self, suite_name: str) -> bool:
        """Run dbt unit tests for a suite"""
        self.print_warning(f"Running dbt unit tests for {suite_name}...")
        print()
        
        test_dir = self.script_dir / suite_name
        venv_dir = test_dir / "venv"
        
        # Create virtual environment if it doesn't exist
        if not venv_dir.exists():
            self.print_info("Creating virtual environment...")
            subprocess.run(
                [sys.executable, "-m", "venv", str(venv_dir)],
                check=True
            )
        
        # Determine venv python and pip paths
        if sys.platform == "win32":
            venv_python = venv_dir / "Scripts" / "python.exe"
            venv_pip = venv_dir / "Scripts" / "pip.exe"
        else:
            venv_python = venv_dir / "bin" / "python"
            venv_pip = venv_dir / "bin" / "pip"
        
        # Install dbt dependencies in the virtual environment
        self.print_info("Installing dbt dependencies...")
        requirements_file = test_dir / "requirements.txt"
        if requirements_file.exists():
            subprocess.run(
                [str(venv_pip), "install", "-q", "-r", str(requirements_file)],
                check=True
            )
        print()
        
        # Copy profiles.yml if it exists
        profiles_file = test_dir / "profiles.yml"
        if profiles_file.exists():
            self.print_info(f"Using profiles.yml from {test_dir}")
            import shutil
            shutil.copy(profiles_file, self.project_root / "profiles.yml")
        
        # Run dbt unit tests
        self.print_info("Running: dbt test --select test_type:unit")
        print()
        
        try:
            # Determine dbt path in venv
            if sys.platform == "win32":
                dbt_cmd = str(venv_dir / "Scripts" / "dbt.exe")
            else:
                dbt_cmd = str(venv_dir / "bin" / "dbt")
            
            subprocess.run(
                [dbt_cmd, "test", "--select", "test_type:unit"],
                cwd=self.project_root,
                check=True
            )
            print()
            self.print_info(f"✓ {suite_name} dbt tests passed")
            return True
        except subprocess.CalledProcessError:
            print()
            self.print_error(f"✗ {suite_name} dbt tests failed")
            return False
    
    def run_pytest_tests(
        self,
        suite_name: str,
        coverage: bool = False,
        verbose: bool = False,
        parallel: bool = False,
        html_report: bool = False
    ) -> bool:
        """Run pytest tests for a suite"""
        test_dir = self.script_dir / suite_name
        
        if not self.has_tests(suite_name):
            self.print_warning(f"Warning: No test files found in {test_dir}")
            return True
        
        # Install requirements
        if not self.install_requirements(test_dir):
            return False
        
        # Build pytest command
        pytest_cmd = [sys.executable, "-m", "pytest"]
        
        # Add verbosity
        if verbose:
            pytest_cmd.append("-vv")
        else:
            pytest_cmd.append("-v")
        
        pytest_cmd.append("--tb=short")
        
        # Add coverage
        if coverage:
            pytest_cmd.extend(["--cov=.", "--cov-report=term", "--cov-report=xml"])
        
        # Add HTML report
        if html_report:
            report_dir = self.project_root / "test-results"
            report_dir.mkdir(exist_ok=True)
            report_file = report_dir / f"{suite_name}-report.html"
            pytest_cmd.extend(["--html", str(report_file), "--self-contained-html"])
        
        # Add parallel execution
        if parallel:
            pytest_cmd.extend(["-n", "auto"])
        
        # Set up environment for pyspark tests
        env = os.environ.copy()
        if suite_name == "pyspark":
            gems_dir = str(self.project_root / "gems")
            pythonpath = env.get("PYTHONPATH", "")
            if pythonpath:
                env["PYTHONPATH"] = f"{gems_dir}:{pythonpath}"
            else:
                env["PYTHONPATH"] = gems_dir
            self.print_info(f"Added gems to PYTHONPATH: {gems_dir}")
        
        # Run tests
        self.print_info(f"Running: {' '.join(pytest_cmd)}")
        print()
        
        try:
            subprocess.run(
                pytest_cmd,
                cwd=test_dir,
                env=env,
                check=True
            )
            print()
            self.print_info(f"✓ {suite_name} tests passed")
            return True
        except subprocess.CalledProcessError:
            print()
            self.print_error(f"✗ {suite_name} tests failed")
            return False
    
    def run_test_suite(
        self,
        suite_name: str,
        coverage: bool = False,
        verbose: bool = False,
        parallel: bool = False,
        html_report: bool = False
    ) -> bool:
        """Run tests for a specific suite"""
        self.print_warning(f"Running {suite_name} tests...")
        print()
        
        test_dir = self.script_dir / suite_name
        
        if not test_dir.is_dir():
            self.print_error(f"Error: Test directory {test_dir} does not exist")
            return False
        
        # Determine test type and run accordingly
        if self.is_dbt_suite(suite_name):
            return self.run_dbt_tests(suite_name)
        else:
            return self.run_pytest_tests(suite_name, coverage, verbose, parallel, html_report)
    
    def run_all_suites(
        self,
        coverage: bool = False,
        verbose: bool = False,
        parallel: bool = False,
        html_report: bool = False
    ) -> bool:
        """Run all available test suites"""
        self.print_header("Test Type: All")
        
        for suite in self.AVAILABLE_SUITES:
            if self.has_tests(suite):
                print()
                print(f"{Colors.BLUE}{'-' * 60}{Colors.NC}")
                if not self.run_test_suite(suite, coverage, verbose, parallel, html_report):
                    self.failed_suites.append(suite)
        
        # Print summary
        print()
        self.print_header("Test Summary")
        
        if not self.failed_suites:
            self.print_info("✓ All test suites passed!")
            return True
        else:
            self.print_error(f"✗ Failed test suites: {', '.join(self.failed_suites)}")
            return False


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Test runner for Prophecy Basics",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_tests.py pyspark
  python run_tests.py all --coverage
  python run_tests.py pyspark --verbose --html
        """
    )
    
    parser.add_argument(
        "test_type",
        choices=["pyspark", "snowflake_sql", "databricks_sql", "duckdb_sql", "all"],
        help="Type of tests to run"
    )
    
    parser.add_argument(
        "-c", "--coverage",
        action="store_true",
        help="Enable coverage reporting"
    )
    
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output"
    )
    
    parser.add_argument(
        "-p", "--parallel",
        action="store_true",
        help="Enable parallel test execution"
    )
    
    parser.add_argument(
        "-h", "--html",
        action="store_true",
        dest="html_report",
        help="Generate HTML test report"
    )
    
    args = parser.parse_args()
    
    # Get script directory
    script_dir = Path(__file__).parent.resolve()
    
    # Create test runner
    runner = TestRunner(script_dir)
    runner.print_header("Prophecy Basics Test Runner")
    
    # Run tests
    if args.test_type == "all":
        success = runner.run_all_suites(
            coverage=args.coverage,
            verbose=args.verbose,
            parallel=args.parallel,
            html_report=args.html_report
        )
    else:
        runner.print_header(f"Test Type: {args.test_type}")
        success = runner.run_test_suite(
            args.test_type,
            coverage=args.coverage,
            verbose=args.verbose,
            parallel=args.parallel,
            html_report=args.html_report
        )
    
    print()
    runner.print_info("Done!")
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

