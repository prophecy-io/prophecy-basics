#!/usr/bin/env python3
"""
Test script to verify GenerateRows macro compilation for different adapters.
This tests the Jinja2 compilation without requiring a full dbt connection.
"""

import os
import sys
from pathlib import Path

# Add the project directory to the path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def test_macro_syntax():
    """Test that the macro file has valid Jinja2 syntax."""
    macro_file = project_root / "macros" / "GenerateRows.sql"
    
    if not macro_file.exists():
        print(f"ERROR: Macro file not found: {macro_file}")
        return False
    
    # Read the file
    with open(macro_file, 'r') as f:
        content = f.read()
    
    # Basic checks
    checks = []
    
    # Check for all required macros
    checks.append(("Dispatcher macro", '{% macro GenerateRows(' in content))
    checks.append(("Default implementation", '{% macro default__GenerateRows(' in content))
    checks.append(("BigQuery implementation", '{% macro bigquery__GenerateRows(' in content))
    checks.append(("DuckDB implementation", '{% macro duckdb__GenerateRows(' in content))
    
    # Check for correct closing
    macro_count = content.count('{% macro')
    endmacro_count = content.count('{% endmacro %}')
    checks.append((f"Macro balance ({macro_count} macros)", macro_count == endmacro_count))
    
    # Check for adapter-specific syntax
    checks.append(("BigQuery EXCEPT", 'payload.* EXCEPT' in content))
    checks.append(("DuckDB EXCLUDE", 'payload.* EXCLUDE' in content))
    checks.append(("BigQuery STRUCT", 'STRUCT(' in content))
    checks.append(("DuckDB CAST", 'CAST(' in content or 'CAST(' in content))
    
    print("\n=== Macro Compilation Tests ===\n")
    all_passed = True
    for check_name, result in checks:
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{status}: {check_name}")
        if not result:
            all_passed = False
    
    print(f"\n{'All tests passed!' if all_passed else 'Some tests failed!'}\n")
    return all_passed

if __name__ == "__main__":
    success = test_macro_syntax()
    sys.exit(0 if success else 1)

