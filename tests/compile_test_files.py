#!/usr/bin/env python3
"""
Script to compile test SQL files and output the generated SQL code.
This simulates dbt compilation for different adapters.
"""

import os
import sys
import re
from pathlib import Path

def extract_macro_call(content):
    """Extract the macro call from the SQL file."""
    # Find the macro call pattern
    pattern = r'\{\{\s*prophecy_basics\.GenerateRows\s*\([^}]+\}\}'
    match = re.search(pattern, content, re.DOTALL)
    if match:
        return match.group(0)
    return None

def read_test_file(filepath):
    """Read and return the content of a test file."""
    with open(filepath, 'r') as f:
        return f.read()

def compile_macro_simulation(macro_call, adapter='default'):
    """Simulate macro compilation by showing what would be generated.
    
    This doesn't actually compile the Jinja2, but shows the structure
    based on the adapter type.
    """
    print(f"\n{'='*80}")
    print(f"Adapter: {adapter.upper()}")
    print(f"Macro Call: {macro_call[:100]}...")
    print(f"{'='*80}\n")
    
    # Extract parameters from the macro call
    params = {}
    
    if 'relation_name=None' in macro_call or "relation_name=None" in macro_call:
        relation_name = None
    else:
        # Try to extract relation_name if it's not None
        rel_match = re.search(r"relation_name='([^']+)'", macro_call)
        if rel_match:
            relation_name = rel_match.group(1)
        else:
            relation_name = None
    
    init_expr = re.search(r"init_expr='([^']+)'", macro_call)
    condition_expr = re.search(r"condition_expr='([^']+)'", macro_call)
    loop_expr = re.search(r"loop_expr='([^']+)'", macro_call)
    column_name = re.search(r"column_name='([^']+)'", macro_call)
    max_rows = re.search(r"max_rows=(\d+)", macro_call)
    
    init_expr = init_expr.group(1) if init_expr else '1'
    condition_expr = condition_expr.group(1) if condition_expr else 'value <= 10'
    loop_expr = loop_expr.group(1) if loop_expr else 'value + 1'
    column_name = column_name.group(1) if column_name else 'value'
    max_rows = int(max_rows.group(1)) if max_rows else 10
    
    # Generate SQL structure based on adapter
    # Note: The actual loop_expr would be processed to replace 'value' with 'gen.__gen_value'
    loop_expr_processed = loop_expr.replace(column_name, f'gen.__gen_{column_name}')
    condition_expr_processed = condition_expr.replace(column_name, f'__gen_{column_name}')
    recursion_condition = condition_expr.replace(column_name, f'gen.__gen_{column_name}')
    
    if relation_name:
        # With relation
        if adapter == 'bigquery':
            sql = f"""with recursive gen as (
    -- base case: one row per input record
    select
        STRUCT(src.*) as payload,
        {init_expr} as __gen_{column_name},
        1 as _iter
    from {relation_name} src

    union all

    -- recursive step
    select
        gen.payload as payload,
        {loop_expr_processed} as __gen_{column_name},
        _iter + 1
    from gen
    where _iter < {max_rows}
      and ({recursion_condition})
)
select
    -- Use EXCEPT for BigQuery
    payload.* EXCEPT (__gen_{column_name}),
    __gen_{column_name} as {column_name}
from gen
where {condition_expr_processed}"""
        
        elif adapter == 'duckdb':
            sql = f"""with recursive gen as (
    -- base case: one row per input record
    select
        STRUCT(src.*) as payload,
        {init_expr} as __gen_{column_name},
        1 as _iter
    from {relation_name} src

    union all

    -- recursive step
    select
        gen.payload as payload,
        {loop_expr_processed} as __gen_{column_name},
        _iter + 1
    from gen
    where _iter < {max_rows}
      and ({recursion_condition})
)
select
    -- Use EXCLUDE for DuckDB
    payload.* EXCLUDE (__gen_{column_name}),
    __gen_{column_name} as {column_name}
from gen
where {condition_expr_processed}"""
        
        else:  # default (Spark/Databricks)
            sql = f"""with recursive gen as (
    -- base case: one row per input record
    select
        struct(src.*) as payload,
        {init_expr} as __gen_{column_name},
        1 as _iter
    from {relation_name} src

    union all

    -- recursive step
    select
        gen.payload as payload,
        {loop_expr_processed} as __gen_{column_name},
        _iter + 1
    from gen
    where _iter < {max_rows}
      and ({recursion_condition})
)
select
    -- Use EXCEPT for Spark/Databricks
    payload.* EXCEPT (__gen_{column_name}),
    __gen_{column_name} as {column_name}
from gen
where {condition_expr_processed}"""
    else:
        # Without relation
        loop_expr_processed = loop_expr.replace(column_name, f'gen.__gen_{column_name}')
        condition_expr_processed = condition_expr.replace(column_name, f'__gen_{column_name}')
        recursion_condition = condition_expr.replace(column_name, f'gen.__gen_{column_name}')
        
        sql = f"""with recursive gen as (
    select {init_expr} as __gen_{column_name}, 1 as _iter
    union all
    select
        {loop_expr_processed} as __gen_{column_name},
        _iter + 1
    from gen
    where _iter < {max_rows}
      and ({recursion_condition})
)
select __gen_{column_name} as {column_name}
from gen
where {condition_expr_processed}"""
    
    return sql

def main():
    project_root = Path(__file__).parent
    tests_dir = project_root / "tests"
    
    test_files = [
        ("test_generaterows.sql", "default"),
        ("test_generaterows_bigquery.sql", "bigquery"),
        ("test_generaterows_duckdb.sql", "duckdb"),
        ("test_generaterows_with_relation.sql", "default"),
    ]
    
    print("\n" + "="*80)
    print("GENERATED SQL CODE FOR TEST FILES")
    print("="*80)
    
    for test_file, adapter in test_files:
        filepath = tests_dir / test_file
        if not filepath.exists():
            print(f"\n⚠️  File not found: {test_file}")
            continue
        
        print(f"\n{'─'*80}")
        print(f"📄 File: {test_file}")
        print(f"{'─'*80}")
        
        content = read_test_file(filepath)
        macro_call = extract_macro_call(content)
        
        if not macro_call:
            print("❌ Could not extract macro call from file")
            continue
        
        # Compile the macro
        generated_sql = compile_macro_simulation(macro_call, adapter)
        
        print(generated_sql)
        print()

if __name__ == "__main__":
    main()

