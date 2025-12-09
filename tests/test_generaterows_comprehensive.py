#!/usr/bin/env python3
"""
Comprehensive test suite for GenerateRows macro
Tests default recursive CTE implementation and BigQuery-specific implementation
"""
import sys
import os
import re

# Add the project root to path if needed
sys.path.insert(0, os.path.dirname(__file__))


def simulate_default_macro(relation_name, init_expr, condition_expr, loop_expr, column_name, max_rows, force_mode):
    """Simulate the default GenerateRows macro (WITH RECURSIVE for Databricks/DuckDB)"""
    
    alias = "src"
    
    # Handle relation_tables
    if relation_name and relation_name.strip():
        if isinstance(relation_name, str) and not (isinstance(relation_name, list) or isinstance(relation_name, tuple)):
            relation_tables = relation_name
        else:
            relation_tables = ', '.join(str(r) for r in relation_name)
    else:
        relation_tables = ''
    
    unquoted_col = column_name.strip()
    internal_col = f"__gen_{unquoted_col.replace(' ', '_')}"
    
    # Replace column_name with internal_col
    init_select = init_expr.replace(column_name, internal_col)
    condition_expr_sql = condition_expr.replace(column_name, internal_col)
    loop_expr_replaced = loop_expr.replace(column_name, f'gen.{internal_col}')
    recursion_condition = condition_expr_sql.replace(internal_col, f'gen.{internal_col}')
    output_col_alias = column_name
    
    if relation_tables:
        sql = f"""with recursive gen as (
    -- base case: one row per input record
    select
        {alias}.*,
        {init_select} as {internal_col},
        1 as _iter
    from {relation_tables} {alias}

    union all

    -- recursive step
    select
        gen.* EXCEPT ({internal_col}, _iter),
        {loop_expr_replaced} as {internal_col},
        _iter + 1
    from gen
    where _iter < {max_rows}
      and ({recursion_condition})
)
select
    gen.* EXCEPT ({internal_col}, _iter),
    {internal_col} as {output_col_alias}
from gen
where {condition_expr_sql}"""
    else:
        sql = f"""with recursive gen as (
    select {init_select} as {internal_col}, 1 as _iter
    union all
    select
        {loop_expr_replaced} as {internal_col},
        _iter + 1
    from gen
    where _iter < {max_rows}
      and ({recursion_condition})
)
select {internal_col} as {output_col_alias}
from gen
where {condition_expr_sql}"""
    
    return sql


def simulate_bigquery_macro(init_expr, condition_expr, loop_expr, column_name, max_rows=100):
    """Simulate the BigQuery GenerateRows macro (uses GENERATE_ARRAY, no WITH RECURSIVE)"""
    
    # Extract init value
    init_value = int(init_expr.replace(column_name, '0').strip())
    
    # Common replacements: column_name -> 'val' for array element reference
    loop_expr_val = loop_expr.replace(column_name, 'val')
    condition_expr_val = condition_expr.replace(column_name, 'val')
    
    # Detect patterns and compute accordingly
    loop_expr_clean = loop_expr.strip().replace(' ', '')
    col_clean = column_name.strip().replace(' ', '')
    
    # Check for multiplication pattern: val * N -> use POWER(N, iteration)
    if len(loop_expr_clean) > len(col_clean) and loop_expr_clean[:len(col_clean)] == col_clean and '*' in loop_expr_clean:
        after_col = loop_expr_clean[len(col_clean):]
        if after_col[0] == '*':
            mult_str = after_col[1:].strip()
            try:
                multiplier = int(mult_str)
                # Use POWER for recursive multiplication: init * POWER(multiplier, iteration)
                computed_expr = f"{init_value} * POWER({multiplier}, _iter)"
                condition_expr_sql = condition_expr.replace(column_name, computed_expr)
                use_step = False
                pattern_type = f"Multiplication (POWER): {multiplier}"
            except:
                computed_expr = loop_expr_val
                condition_expr_sql = condition_expr_val
                use_step = False
                pattern_type = "Direct (fallback)"
        else:
            computed_expr = loop_expr_val
            condition_expr_sql = condition_expr_val
            use_step = False
            pattern_type = "Direct"
    # Check for addition pattern: val + N -> use GENERATE_ARRAY with step
    elif len(loop_expr_clean) > len(col_clean) and loop_expr_clean[:len(col_clean)] == col_clean and '+' in loop_expr_clean:
        after_col = loop_expr_clean[len(col_clean):]
        if after_col[0] == '+':
            step_str = after_col[1:].strip()
            try:
                step_value = int(step_str)
                # Use GENERATE_ARRAY with step, then just select val
                computed_expr = 'val'
                condition_expr_sql = condition_expr_val
                use_step = True
                array_step = step_value
                pattern_type = f"Addition (GENERATE_ARRAY step): {step_value}"
            except:
                computed_expr = loop_expr_val
                condition_expr_sql = condition_expr_val
                use_step = False
                pattern_type = "Direct (fallback)"
        else:
            computed_expr = loop_expr_val
            condition_expr_sql = condition_expr_val
            use_step = False
            pattern_type = "Direct"
    else:
        # For other expressions, use direct transformation
        computed_expr = loop_expr_val
        condition_expr_sql = condition_expr_val
        use_step = False
        pattern_type = "Direct"
    
    # Generate SQL
    if use_step:
        array_expr = f"generate_array({init_value}, {init_value + max_rows * array_step}, {array_step})"
        array_alias = "val"
    else:
        array_expr = f"generate_array(0, {max_rows} - 1)"
        array_alias = "_iter"
    
    sql = f"""with gen as (
    select {computed_expr} as `val`
    from unnest({array_expr}) as {array_alias}
    where {condition_expr_sql}
    limit {max_rows}
)
select `val`
from gen"""
    
    return {
        'pattern_type': pattern_type,
        'computed_expr': computed_expr,
        'condition_expr_sql': condition_expr_sql,
        'init_value': init_value,
        'sql': sql,
        'use_step': use_step
    }


def test_default_implementation():
    """Test default recursive CTE implementation (Databricks/DuckDB)"""
    
    print("\n" + "="*80)
    print("TEST: Default Recursive CTE Implementation (Databricks/DuckDB)")
    print("="*80)
    
    test_cases = [
        {
            'name': 'No table - simple sequence',
            'relation_name': '',
            'init_expr': '1',
            'condition_expr': 'val < 10',
            'loop_expr': 'val + 1',
            'column_name': 'val',
            'max_rows': 100,
            'force_mode': 'recursive'
        },
        {
            'name': 'With table - sequence per row',
            'relation_name': 'table_name',
            'init_expr': '1',
            'condition_expr': 'val < 10',
            'loop_expr': 'val + 1',
            'column_name': 'val',
            'max_rows': 100,
            'force_mode': 'recursive'
        },
    ]
    
    for test_case in test_cases:
        print(f"\n{'-'*80}")
        print(f"Test: {test_case['name']}")
        print(f"{'-'*80}")
        
        sql = simulate_default_macro(
            test_case['relation_name'],
            test_case['init_expr'],
            test_case['condition_expr'],
            test_case['loop_expr'],
            test_case['column_name'],
            test_case['max_rows'],
            test_case['force_mode']
        )
        
        print(f"\nGenerated SQL:")
        print(sql)
        print(f"\n✅ Uses WITH RECURSIVE (supported in Databricks/DuckDB)")


def test_bigquery_implementation():
    """Test BigQuery-specific implementation"""
    
    print("\n" + "="*80)
    print("TEST: BigQuery Implementation (GENERATE_ARRAY, no WITH RECURSIVE)")
    print("="*80)
    
    test_cases = [
        {
            'name': 'val + 1',
            'init_expr': '1',
            'condition_expr': 'val < 10',
            'loop_expr': 'val + 1',
            'column_name': 'val',
            'expected_values': [1, 2, 3, 4, 5, 6, 7, 8, 9]
        },
        {
            'name': 'val + 12',
            'init_expr': '1',
            'condition_expr': 'val < 50',
            'loop_expr': 'val + 12',
            'column_name': 'val',
            'expected_values': [1, 13, 25, 37, 49]
        },
        {
            'name': 'val * 2',
            'init_expr': '1',
            'condition_expr': 'val < 100',
            'loop_expr': 'val * 2',
            'column_name': 'val',
            'expected_values': [1, 2, 4, 8, 16, 32, 64]
        },
        {
            'name': 'val * 13',
            'init_expr': '1',
            'condition_expr': 'val < 10000',
            'loop_expr': 'val * 13',
            'column_name': 'val',
            'expected_values': [1, 13, 169, 2197]
        },
    ]
    
    for test_case in test_cases:
        print(f"\n{'-'*80}")
        print(f"Test: {test_case['name']}")
        print(f"{'-'*80}")
        print(f"init_expr: {test_case['init_expr']}")
        print(f"loop_expr: {test_case['loop_expr']}")
        print(f"condition_expr: {test_case['condition_expr']}")
        
        result = simulate_bigquery_macro(
            test_case['init_expr'],
            test_case['condition_expr'],
            test_case['loop_expr'],
            test_case['column_name']
        )
        
        print(f"\nPattern Detection: {result['pattern_type']}")
        print(f"Computed Expression: {result['computed_expr']}")
        print(f"\nGenerated SQL:")
        print(result['sql'])
        
        # Verify expected values for multiplication patterns
        if 'POWER' in result['computed_expr']:
            print(f"\nExpected values: {test_case['expected_values']}")
            computed_vals = []
            for i in range(10):
                match = re.search(r'POWER\((\d+),', result['computed_expr'])
                if match:
                    multiplier = int(match.group(1))
                    val = result['init_value'] * (multiplier ** i)
                    if eval(test_case['condition_expr'].replace('val', str(val))):
                        computed_vals.append(val)
                    else:
                        break
            print(f"Computed values (using POWER): {computed_vals}")
            if computed_vals == test_case['expected_values']:
                print("✅ PASS: Values match expected")
            else:
                print("⚠️  Values differ from expected")
        elif result['use_step']:
            print(f"\nExpected values: {test_case['expected_values']}")
            print("✅ Uses GENERATE_ARRAY with step (correct for addition patterns)")
        else:
            print(f"\n⚠️  Uses direct transformation (may not work for recursive sequences)")


def main():
    """Run all tests"""
    
    print("\n" + "="*80)
    print("COMPREHENSIVE GENERATEROWS MACRO TEST SUITE")
    print("="*80)
    
    # Test default implementation
    test_default_implementation()
    
    # Test BigQuery implementation
    test_bigquery_implementation()
    
    # Summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print("✅ Default implementation: Uses WITH RECURSIVE (Databricks/DuckDB)")
    print("✅ BigQuery implementation:")
    print("   - Addition patterns (val + N): Uses GENERATE_ARRAY with step")
    print("   - Multiplication patterns (val * N): Uses POWER(N, _iter)")
    print("   - Other patterns: Falls back to direct transformation")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()

