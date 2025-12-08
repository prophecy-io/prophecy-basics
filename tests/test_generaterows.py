#!/usr/bin/env python3
"""
Test script to render GenerateRows macro and show the generated SQL
"""
import sys
import os

# Add the project root to path if needed
sys.path.insert(0, os.path.dirname(__file__))

# Test the macro logic manually
def test_macro(relation_name, init_expr, condition_expr, loop_expr, column_name, max_rows, force_mode):
    print(f"\n{'='*80}")
    print(f"Testing: GenerateRows('{relation_name}', '{init_expr}', '{condition_expr}', '{loop_expr}', '{column_name}', {max_rows}, '{force_mode}')")
    print(f"{'='*80}\n")
    
    # Simulate the macro logic
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
    
    print(f"Variables:")
    print(f"  relation_tables: '{relation_tables}'")
    print(f"  internal_col: {internal_col}")
    print(f"  init_select: {init_select}")
    print(f"  condition_expr_sql: {condition_expr_sql}")
    print(f"  loop_expr_replaced: {loop_expr_replaced}")
    print(f"  recursion_condition: {recursion_condition}")
    print(f"\nGenerated SQL:\n")
    
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
    
    print(sql)
    return sql

if __name__ == "__main__":
    # Test case 1: No table
    test_macro('', '1', 'val < 10', 'val + 1', 'val', 100, 'recursive')
    
    # Test case 2: With table
    test_macro('table_name', '1', 'val < 10', 'val + 1', 'val', 100, 'recursive')

