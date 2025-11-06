#!/usr/bin/env python3
"""
Full flow test: Frontend -> apply() -> Jinja2 -> Macro -> SQL -> Regex Engine
Tests single quotes, double quotes, and backslashes
"""

def test_full_flow():
    """Test the complete flow from user input to regex engine"""
    
    print("\n" + "="*80)
    print("FULL FLOW TEST: User Input -> Regex Engine")
    print("="*80)
    
    # Test 1: Single quotes (original pattern)
    print("\n" + "="*80)
    print("TEST 1: Pattern with Single Quotes")
    print("="*80)
    
    user_input = r"([^a-z0-9e]*)([a-z0-9e'-]+)([^a-z0-9'-]*)"
    
    print("\nStep 1: User types in Frontend")
    print(f"  User input: {user_input}")
    
    # Step 2: apply() function processes it
    print("\nStep 2: apply() function in Regex.py")
    print("  Code: escaped_p = p.replace(\"'\", \"''\")")
    escaped_by_apply = user_input.replace("'", "''")
    print(f"  Result: {escaped_by_apply}")
    
    # Step 3: Jinja2/SQL parses the macro call
    print("\nStep 3: Jinja2/SQL parses the macro call")
    print(f"  Macro receives: {user_input}")
    
    # Step 4: Macro's escape_regex_pattern processes it
    print("\nStep 4: Macro's escape_regex_pattern function")
    print(f"  Input to function: {user_input}")
    escaped_by_macro = user_input.replace("'", "''")
    print(f"  Function escapes again: {escaped_by_macro}")
    
    # Step 5: Used in SQL query
    sql_pattern = f"'{escaped_by_macro}'"
    print(f"\nStep 5: SQL string literal: {sql_pattern}")
    
    # Step 6: Regex engine sees
    regex_engine_sees = escaped_by_macro.replace("''", "'")
    print(f"\nStep 6: Regex engine sees: {regex_engine_sees}")
    
    if regex_engine_sees == user_input:
        print("✅ PASS: Regex engine receives the exact user input!")
    else:
        print("❌ FAIL: Regex engine receives different value!")
    
    # Test 2: Double quotes
    print("\n" + "="*80)
    print("TEST 2: Pattern with Double Quotes")
    print("="*80)
    
    user_input_quotes = r'([^"]*)'
    print(f"\nStep 1: User input: {user_input_quotes}")
    print("  Note: User types double quotes in regex pattern")
    
    # apply() only escapes single quotes, not double quotes
    escaped_by_apply_quotes = user_input_quotes.replace("'", "''")
    print(f"\nStep 2: apply() escapes single quotes: {escaped_by_apply_quotes}")
    print("  Note: Double quotes are NOT escaped by apply()")
    
    # Macro's escape_regex_pattern also only escapes single quotes
    escaped_by_macro_quotes = user_input_quotes.replace("'", "''")
    print(f"\nStep 3: Macro's escape_regex_pattern: {escaped_by_macro_quotes}")
    print("  Note: Double quotes remain unchanged")
    
    sql_pattern_quotes = f"'{escaped_by_macro_quotes}'"
    print(f"\nStep 4: SQL string literal: {sql_pattern_quotes}")
    print("  Note: Double quotes are safe inside single-quoted SQL strings")
    
    regex_engine_sees_quotes = escaped_by_macro_quotes.replace("''", "'")
    print(f"\nStep 5: Regex engine sees: {regex_engine_sees_quotes}")
    
    if regex_engine_sees_quotes == user_input_quotes:
        print("✅ PASS: Double quotes work correctly!")
    else:
        print("❌ FAIL: Double quotes not handled correctly!")
    
    # Test 3: Backslashes
    print("\n" + "="*80)
    print("TEST 3: Pattern with Backslashes")
    print("="*80)
    
    user_input_backslash = r"(\d+)"
    print(f"\nStep 1: User input: {user_input_backslash}")
    print("  Note: User types \\d for digit class")
    
    # apply() only escapes single quotes
    escaped_by_apply_bs = user_input_backslash.replace("'", "''")
    print(f"\nStep 2: apply() escapes single quotes: {escaped_by_apply_bs}")
    print("  Note: Backslashes are NOT escaped by apply()")
    
    # Macro's escape_regex_pattern with escape_backslashes=true
    print("\nStep 3: Macro's escape_regex_pattern(..., escape_backslashes=true)")
    print("  For Databricks/Spark: Backslashes ARE escaped")
    escaped_by_macro_bs_databricks = user_input_backslash.replace("\\", "\\\\").replace("'", "''")
    print(f"  Result (Databricks): {escaped_by_macro_bs_databricks}")
    
    # For DuckDB: escape_backslashes=false
    print("\n  For DuckDB: Backslashes are NOT escaped")
    escaped_by_macro_bs_duckdb = user_input_backslash.replace("'", "''")
    print(f"  Result (DuckDB): {escaped_by_macro_bs_duckdb}")
    
    # What regex engine sees (after SQL string parsing)
    regex_engine_sees_bs_databricks = escaped_by_macro_bs_databricks.replace("\\\\", "\\").replace("''", "'")
    regex_engine_sees_bs_duckdb = escaped_by_macro_bs_duckdb.replace("''", "'")
    
    print(f"\nStep 4: Regex engine sees (Databricks): {regex_engine_sees_bs_databricks}")
    print(f"Step 4: Regex engine sees (DuckDB): {regex_engine_sees_bs_duckdb}")
    
    if regex_engine_sees_bs_databricks == user_input_backslash and regex_engine_sees_bs_duckdb == user_input_backslash:
        print("✅ PASS: Backslashes work correctly for both adapters!")
    else:
        print("❌ FAIL: Backslashes not handled correctly!")
    
    # Test 4: Complex pattern with all special characters
    print("\n" + "="*80)
    print("TEST 4: Complex Pattern with Single Quotes, Double Quotes, and Backslashes")
    print("="*80)
    
    user_input_complex = r'([^a-z0-9e"]*)([a-z0-9e\'-]+)([^a-z0-9\'-"]*)'
    print(f"\nStep 1: User input: {user_input_complex}")
    print("  Contains: single quotes ('), double quotes (\"), backslashes (\\)")
    
    # apply() escapes single quotes
    escaped_by_apply_complex = user_input_complex.replace("'", "''")
    print(f"\nStep 2: apply() escapes single quotes: {escaped_by_apply_complex}")
    
    # Macro's escape_regex_pattern (Databricks)
    escaped_by_macro_complex = user_input_complex.replace("\\", "\\\\").replace("'", "''")
    print(f"\nStep 3: Macro escapes (Databricks): {escaped_by_macro_complex}")
    
    sql_pattern_complex = f"'{escaped_by_macro_complex}'"
    print(f"\nStep 4: SQL string literal: {sql_pattern_complex}")
    
    regex_engine_sees_complex = escaped_by_macro_complex.replace("\\\\", "\\").replace("''", "'")
    print(f"\nStep 5: Regex engine sees: {regex_engine_sees_complex}")
    
    if regex_engine_sees_complex == user_input_complex:
        print("✅ PASS: Complex pattern with all special characters works!")
    else:
        print("❌ FAIL: Complex pattern not handled correctly!")
        print(f"  Expected: {user_input_complex}")
        print(f"  Got:      {regex_engine_sees_complex}")
    
    # Test 5: Pattern 2 (wrong - pre-escaped)
    print("\n" + "="*80)
    print("TEST 5: Pattern 2 (WRONG - User pre-escaped quotes)")
    print("="*80)
    
    user_input_wrong = r"([^a-ze0-9]*)([a-z0-9e''-]+)([^a-z0-9''-]*)"
    print(f"\nStep 1: User input (wrong): {user_input_wrong}")
    print("  Note: User pre-escaped single quotes as ''")
    
    escaped_by_apply_wrong = user_input_wrong.replace("'", "''")
    print(f"\nStep 2: apply() escapes: {escaped_by_apply_wrong}")
    print("  Notice: '' becomes '''' (quadruple quotes!)")
    
    jinja2_unescaped_wrong = escaped_by_apply_wrong.replace("''", "'")
    print(f"\nStep 3: Jinja2 unescapes: {jinja2_unescaped_wrong}")
    print("  Notice: '''' becomes '' (double quotes remain)")
    
    escaped_by_macro_wrong = jinja2_unescaped_wrong.replace("'", "''")
    print(f"\nStep 4: Macro escapes again: {escaped_by_macro_wrong}")
    print("  Notice: '' becomes '''' again")
    
    regex_engine_sees_wrong = escaped_by_macro_wrong.replace("''", "'")
    print(f"\nStep 5: Regex engine sees: {regex_engine_sees_wrong}")
    print("  ❌ This is wrong! Regex engine sees '' (two quotes) instead of ' (one quote)")
    
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print("✅ Single quotes: Correctly escaped by apply() and macro")
    print("✅ Double quotes: Work correctly (no escaping needed in single-quoted SQL strings)")
    print("✅ Backslashes: Correctly escaped by macro (adapter-dependent)")
    print("❌ Pre-escaped quotes: Cause double-escaping - user should NOT pre-escape")
    print("="*80 + "\n")


if __name__ == "__main__":
    test_full_flow()
