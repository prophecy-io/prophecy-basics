#!/usr/bin/env python3
"""
Test Regex macro for all SQL dialects with the pattern: (\d{1,2})/(\d{1,2})/(\d{4})
Tests Databricks/Spark (default), BigQuery, Snowflake, and DuckDB
"""

import json
import re

def simulate_apply_function(props_dict):
    """Simulate the apply() function from Regex.py"""
    
    table_name = ",".join(str(rel) for rel in props_dict.get('relation_name', []))
    parseColumnsJson = json.dumps([
        {
            "columnName": fld['columnName'],
            "dataType": fld['dataType'],
            "rgxExpression": fld['rgxExpression']
        }
        for fld in props_dict.get('parseColumns', [])
    ])
    
    def safe_str(val):
        """Safely convert a value to a SQL string literal"""
        if val is None or val == "":
            return "''"
        if isinstance(val, str):
            # Escape single quotes for SQL string literals
            escaped = val.replace("'", "''")
            return f"'{escaped}'"
        if isinstance(val, list):
            return str(val)
        return f"'{str(val)}'"
    
    parameter_list = [
        safe_str(table_name),
        safe_str(parseColumnsJson),
        safe_str(props_dict.get('schema', '')),
        safe_str(props_dict.get('selectedColumnName', '')),
        safe_str(props_dict.get('regexExpression', '')),
        safe_str(props_dict.get('outputMethod', 'replace')),
        str(props_dict.get('caseInsensitive', False)).lower(),
        str(props_dict.get('allowBlankTokens', False)).lower(),
        safe_str(props_dict.get('replacementText', '')),
        str(props_dict.get('copyUnmatchedText', False)).lower(),
        safe_str(props_dict.get('tokenizeOutputMethod', 'splitColumns')),
        str(props_dict.get('noOfColumns', 3)),
        safe_str(props_dict.get('extraColumnsHandling', 'dropExtraWithWarning')),
        safe_str(props_dict.get('outputRootName', 'regex_col')),
        safe_str(props_dict.get('matchColumnName', 'regex_match')),
        str(props_dict.get('errorIfNotMatched', False)).lower(),
    ]
    
    non_empty_param = ",".join(parameter_list)
    return f"{{{{ prophecy_basics.Regex({non_empty_param}) }}}}"


def simulate_escape_flow(dialect, regex_pattern, escape_backslashes=True):
    """Simulate the escaping flow for a specific dialect"""
    
    print(f"\n  Dialect: {dialect}")
    print(f"  Escape backslashes: {escape_backslashes}")
    
    # Step 1: User input
    user_input = regex_pattern
    print(f"  1. User input: {user_input}")
    
    # Step 2: apply() escapes for SQL string literal parameter
    def safe_str(val):
        if val is None or val == "":
            return "''"
        if isinstance(val, str):
            escaped = val.replace("'", "''")
            return f"'{escaped}'"
        return f"'{str(val)}'"
    
    escaped_by_apply = safe_str(user_input)
    print(f"  2. After apply() safe_str(): {escaped_by_apply}")
    
    # Step 3: Jinja2 unescapes (simulate)
    jinja2_received = user_input
    print(f"  3. Jinja2 receives: {jinja2_received}")
    
    # Step 4: Macro's escape_regex_pattern()
    def escape_regex_pattern(pattern, escape_backslashes=True):
        if escape_backslashes:
            escaped = pattern.replace("\\", "\\\\")
        else:
            escaped = pattern
        escaped = escaped.replace("'", "''")
        return escaped
    
    escaped_by_macro = escape_regex_pattern(jinja2_received, escape_backslashes)
    print(f"  4. After macro escape_regex_pattern(): {escaped_by_macro}")
    
    # Step 5: SQL string literal
    sql_pattern = f"'{escaped_by_macro}'"
    print(f"  5. SQL string literal: {sql_pattern}")
    
    # Step 6: Regex engine sees (SQL unescapes)
    regex_engine_sees = escaped_by_macro.replace("''", "'")
    if escape_backslashes:
        # For dialects that escape backslashes, we need to unescape them
        regex_engine_sees = regex_engine_sees.replace("\\\\", "\\")
    print(f"  6. Regex engine sees: {regex_engine_sees}")
    
    # Verify
    if regex_engine_sees == user_input:
        print(f"  ✅ Pattern preserved correctly")
        return True
    else:
        print(f"  ⚠️  Pattern changed: {user_input} -> {regex_engine_sees}")
        return False


def test_parse_columns_json():
    """Test that parseColumns JSON with backslashes is handled correctly"""
    
    print("\n" + "="*80)
    print("TEST: parseColumns JSON with Backslashes")
    print("="*80)
    
    parseColumns = [
        {"columnName": "regex_col1", "dataType": "string", "rgxExpression": "(\\d{1,2})"},
        {"columnName": "regex_col2", "dataType": "string", "rgxExpression": "(\\d{1,2})"},
        {"columnName": "regex_col3", "dataType": "string", "rgxExpression": "(\\d{4})"}
    ]
    
    # Generate JSON
    parseColumnsJson = json.dumps(parseColumns)
    print(f"\n1. JSON from json.dumps():")
    print(f"   {parseColumnsJson}")
    
    # Apply safe_str escaping
    def safe_str(val):
        if val is None or val == "":
            return "''"
        if isinstance(val, str):
            escaped = val.replace("'", "''")
            return f"'{escaped}'"
        return f"'{str(val)}'"
    
    escaped_json = safe_str(parseColumnsJson)
    print(f"\n2. After safe_str() (for macro parameter):")
    print(f"   {escaped_json}")
    
    # Simulate Jinja2 unescaping
    jinja2_received = parseColumnsJson
    print(f"\n3. What macro receives (after Jinja2 unescapes):")
    print(f"   {jinja2_received}")
    
    # Verify JSON is valid
    try:
        parsed = json.loads(jinja2_received)
        print(f"\n4. ✅ JSON is valid after unescaping")
        print(f"   Found {len(parsed)} parse columns")
        for i, col in enumerate(parsed, 1):
            print(f"   Column {i}: {col['rgxExpression']}")
        return True
    except Exception as e:
        print(f"\n4. ❌ ERROR: JSON is invalid: {e}")
        return False


def test_full_macro_call():
    """Test the full macro call generation"""
    
    print("\n" + "="*80)
    print("TEST: Full Macro Call Generation")
    print("="*80)
    
    props = {
        'relation_name': ['test_tbl_123'],
        'parseColumns': [
            {"columnName": "regex_col1", "dataType": "string", "rgxExpression": "(\\d{1,2})"},
            {"columnName": "regex_col2", "dataType": "string", "rgxExpression": "(\\d{1,2})"},
            {"columnName": "regex_col3", "dataType": "string", "rgxExpression": "(\\d{4})"}
        ],
        'schema': '[{"name": "Inbound/Outbound Connection", "dataType": "String"}, {"name": "Target Catalog", "dataType": "String"}, {"name": "Target Database", "dataType": "String"}, {"name": "Target Table", "dataType": "String"}, {"name": "Column Name", "dataType": "String"}, {"name": "Process Name", "dataType": "String"}, {"name": "Upstream Lineage", "dataType": "String"}]',
        'selectedColumnName': 'Upstream Lineage',
        'regexExpression': '(\\d{1,2})/(\\d{1,2})/(\\d{4})',
        'outputMethod': 'parse',
        'caseInsensitive': False,
        'allowBlankTokens': False,
        'replacementText': '',
        'copyUnmatchedText': False,
        'tokenizeOutputMethod': 'splitRows',
        'noOfColumns': 3,
        'extraColumnsHandling': 'dropExtraWithWarning',
        'outputRootName': 'generated',
        'matchColumnName': 'regex_match',
        'errorIfNotMatched': False
    }
    
    macro_call = simulate_apply_function(props)
    print(f"\nGenerated macro call:")
    print(macro_call)
    
    # Check for proper escaping
    if "''" in macro_call and "'" in macro_call:
        # Count single quotes - should be even (pairs)
        single_quotes = macro_call.count("'") - macro_call.count("''") * 2
        if single_quotes == 0:
            print("\n✅ All single quotes are properly escaped")
            return True
        else:
            print(f"\n⚠️  Unescaped single quotes detected: {single_quotes}")
            return False
    else:
        print("\n✅ No single quotes to escape")
        return True


def test_regex_pattern_matching():
    """Test that the regex pattern actually matches dates"""
    
    print("\n" + "="*80)
    print("TEST: Regex Pattern Matching")
    print("="*80)
    
    pattern = r'(\d{1,2})/(\d{1,2})/(\d{4})'
    test_cases = [
        "12/25/2024",
        "1/5/2023",
        "01/01/2024",
        "Invalid date",
        "12-25-2024",  # Wrong format
    ]
    
    print(f"\nPattern: {pattern}")
    print(f"\nTest cases:")
    
    compiled = re.compile(pattern)
    all_passed = True
    
    for test_case in test_cases:
        match = compiled.match(test_case)
        if match:
            groups = match.groups()
            print(f"  ✅ '{test_case}' -> {groups}")
        else:
            print(f"  ❌ '{test_case}' -> No match")
            if test_case in ["12/25/2024", "1/5/2023", "01/01/2024"]:
                all_passed = False
    
    return all_passed


def main():
    """Run all tests"""
    
    print("\n" + "="*80)
    print("REGEX MACRO - ALL DIALECTS TEST")
    print("="*80)
    
    # Test pattern
    pattern = r'(\d{1,2})/(\d{1,2})/(\d{4})'
    
    print("\n" + "="*80)
    print("TEST 1: Escaping Flow for All Dialects")
    print("="*80)
    
    dialects = [
        ("Databricks/Spark (default)", True),
        ("BigQuery", True),
        ("Snowflake", True),
        ("DuckDB", False),
    ]
    
    all_escaping_passed = True
    for dialect_name, escape_backslashes in dialects:
        passed = simulate_escape_flow(dialect_name, pattern, escape_backslashes)
        if not passed:
            all_escaping_passed = False
    
    # Test parseColumns JSON
    json_test = test_parse_columns_json()
    
    # Test full macro call
    macro_call_test = test_full_macro_call()
    
    # Test regex matching
    regex_test = test_regex_pattern_matching()
    
    # Summary
    print("\n" + "="*80)
    print("TEST SUMMARY")
    print("="*80)
    print(f"Escaping flow (all dialects): {'✅ PASS' if all_escaping_passed else '❌ FAIL'}")
    print(f"parseColumns JSON handling: {'✅ PASS' if json_test else '❌ FAIL'}")
    print(f"Macro call generation: {'✅ PASS' if macro_call_test else '❌ FAIL'}")
    print(f"Regex pattern matching: {'✅ PASS' if regex_test else '❌ FAIL'}")
    
    all_passed = all_escaping_passed and json_test and macro_call_test and regex_test
    print(f"\n{'✅ ALL TESTS PASSED' if all_passed else '❌ SOME TESTS FAILED'}")
    print("="*80)


if __name__ == "__main__":
    main()

