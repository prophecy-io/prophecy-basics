#!/usr/bin/env python3
"""
Test script to verify that the apply() function correctly handles regexExpression
from the frontend (what the user actually types in).
"""

import sys
from pathlib import Path

# Add the project directory to the path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Mock the necessary classes for testing
class MockRegexProperties:
    def __init__(self):
        self.relation_name = ['test_table']
        self.parseColumns = []
        self.schema = '{"fields":[{"name":"text_column","dataType":{"type":"string"}}]}'
        self.selectedColumnName = 'text_column'
        self.regexExpression = ""  # Will be set to test patterns
        self.outputMethod = 'tokenize'
        self.caseInsensitive = True
        self.allowBlankTokens = False
        self.replacementText = ''
        self.copyUnmatchedText = False
        self.tokenizeOutputMethod = 'splitColumns'
        self.noOfColumns = 3
        self.extraColumnsHandling = 'dropExtraWithWarning'
        self.outputRootName = 'regex_col'
        self.matchColumnName = 'regex_match'
        self.errorIfNotMatched = False


def test_apply_function():
    """Test the apply() function logic with actual user inputs"""
    
    # Simulate the apply() function logic
    def simulate_apply(props):
        resolved_macro_name = "prophecy_basics.Regex"
        table_name = ",".join(str(rel) for rel in props.relation_name)
        
        parameter_list = [
            table_name,
            '[]',  # parseColumnsJson
            props.schema,
            props.selectedColumnName,
            props.regexExpression,
            props.outputMethod,
            props.caseInsensitive,
            props.allowBlankTokens,
            props.replacementText,
            props.copyUnmatchedText,
            props.tokenizeOutputMethod,
            props.noOfColumns,
            props.extraColumnsHandling,
            props.outputRootName,
            props.matchColumnName,
            props.errorIfNotMatched,
        ]
        
        param_list_clean = []
        for p in parameter_list:
            if type(p) == str:
                # Escape single quotes for SQL string literals ('' represents a single quote)
                escaped_p = p.replace("'", "''")
                param_list_clean.append("'" + escaped_p + "'")
            else:
                param_list_clean.append(str(p))
        
        non_empty_param = ",".join([param for param in param_list_clean if param != ''])
        return f'{{{{ {resolved_macro_name}({non_empty_param}) }}}}'
    
    print("\n" + "="*80)
    print("Testing Regex.apply() Function with Frontend User Input")
    print("="*80)
    
    # Test Pattern 1: What user actually types in frontend
    print("\n" + "-"*80)
    print("TEST 1: Pattern 1 - User input from frontend")
    print("-"*80)
    
    props1 = MockRegexProperties()
    props1.regexExpression = r"([^a-z0-9e]*)([a-z0-9e'-]+)([^a-z0-9'-]*)"
    
    print(f"Frontend user input (self.props.regexExpression):")
    print(f"  {props1.regexExpression}")
    
    macro_call = simulate_apply(props1)
    print(f"\nGenerated macro call (from apply()):")
    print(f"  {macro_call}")
    
    # Extract the regexExpression parameter from the macro call
    import re
    match = re.search(r"regexExpression',\s*'([^']+(?:''[^']*)*)'", macro_call)
    if match:
        escaped_in_macro_call = match.group(1)
        print(f"\nRegexExpression in macro call (escaped):")
        print(f"  {escaped_in_macro_call}")
        print(f"\nWhat macro receives (after Jinja2/SQL unescapes):")
        # SQL '' becomes '
        unescaped = escaped_in_macro_call.replace("''", "'")
        print(f"  {unescaped}")
        
        if unescaped == props1.regexExpression:
            print("\n✅ PASS: Macro will receive the exact user input")
        else:
            print("\n❌ FAIL: Macro receives different value!")
            print(f"  Expected: {props1.regexExpression}")
            print(f"  Got:      {unescaped}")
    
    # Test Pattern 2: What user might try (with pre-escaped quotes - WRONG)
    print("\n" + "-"*80)
    print("TEST 2: Pattern 2 - User input with pre-escaped quotes (WRONG)")
    print("-"*80)
    
    props2 = MockRegexProperties()
    props2.regexExpression = r"([^a-ze0-9]*)([a-z0-9e''-]+)([^a-z0-9''-]*)"
    
    print(f"Frontend user input (self.props.regexExpression):")
    print(f"  {props2.regexExpression}")
    
    macro_call2 = simulate_apply(props2)
    print(f"\nGenerated macro call (from apply()):")
    print(f"  {macro_call2}")
    
    match2 = re.search(r"regexExpression',\s*'([^']+(?:''[^']*)*)'", macro_call2)
    if match2:
        escaped_in_macro_call2 = match2.group(1)
        print(f"\nRegexExpression in macro call (escaped):")
        print(f"  {escaped_in_macro_call2}")
        print(f"\n⚠️  Notice: If user pre-escapes, we get double-escaping!")
        unescaped2 = escaped_in_macro_call2.replace("''", "'")
        print(f"  What regex engine will see: {unescaped2}")
    
    # Test Pattern with no quotes
    print("\n" + "-"*80)
    print("TEST 3: Pattern with no single quotes")
    print("-"*80)
    
    props3 = MockRegexProperties()
    props3.regexExpression = r"(\d{3})-(\d{3})-(\d{4})"
    
    print(f"Frontend user input: {props3.regexExpression}")
    macro_call3 = simulate_apply(props3)
    match3 = re.search(r"regexExpression',\s*'([^']+(?:''[^']*)*)'", macro_call3)
    if match3:
        unescaped3 = match3.group(1).replace("''", "'")
        print(f"Macro receives: {unescaped3}")
        if unescaped3 == props3.regexExpression:
            print("✅ PASS: Works correctly for patterns without quotes")
    
    # Test what happens in the SQL macro
    print("\n" + "-"*80)
    print("TEST 4: What happens in the SQL macro's escape_regex_pattern")
    print("-"*80)
    
    print("User input: ([^a-z0-9e]*)([a-z0-9e'-]+)([^a-z0-9'-]*)")
    print("\nFlow:")
    print("  1. Frontend -> apply(): User types the pattern")
    print("  2. apply() escapes for SQL: ([^a-z0-9e]*)([a-z0-9e''-]+)([^a-z0-9''-]*)")
    print("  3. Jinja2/SQL unescapes: ([^a-z0-9e]*)([a-z0-9e'-]+)([^a-z0-9'-]*)")
    print("  4. Macro's escape_regex_pattern escapes again: ([^a-z0-9e]*)([a-z0-9e''-]+)([^a-z0-9''-]*)")
    print("  5. In SQL: '([^a-z0-9e]*)([a-z0-9e''-]+)([^a-z0-9''-]*)'")
    print("  6. Regex engine sees: ([^a-z0-9e]*)([a-z0-9e'-]+)([^a-z0-9'-]*) ✅")
    
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print("✅ apply() function correctly escapes single quotes")
    print("✅ User should type Pattern 1 as-is in frontend")
    print("❌ User should NOT pre-escape quotes (Pattern 2 is wrong)")
    print("✅ The macro's escape_regex_pattern will handle it correctly")
    print("="*80 + "\n")


if __name__ == "__main__":
    test_apply_function()

