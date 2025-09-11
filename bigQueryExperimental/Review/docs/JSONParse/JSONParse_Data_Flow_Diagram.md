# JSONParse Data Flow Diagram

## Overview
This diagram shows the complete data flow and processing pipeline for the JSONParse gem, from user input to generated BigQuery SQL.

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                                JSONParse Gem Data Flow                         │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   User Input    │    │   UI Dialog     │    │  Validation     │
│                 │    │                 │    │                 │
│ • Column Name   │───▶│ • Column Select │───▶│ • Column Check  │
│ • Sample JSON   │    │ • Method Choice │    │ • JSON Validate │
│ • Schema Def    │    │ • Input Fields  │    │ • Method Check  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                       │
                                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              onChange() Method                                  │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐             │
│  │ Get Relations   │    │ Generate SQL    │    │ Update State    │             │
│  │                 │    │                 │    │                 │             │
│  │ • Extract       │───▶│ • Parse JSON    │───▶│ • Relation Names│             │
│  │   upstream      │    │ • Analyze       │    │ • Generated SQL │             │
│  │   table names   │    │   structure     │    │ • Properties    │             │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘             │
└─────────────────────────────────────────────────────────────────────────────────┘
                                                       │
                                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                            SQL Generation Pipeline                             │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Parse Method   │    │  Sample Record  │    │  Schema Def     │
│   Decision      │    │   Processing    │    │   Processing    │
│                 │    │                 │    │                 │
│ • Sample Record │───▶│ • JSON.parse()  │    │ • Parse STRUCT  │
│ • Schema Def    │    │ • Type Analysis │    │ • Field Mapping │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          Recursive Structure Analysis                          │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐             │
│  │  Type Detection │    │  SQL Generation │    │  Nested Objects │             │
│  │                 │    │                 │    │                 │             │
│  │ • dict → STRUCT │───▶│ • JSON_EXTRACT  │───▶│ • Recursive     │             │
│  │ • list → ARRAY  │    │ • JSON_EXTRACT_ │    │   Processing    │             │
│  │ • prim → SCALAR │    │   SCALAR        │    │ • Path Building │             │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘             │
└─────────────────────────────────────────────────────────────────────────────────┘
                                                       │
                                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              Generated SQL                                     │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐             │
│  │  BigQuery SQL   │    │  Type Casting   │    │  STRUCT Output  │             │
│  │                 │    │                 │    │                 │             │
│  │ • Native        │───▶│ • CAST to       │───▶│ • Nested        │             │
│  │   Functions     │    │   BigQuery      │    │   Structure     │             │
│  │ • No UDFs       │    │   Types         │    │ • Field Names   │             │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘             │
└─────────────────────────────────────────────────────────────────────────────────┘
                                                       │
                                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              Macro Call Generation                             │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐             │
│  │  Parameter      │    │  String         │    ┌─────────────────┐             │
│  │  Assembly       │    │  Escaping       │    │  Final Macro    │             │
│  │                 │    │                 │    │  Call           │             │
│  │ • Table Names   │───▶│ • SQL Escaping  │───▶│ • {{ Macro() }} │             │
│  │ • Column Name   │    │ • Quote Handling│    │ • Parameters    │             │
│  │ • Generated SQL │    │ • Safety        │    │ • Ready for     │             │
│  └─────────────────┘    └─────────────────┘    │   Execution     │             │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Detailed Processing Steps

### 1. User Input Phase
- **Column Selection**: User selects JSON column from input schema
- **Method Choice**: User chooses between sample record or schema definition
- **Data Input**: User provides sample JSON or STRUCT schema

### 2. Validation Phase
- **Column Validation**: Ensures selected column exists in input schema
- **Method Validation**: Validates parsing method selection
- **Data Validation**: Validates JSON syntax or STRUCT format

### 3. SQL Generation Phase
- **Method Routing**: Routes to appropriate processing method
- **Sample Record Processing**:
  - Parse JSON string using `json.loads()`
  - Analyze structure recursively
  - Generate SQL based on inferred types
- **Schema Processing**:
  - Parse STRUCT definition
  - Map fields to JSON paths
  - Generate SQL with explicit types

### 4. Structure Analysis Phase
- **Type Detection**: Maps Python types to BigQuery types
- **Recursive Processing**: Handles nested objects and arrays
- **Path Building**: Constructs JSON paths for extraction

### 5. SQL Assembly Phase
- **Function Selection**: Chooses appropriate BigQuery JSON functions
- **Type Casting**: Applies CAST operations for type conversion
- **STRUCT Creation**: Builds nested STRUCT SQL

### 6. Macro Generation Phase
- **Parameter Assembly**: Combines all parameters
- **String Escaping**: Escapes SQL for safe parameter passing
- **Template Creation**: Generates final macro call

## Key Functions Used

### BigQuery JSON Functions
- `JSON_EXTRACT()`: For objects and arrays
- `JSON_EXTRACT_SCALAR()`: For scalar values
- `CAST()`: For type conversion
- `STRUCT()`: For nested structures

### Python Processing
- `json.loads()`: Parse JSON strings
- `isinstance()`: Type detection
- Recursive functions: Handle nested structures

## Error Handling Points

1. **JSON Parsing**: Malformed JSON in sample record
2. **Type Detection**: Unsupported data types
3. **SQL Generation**: Complex nested structures
4. **Parameter Escaping**: Special characters in data

## Performance Considerations

- **Native Functions**: Uses BigQuery's optimized JSON functions
- **No UDFs**: Eliminates deployment complexity
- **Columnar Processing**: Leverages BigQuery's columnar architecture
- **Query Optimization**: Generated SQL supports BigQuery optimization
