# test7db.py

> Auto-generated description — please expand with architecture notes, data models, and examples.

## Contract (heuristic)

- Inputs: likely Azure Data Factory JSON templates, analyzer workbook, or pipeline metadata (see module usage).
- Outputs: reports (Excel/HTML/JSON), dashboard tiles, or transformed datasets.
- Error modes: missing workbook, malformed pipeline definitions, unsupported activities.

## Top-level variables

- test_sql
- merge_tables

## Source preview (first 20 lines)

```from adf_analyzer_v10_complete import SQLParser

# Test SQL
test_sql = """
MERGE INTO dbo.Customers AS target
USING staging.CustomerUpdates AS source
ON target.CustomerId = source.CustomerId
WHEN MATCHED THEN UPDATE SET target.Name = source.Name
"""

print("Testing MERGE extraction directly...")
print("="*70)
print(f"SQL:\n{test_sql}\n")

# Test the parser directly
tables, columns = SQLParser.parse_sql(test_sql)

print(f"Tables extracted: {tables}")
print(f"Expected: ['dbo.Customers', 'staging.CustomerUpdates'] (or without schema)")

```

## Notes

Add: examples, how to run, expected input file names (e.g., `output/adf_analysis_latest.xlsx`), related modules, and common failure modes.
