# test_metrics_enhanced.py

> Auto-generated description — please expand with architecture notes, data models, and examples.

## Overview

Enhanced test to check SourceTable and SinkTable columns for file/table patterns

## Contract (heuristic)

- Inputs: likely Azure Data Factory JSON templates, analyzer workbook, or pipeline metadata (see module usage).
- Outputs: reports (Excel/HTML/JSON), dashboard tiles, or transformed datasets.
- Error modes: missing workbook, malformed pipeline definitions, unsupported activities.

## Top-level variables

- excel_file

## Source preview (first 20 lines)

```"""
Enhanced test to check SourceTable and SinkTable columns for file/table patterns
"""
import pandas as pd

excel_file = "streamlit_app/data/adf_analysis_latest.xlsx"

print("=" * 80)
print("Enhanced Source/Target Analysis - Checking Table Columns")
print("=" * 80)

try:
    df = pd.read_excel(excel_file, sheet_name="DataLineage")
    
    print(f"\n📊 DataLineage Sheet: {len(df)} records")
    
    # Analyze SourceTable column
    if "SourceTable" in df.columns:
        source_tables = df["SourceTable"].dropna().unique()
        print(f"\n📥 SourceTable Column Analysis:")
```

## Notes

Add: examples, how to run, expected input file names (e.g., `output/adf_analysis_latest.xlsx`), related modules, and common failure modes.
