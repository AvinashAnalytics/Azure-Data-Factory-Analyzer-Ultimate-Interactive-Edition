# test_metrics.py

> Auto-generated description — please expand with architecture notes, data models, and examples.

## Overview

Test script to verify the new source/target metrics calculation

## Contract (heuristic)

- Inputs: likely Azure Data Factory JSON templates, analyzer workbook, or pipeline metadata (see module usage).
- Outputs: reports (Excel/HTML/JSON), dashboard tiles, or transformed datasets.
- Error modes: missing workbook, malformed pipeline definitions, unsupported activities.

## Top-level variables

- excel_file

## Source preview (first 20 lines)

```"""
Test script to verify the new source/target metrics calculation
"""
import pandas as pd
import openpyxl

# Load the test Excel file
excel_file = "streamlit_app/data/adf_analysis_latest.xlsx"

print("=" * 80)
print("Testing Source/Target Metrics Calculation")
print("=" * 80)

try:
    # Read DataLineage sheet
    df = pd.read_excel(excel_file, sheet_name="DataLineage")
    
    print(f"\n📊 DataLineage Sheet Loaded")
    print(f"   Total Records: {len(df)}")
    print(f"   Columns: {', '.join(df.columns.tolist())}")
```

## Notes

Add: examples, how to run, expected input file names (e.g., `output/adf_analysis_latest.xlsx`), related modules, and common failure modes.
