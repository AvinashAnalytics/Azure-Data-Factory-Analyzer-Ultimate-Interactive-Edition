# analyze_lineage_data.py

> Auto-generated description — please expand with architecture notes, data models, and examples.

## Overview

Analyze DataLineage to understand why file detection is returning 0

## Contract (heuristic)

- Inputs: likely Azure Data Factory JSON templates, analyzer workbook, or pipeline metadata (see module usage).
- Outputs: reports (Excel/HTML/JSON), dashboard tiles, or transformed datasets.
- Error modes: missing workbook, malformed pipeline definitions, unsupported activities.

## Top-level variables

- excel_file
- df

## Source preview (first 20 lines)

```"""
Analyze DataLineage to understand why file detection is returning 0
"""
import pandas as pd

excel_file = "streamlit_app/data/adf_analysis_latest.xlsx"

print("=" * 80)
print("DEEP DIVE: Why are file counts zero?")
print("=" * 80)

df = pd.read_excel(excel_file, sheet_name="DataLineage")

print(f"\n📊 Total Records: {len(df)}")
print(f"📋 Columns: {', '.join(df.columns.tolist())}")

# Examine Source column
print("\n" + "=" * 80)
print("SOURCE COLUMN ANALYSIS")
print("=" * 80)
```

## Notes

Add: examples, how to run, expected input file names (e.g., `output/adf_analysis_latest.xlsx`), related modules, and common failure modes.
