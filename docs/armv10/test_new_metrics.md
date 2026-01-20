# test_new_metrics.py

> Auto-generated description — please expand with architecture notes, data models, and examples.

## Overview

Test the new dashboard metrics logic

## Contract (heuristic)

- Inputs: likely Azure Data Factory JSON templates, analyzer workbook, or pipeline metadata (see module usage).
- Outputs: reports (Excel/HTML/JSON), dashboard tiles, or transformed datasets.
- Error modes: missing workbook, malformed pipeline definitions, unsupported activities.

## Top-level variables

- excel_file
- df
- total_source_datasets
- total_target_datasets
- total_source_static
- total_target_static
- total_source_dynamic
- total_target_dynamic

## Source preview (first 20 lines)

```"""
Test the new dashboard metrics logic
"""
import pandas as pd

excel_file = "streamlit_app/data/adf_analysis_latest.xlsx"

print("=" * 80)
print("Testing Updated Dashboard Metrics")
print("=" * 80)

df = pd.read_excel(excel_file, sheet_name="DataLineage")

total_source_datasets = 0
total_target_datasets = 0
total_source_static = 0
total_target_static = 0
total_source_dynamic = 0
total_target_dynamic = 0

```

## Notes

Add: examples, how to run, expected input file names (e.g., `output/adf_analysis_latest.xlsx`), related modules, and common failure modes.
