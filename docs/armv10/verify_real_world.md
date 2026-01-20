# verify_real_world.py

> Auto-generated description — please expand with architecture notes, data models, and examples.

## Overview

Comprehensive verification using real-world ADF template
Run the analyzer, then verify all dashboard metrics

## Contract (heuristic)

- Inputs: likely Azure Data Factory JSON templates, analyzer workbook, or pipeline metadata (see module usage).
- Outputs: reports (Excel/HTML/JSON), dashboard tiles, or transformed datasets.
- Error modes: missing workbook, malformed pipeline definitions, unsupported activities.

## Top-level variables

- template_file
- excel_file

## Source preview (first 20 lines)

```"""
Comprehensive verification using real-world ADF template
Run the analyzer, then verify all dashboard metrics
"""
import sys
import os
import pandas as pd
from datetime import datetime

print("=" * 80)
print("COMPREHENSIVE REAL-WORLD VERIFICATION")
print("=" * 80)
print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

# Step 1: Run analyzer on the real template
print("\n" + "=" * 80)
print("STEP 1: Analyzing Real ADF Template")
print("=" * 80)

```

## Notes

Add: examples, how to run, expected input file names (e.g., `output/adf_analysis_latest.xlsx`), related modules, and common failure modes.
