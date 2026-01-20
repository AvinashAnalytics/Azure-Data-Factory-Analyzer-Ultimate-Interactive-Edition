# armv10\scripts\cross_verify_all.py

> Auto-generated summary. Improve this page with architecture notes, examples and references.

## Module summary

Cross-verify analyzer workbook metrics:
- Run canonical validator (scripts/validate_tiles.py) and capture its JSON summary
- Read the workbook and compute dashboard-style metrics (summary coercion + fallbacks)
- Compare validator vs summary vs computed values and write a report

Outputs:
 - output/cross_verify_report.json
 - output/cross_verify_report.txt

Usage: python scripts/cross_verify_all.py path/to/adf_analysis_latest.xlsx

## Functions

- **coerce_summary()** — 
- **get_dataframe_by_names()** — 
- **sum_numeric_columns_by_keywords()** — 
- **compute_counts()** — 

## Notes

Add usage, examples, cross-references, data shapes, and important edge cases here.
