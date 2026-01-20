# armv10\scripts\validate_tiles_fixed.py

> Auto-generated summary. Improve this page with architecture notes, examples and references.

## Module summary

Validate dashboard tile values against an ADF Analyzer Excel workbook.

Usage:
    python validate_tiles_fixed.py path/to/analysis.xlsx [--dump] [--top N]

This script loads the workbook, applies the same heuristics used by the dashboard
and prints each tile name, the raw source used and the computed value in a
human-readable and JSON summary form.

The script intentionally mirrors the dashboard logic (fallbacks, Summary coercion,
lineage table heuristics) so you can run it locally and verify what the dashboard
will display.

## Functions

- **load_workbook()** — 
- **normalize_key()** — 
- **coerce_summary_values()** — 
- **get_summary_metric()** — 
- **get_count_with_fallback()** — 
- **aggregate_unique()** — 
- **extract_values_counter()** — Return a Counter of values found across candidate columns in given dfs.
- **is_dynamic_value()** — Heuristic to detect dynamic/parameterized expressions in a value.
- **dump_top_values()** — Return list of (value, count, is_dynamic) for top_n items.
- **main()** — 

## Notes

Add usage, examples, cross-references, data shapes, and important edge cases here.
