# armv10\scripts\check_dashboard_tiles.py

> Auto-generated summary. Improve this page with architecture notes, examples and references.

## Module summary

Small verifier for dashboard tiles.
Reads the analyzer workbook and prints:
 - Impact severity counts (CRITICAL/HIGH/MEDIUM/LOW)
 - Orphaned counts (pipelines, datasets, linked services)
 - Broken/inactive triggers (sheet/orphan counts)
 - DataLineage: total records, unique sources, unique sinks, copy activity count

Usage: run with the workspace venv python

## Functions

- **normalize()** — 
- **count_sheet()** — 

## Notes

Add usage, examples, cross-references, data shapes, and important edge cases here.
