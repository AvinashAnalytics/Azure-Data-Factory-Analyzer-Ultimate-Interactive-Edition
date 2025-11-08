# armv10 — Recent changes (Nov 07, 2025)

This short update summarizes the latest functional and UX changes in the `armv10/` workspace so you can quickly understand what's new.

## Key changes
- Streamlit dashboard (`adf_dashboard.py`):
  - Launcher (Run Analyzer vs Upload/Analyze) and safer runner controls.
  - Verify tiles button that produces persisted verification snapshots in `armv10/output/`.
  - Enhanced metric heuristics (robust fallbacks, sum-by-keyword helpers for DataFlows).
  - Runner integration (import runner when available, otherwise subprocess), run history and logs.
- Patched runner: `adf_analyzer_v10_patched_runner.py` applies patches and excel enhancements before running the analyzer and writes `armv10/output/adf_analysis_latest.xlsx`.
- Validator / verification: `armv10/scripts/validate_tiles.py` is the canonical validator used by cross verification tools.
- Preview generator: `armv10/scripts/preview_dashboard_html.py` creates a standalone Plotly + Sankey HTML report.
- Cross-verify: `armv10/scripts/cross_verify_all.py` compares validator output vs dashboard heuristics and writes `armv10/output/cross_verify_report.json`.

## Quick sanity checks
- Workbook path expected by the dashboard and scripts: `armv10/output/adf_analysis_latest.xlsx`.
- Validator CSV output: `armv10/output/validator_breakdowns.csv` (common default).

## What I recommend next
- Add download links for `adf_analysis_latest.xlsx`, `validator_breakdowns.csv`, and last run logs inside the dashboard.
- Standardize preview output path so the dashboard can reliably link/embed it (prefer `armv10/output/preview_report.html`).

---
If you'd like, I can:
- embed the preview.html into the Streamlit app, and
- add the download buttons and a safer two-step runner confirmation.

---

## Local change summary

A local `CHANGES_SUMMARY.md` file has been added to this folder (`armv10/CHANGES_SUMMARY.md`) which mirrors the repository-level change summary and contains quick commands and notes for maintainers.

This folder also contains `LOGIC.md` and `TILES.md` which document scoring/thresholds and tile mappings used by the dashboard.
