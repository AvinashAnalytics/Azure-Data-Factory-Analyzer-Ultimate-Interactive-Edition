# Change summary — Nov 07, 2025

This file summarizes the recent edits and actionable changes made across the repository so you can quickly see what to test and where to look.

## Highlights (latest)
- Streamlit dashboard (`armv10/adf_dashboard.py`) modernized:
  - New launcher/landing chooser (Run Analyzer vs Upload/Analyze).
  - Tile layout modernization, improved CSS, and robust metric helpers.
  - "🔍 Verify tiles" snapshot action that computes and persists verification JSONs.
  - Runner integration (import or subprocess) with run history and logs.
  - Enhancement-config quick toggles to control Excel beautification features.
- Patched analyzer runner added/updated: `armv10/adf_analyzer_v10_patched_runner.py` (applies functional patches and Excel enhancements, then runs the analyzer).
- Canonical validator: `armv10/scripts/validate_tiles.py` — repaired and used as authoritative tile verifier.
- Expert preview: `armv10/scripts/preview_dashboard_html.py` generates a standalone HTML preview (Plotly + Sankey).
- Cross-verify script: `armv10/scripts/cross_verify_all.py` runs the validator, computes dashboard-like metrics from the workbook, and writes parity reports to `armv10/output/`.

## Typical workflows / commands

Run patched analyzer (CLI):

```powershell
cd d:\armtemp\armv10
D:/path/to/python.exe adf_analyzer_v10_patched_runner.py test2.json
```

Start the Streamlit dashboard:

```powershell
cd d:\armtemp\armv10
streamlit run adf_dashboard.py
```

Validate a produced workbook (canonical validator):

```powershell
cd d:\armtemp\armv10
D:/path/to/python.exe scripts/validate_tiles.py output/adf_analysis_latest.xlsx --csv output/validator_breakdowns.csv
```

Generate expert HTML preview:

```powershell
cd d:\armtemp\armv10
D:/path/to/python.exe scripts/preview_dashboard_html.py output/adf_analysis_latest.xlsx
```

Cross-verify validator vs dashboard heuristics:

```powershell
cd d:\armtemp\armv10
D:/path/to/python.exe scripts/cross_verify_all.py output/adf_analysis_latest.xlsx
```

## Notes & next steps
- The dashboard reads `armv10/output/adf_analysis_latest.xlsx` by default. Many scripts write into `armv10/output/` — consider standardizing any other output paths (some preview scripts write to `scripts/output/`).
- Suggested next tasks:
  - Embed or link the HTML preview into the Streamlit dashboard.
  - Add download links in the dashboard for the latest workbook, validator CSV, run logs, and verify snapshots.
  - Add a CI parity check that fails on validator vs dashboard metric mismatches.

If you want, I can implement any of the next steps above—tell me which to prioritize and I'll make the edits and run quick smoke tests.
