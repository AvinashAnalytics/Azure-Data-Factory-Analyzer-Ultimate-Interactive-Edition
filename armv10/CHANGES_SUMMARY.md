# Change summary — Nov 07, 2025

This file mirrors the repository-level change summary and lists the recent edits and actionable changes applied inside the `armv10/` folder so maintainers have a self-contained update list.

## Highlights (latest)
- Streamlit dashboard (`adf_dashboard.py`) modernized:
  - New launcher/landing chooser (Run Analyzer vs Upload/Analyze).
  - Tile layout modernization, improved CSS, and robust metric helpers.
  - "🔍 Verify tiles" snapshot action that computes and persists verification JSONs to `armv10/output/`.
  - Runner integration (import or subprocess) with run history and logs.
  - Enhancement-config quick toggles to control Excel beautification features.
- Patched analyzer runner: `adf_analyzer_v10_patched_runner.py` applies patches then runs the analyzer and writes `armv10/output/adf_analysis_latest.xlsx`.
- Canonical validator: `armv10/scripts/validate_tiles.py` — repaired and used as authoritative tile verifier (produces JSON and CSV summaries).
- Expert preview: `armv10/scripts/preview_dashboard_html.py` generates a standalone Plotly + Sankey HTML preview (recommend moving output to `armv10/output/preview_report.html`).
- Cross-verify script: `armv10/scripts/cross_verify_all.py` runs the validator, computes dashboard-like metrics from the workbook, and writes parity reports to `armv10/output/`.

## Quick commands (examples)

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
D:/path/to/python.exe scripts/preview_dashboard_html.py output/adf_analysis_latest.xlsx --out output/preview_report.html
```

Cross-verify validator vs dashboard heuristics:

```powershell
cd d:\armtemp\armv10
D:/path/to/python.exe scripts/cross_verify_all.py output/adf_analysis_latest.xlsx
```

## Notes & next steps
- The dashboard and related scripts expect `armv10/output/adf_analysis_latest.xlsx`. Some preview scripts write to `scripts/output/` — recommended to standardize to `armv10/output/`.
- Recommended next tasks (I can implement any on request):
  - Embed or link the HTML preview into the Streamlit dashboard.
  - Add download links in the dashboard for the latest workbook, validator CSV, run-log and verify snapshots.
  - Add a CI parity check that fails on validator vs dashboard metric mismatches.

If you want, I can implement the most important next step now—tell me which to prioritize and I'll apply the change and run quick smoke tests.

D:\armtemp\armv10\scripts\normalize_readme.py: Normalized 'Last updated' to 2025-11-07 and removed decorative 'END OF PART' banners on 2025-11-07.

- docs: added `docs/armv10/REQUIRED_FILES.md` and updated `docs/README.md` to reference armv10 manifest (2025-11-07)
- dashboard: removed duplicate ADF Patch Runner expander section; functionality moved to top-level "Generate Excel" tab and updated load_excel_file to handle file paths (2025-11-07)
- testing: successfully tested `adf_analyzer_v10_patched_runner.py` with virtual environment `D:/sql_generator/.venv`, all dependencies working, analysis completed with 886 resources processed and Excel output generated (2025-11-07)
- dashboard: restructured to show two main options upfront - "⚙️ Generate Excel" (patch runner) and "📊 Upload & Analyze" (existing dashboard) as top-level tabs without requiring launcher selection (2025-11-07)
- bugfix: fixed Unicode encoding issue in dashboard subprocess execution by adding PYTHONIOENCODING=utf-8 environment variable and using virtual environment Python path; patched runner now works correctly from dashboard (2025-11-07)
- enhancement: improved Generate Excel tab with detailed script information, clear enhancement options explanations, execution summary, and better user guidance for which files are needed for each script type (2025-11-07)
