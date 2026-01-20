# armv10 — Required files and intent

This file lists the essential Python modules, scripts, configs and supporting files in `armv10/` that are required to generate the ADF Analyzer Excel workbook and run the Streamlit dashboard. Use this manifest to focus maintenance, CI, and code review.

## Top-priority (keep and maintain)

- `adf_analyzer_v10_complete.py` — Core analyzer implementation (UltimateEnterpriseADFAnalyzer). Produces the analysis and writes the Excel workbook. (Required)
- `adf_analyzer_v10_patched_runner.py` — High-level runner that applies patches and invokes the analyzer. The Streamlit "Generate Excel" UI runs this script by default. (Required)
- `adf_analyzer_v10_patch.py` — Functional patches applied before analysis (missing parsers, bugfixes). (Required)
- `adf_analyzer_v10_excel_enhancements.py` — Excel formatting and enhancements (project banner, tiles, conditional formatting). (Required for final workbook look-and-feel)
- `adf_dashboard.py` — Streamlit dashboard UI. Displays the produced workbook and contains the integrated "Generate Excel" workflow.

## Supporting scripts and utilities (keep)

- `scripts/cross_verify_all.py` — Cross-verification utilities (validate Excel parity and produce reports). Useful for QA and CI.
- `scripts/preview_dashboard_html.py` / `scripts/preview_dashboard.py` — Generate Plotly/HTML previews from workbook output.
- `scripts/run_validator_runs.py`, `scripts/validate_tiles*.py`, `scripts/check_dashboard_tiles.py` — Tile validation and QA tools.
- `analyze_lineage_data.py`, `verify_real_world.py`, `VERIFICATION_REPORT.py` — Lineage analysis and verification helpers.

## Configuration and outputs (keep and manage)

- `enhancement_config.json` — Canonical configuration for Excel enhancements and dashboard advanced toggles. The UI may read or write equivalent values.
- `streamlit_config.json`, `settings.json` — App settings useful for deployment and behavior toggles.
- `output/` — Generated workbooks and auxiliary artifacts (keep in `.gitignore` for large files; treat as runtime output directory).

## Documentation and artifacts (keep)

- `TILES.md`, `LOGIC.md`, `CHANGES_SUMMARY.md`, `readme_v10.md` — Documentation that the dashboard references; keep them synchronized with code changes.

## Archive / candidates for removal

- `bak/` folder, `readme_v10.md.bak`, and `tmp_*` files — backups and temporary copies. Move to an `archive/` location or remove from main branch to reduce noise.

## Recommended actions

1. Keep the top-priority files always in the main branch. Add tests that run the patched runner with a small sample template (CI smoke test).
2. Ensure `adf_analyzer_v10_patched_runner.py` documents and accepts the config shape you use in the dashboard. If it reads `ADF_ANALYZER_CONFIG_JSON` or `adf_runner_temp_config.json`, keep that contract stable.
3. Move `bak/` content to an `archive/` branch or folder outside the main repo to reduce accidental edits.
4. Add a small test fixture (example template JSON) and a quick CI job that runs the patched runner and checks `output/adf_analysis_latest.xlsx` exists and is a valid workbook.

---

Generated on: 2025-11-07
