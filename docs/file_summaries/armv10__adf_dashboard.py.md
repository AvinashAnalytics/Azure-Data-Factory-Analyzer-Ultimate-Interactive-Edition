# armv10\adf_dashboard.py

> Auto-generated summary. Improve this page with architecture notes, examples and references.

## Module summary

═══════════════════════════════════════════════════════════════════════════════
Azure Data Factory Analyzer Dashboard v10.1 - FIXED & PRODUCTION READY
═══════════════════════════════════════════════════════════════════════════════

✨ FEATURES:
  🌐 Advanced Network Visualizations (2D & 3D)
  📊 20+ Interactive Charts
  🎨 Modern Material Design UI
  🔍 Smart Search & Filtering
  📈 Real-time Analytics
  💡 AI-Powered Insights
  🎯 Impact Analysis
  📱 Responsive Design
  📥 Multiple Export Formats

FIXES APPLIED:
  ✅ Fixed all incomplete functions
  ✅ Fixed data structure compatibility with v9.1 analyzer
  ✅ Fixed session state management
  ✅ Fixed CSS rendering issues
  ✅ Added comprehensive error handling
  ✅ Optimized for large datasets
  ✅ Fixed network visualization bugs
  ✅ Added caching for performance
  ✅ Fixed Excel sheet name mismatches

Author: Enterprise ADF Team
Date: 2024
Version: 10.1 - Fixed & Production Ready
═══════════════════════════════════════════════════════════════════════════════

## Classes

- **ADF_Dashboard** — Enterprise ADF Analysis Dashboard v10.1

## Functions

- **load_custom_css()** — Load optimized custom CSS
- **_ensure_css_loaded()** — Ensure the custom CSS is injected only once per session.
- **render_info_card()** — Render a consistent info-card using the app CSS.
- **render_feature_card()** — Render a visually prominent gradient feature card (matches the sample look).
- **prepare_pie_data()** — Helper to prepare pie chart labels and values safely.
- **to_csv_bytes()** — Return CSV bytes with UTF-8 BOM so Excel opens it correctly.
- **to_json_bytes()** — Return JSON bytes (utf-8).
- **to_excel_bytes()** — Write a dict of DataFrames to an in-memory Excel workbook and return bytes.
- **initialize_session_state()** — Initialize all session state variables with defaults
- **safe_get_dataframe()** — Safely get DataFrame from excel_data with fallback names
- **get_summary_metric()** — Get metric from Summary sheet
- **get_count_with_fallback()** — Retrieve a numeric count from the Summary sheet, coercing strings to numbers,
- **format_number()** — Format number with thousand separators
- **sum_numeric_columns_by_keywords()** — Sum numeric-looking columns whose names contain any of the provided keywords.
- **truncate_text()** — Truncate text with ellipsis
- **_merge_split_sheets_inplace()** — Detect sheets split with suffix _P1/_P2/... and merge them into a single sheet.
- **_normalize_sheet_map_inplace()** — Create convenient aliases in the excel_data map for common variants.
- **safe_plotly()** — Safely render a plotly figure in Streamlit.
- **main()** — Main application entry point

## Notes

Add usage, examples, cross-references, data shapes, and important edge cases here.
