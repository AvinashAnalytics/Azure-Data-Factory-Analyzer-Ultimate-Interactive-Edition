# Dashboard Tiles — Meaning and Data Sources
> Last updated: 2025-11-07 (dashboard tiles/verification/runner improvements summarized)

This document describes every metric tile shown by the dashboard in `armv10/adf_dashboard.py`:
- what the tile means,
- the exact sheet(s) or dataframe(s) it comes from (including fallback logic), and
- any calculation or formatting applied.

Reference: the project `enhancement_config.json` (advanced_dashboard section) controls whether enhanced tiles and the health score are enabled. See `enhancement_config.json` for toggles like `advanced_dashboard.health_score`.

---

## Top-row metric tiles (Enhanced metrics row)
These appear in `render_enhanced_metrics()`.

1. Pipelines
   - Meaning: Number of pipeline resources discovered in the factory.
   - Data source / fallback logic:
     - Primary: `Summary` sheet metric `Pipelines` when present (via `get_summary_metric`).
     - Fallbacks (in order): count rows in any of these sheets if `Summary` missing: `ImpactAnalysis`, `PipelineAnalysis`, `Pipeline_Analysis`, `Pipelines`.
   - Type: integer count.

2. DataFlows
   - Meaning: Number of Data Flow resources discovered.
   - Data source / fallback logic:
     - Primary: `Summary` sheet `DataFlows`.
     - Fallbacks: `DataFlows`, `DataFlowLineage`, `DataFlow_Summary` sheets (row count or summary metric).

3. Datasets
   - Meaning: Number of dataset resources discovered.
   - Data source: `Summary` sheet `Datasets` or fallback to the `Datasets` sheet row count.

4. Triggers
  - Meaning: Number of triggers (scheduled/evented) discovered.
  - Data source / fallback logic (explicit):
    - Primary: `Summary` sheet `Triggers` when present.
    - Preferred sheet: `Triggers` (one row per trigger) — use this count when present.
    - Fallback: `TriggerDetails` — this sheet contains trigger occurrence/usage rows and can include multiple rows per trigger. When `Triggers` is absent the dashboard/validator will count unique trigger names from `TriggerDetails` (if a sensible name column like `Trigger` exists); otherwise it falls back to the raw row count.
  - Rationale: some analyzer outputs include a separate `Triggers` sheet listing unique triggers (canonical). `TriggerDetails` is richer (pipeline associations, schedules) but counts of its rows do not equal unique trigger definitions.

5. Dependencies
   - Meaning: Total number of dependency edges found (activity/pipeline triggers/dependsOn relationships).
   - Data source / fallback logic:
     - `Summary` sheet metric `Total Dependencies` when present.
     - Fallback sheets used to build dependency counts: `ActivityExecutionOrder`, `DataLineage`, `Pipeline_Pipeline`, `Pipeline_DataFlow`, `TriggerDetails`.

6. Health
   - Meaning: Factory-level health score (0–100) based chiefly on orphaned pipelines as a fraction of pipelines.
   - Data source / calculation:
     - `pipelines` (see Pipelines) and `orphaned` (see Orphaned tile) are used.
     - Formula (canonical):
       - If pipelines > 0:
         health_score = int((1 - orphaned / pipelines) * 100)
       - Else: health_score = 100
     - The gauge (`render_health_gauge`) maps the numeric score into statuses:
       - >=90 → Excellent (green)
       - 75–89 → Good (blue)
       - 60–74 → Fair (yellow)
       - <60 → Needs Attention (red)
   - Controlled by: `enhancement_config.json` → `advanced_dashboard.health_score` (true/false)

7. Orphaned
   - Meaning: Count of orphaned pipelines (resources with no inbound references/usage).
   - Data source:
     - `OrphanedPipelines` or `Orphaned_Pipelines` sheets (row count) or Summary metric `Orphaned Pipelines`.
   - Formatting: When orphaned > 0 the tile uses a warning gradient and shows a warning emoji.

---

## Secondary row: Source/Target and Static vs Dynamic metrics
Also rendered by `render_enhanced_metrics()`.

1. Source Datasets
   - Meaning: Unique count of source datasets (distinct dataset names that appear as sources in lineage).
   - Data source: `DataLineage` sheet / dataframe column `Source` (unique non-null count).

2. Target Datasets
   - Meaning: Unique count of sink/target datasets.
   - Data source: `DataLineage` sheet / dataframe column `Sink` (unique non-null count).

3. Static Sources / Static Targets
   - Meaning: Count of source/sink entries that appear to be concrete/static dataset identifiers.
   - Detection rule: `SourceTable`/`SinkTable` values that do NOT contain parameterization patterns such as `@dataset`, `@{`, `pipeline()`, `activity()`.
   - Data source: `DataLineage` sheet columns `SourceTable` and `SinkTable`.

4. Dynamic Sources / Dynamic Targets
   - Meaning: Count of parameterized or templated dataset references (suggesting runtime variation).
   - Detection rule: `SourceTable`/`SinkTable` values that contain `@dataset`, `@{`, `pipeline()` or `activity()`.
   - Data source: `DataLineage` sheet.

---

## Impact level and severity tiles
These appear in impact analysis and the "impact level metrics" row.

- CRITICAL / HIGH / MEDIUM / LOW counts
  - Meaning: Number of pipelines (or items) flagged with that impact level.
  - Data source: `ImpactAnalysis` or `impact`/`Impact` column in pipeline analysis dataframes. If missing, the analyzer defaults some impacts to `LOW` for display.
  - Ordering and color mapping are controlled by `impact_order = {'CRITICAL':0,'HIGH':1,'MEDIUM':2,'LOW':3}` and colors defined in code (CRITICAL red, HIGH orange, MEDIUM yellow, LOW green).

---

## Overview & utility tiles (summary area)
Across the UI there are a few extra tiles and metrics: 

- Sheets Loaded
  - Meaning: Number of sheets read from the analysis workbook.
  - Data source: `metadata.get('sheets', [])` length.

- Total Records
  - Meaning: Sum of rows across data sheets that contribute to the analysis.
  - Data source: calculated `total_records` aggregated during metadata extraction.

- Total Lineage Records / Unique Sources / Unique Sinks
  - Meaning: Lineage-specific metrics derived from `DataLineage` dataframe (counts of records, unique sources, unique sinks).

- Activity-specific counts (e.g., Copy Activities)
  - Meaning: Counts of activities of a particular type discovered during parsing.
  - Data source: `ActivityCount` or similar sheets; the dashboard aggregates by `ActivityType`.

---

## Where tile data comes from (summary)
- Preferred single source: `Summary` sheet (if present) — many top-level metrics are read from named `Metric` rows in this sheet.
- If `Summary` is missing or incomplete, the dashboard uses sheet-specific fallbacks (counts from canonical sheet names). Example fallback mapping is implemented in `get_count_with_fallback()` in `adf_dashboard.py`.
- Lineage and source/target classification use `DataLineage`.
- Circular detection / orphan counts come from `CircularDependencies` and `Orphaned*` sheets generated by the analyzer.
- Impact levels come from `ImpactAnalysis` / `impact` column in pipeline analysis results.

---

## Where to change tiles or formulas
- `adf_dashboard.py` is the place to change tile labels, HTML/CSS, or visual mapping.
- `adf_analyzer_v10_excel_enhancements.py` and analyzer modules are the place to change detection logic (e.g., how orphans or circular deps are computed) or to change scoring/impact assignment.
- `enhancement_config.json` toggles features (see `advanced_dashboard` / `health_score`, `activity_distribution`, `network_stats`, etc.).

---

## Notes & assumptions
- The UI code includes defensive fallbacks for missing sheets: blank or missing summary metrics do not crash the UI; counts default to 0 and are derived from fallback sheets.
- The dashboard treats circular dependencies as CRITICAL (must-fix). See `CircularDependencies` sheet and `LOGIC.md` for scoring impacts.
- If you want image icons instead of emojis, we can replace the emoji glyphs with embedded SVGs or data-URI images for crisp cross-platform rendering.

If you'd like, I can add a short link from the dashboard homepage to this `TILES.md` and embed a little legend tooltip for each tile.
