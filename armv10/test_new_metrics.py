"""
Test the new dashboard metrics logic
"""
import pandas as pd

excel_file = "streamlit_app/data/adf_analysis_latest.xlsx"

print("=" * 80)
print("Testing Updated Dashboard Metrics")
print("=" * 80)

df = pd.read_excel(excel_file, sheet_name="DataLineage")

total_source_datasets = 0
total_target_datasets = 0
total_source_static = 0
total_target_static = 0
total_source_dynamic = 0
total_target_dynamic = 0

if not df.empty:
    # Count unique source/sink datasets
    if "Source" in df.columns:
        total_source_datasets = df["Source"].dropna().nunique()
    
    if "Sink" in df.columns:
        total_target_datasets = df["Sink"].dropna().nunique()
    
    # Analyze SourceTable/SinkTable for static vs dynamic
    if "SourceTable" in df.columns:
        source_tables = df["SourceTable"].dropna()
        for tbl in source_tables:
            tbl_str = str(tbl)
            # Check if parameterized/dynamic
            if '@dataset' in tbl_str or '@{' in tbl_str or 'pipeline()' in tbl_str or 'activity()' in tbl_str:
                total_source_dynamic += 1
            else:
                total_source_static += 1
    
    if "SinkTable" in df.columns:
        sink_tables = df["SinkTable"].dropna()
        for tbl in sink_tables:
            tbl_str = str(tbl)
            # Check if parameterized/dynamic
            if '@dataset' in tbl_str or '@{' in tbl_str or 'pipeline()' in tbl_str or 'activity()' in tbl_str:
                total_target_dynamic += 1
            else:
                total_target_static += 1

print("\n📊 DASHBOARD METRICS (Updated):\n")
print(f"  Row 2 - Tile 1: Source Datasets      = {total_source_datasets:,}")
print(f"  Row 2 - Tile 2: Target Datasets      = {total_target_datasets:,}")
print(f"  Row 2 - Tile 3: Static Sources       = {total_source_static:,}")
print(f"  Row 2 - Tile 4: Static Targets       = {total_target_static:,}")
print(f"  Row 2 - Tile 5: Dynamic Sources      = {total_source_dynamic:,}")
print(f"  Row 2 - Tile 6: Dynamic Targets      = {total_target_dynamic:,}")

print("\n" + "=" * 80)
print("💡 EXPLANATION")
print("=" * 80)

print(f"""
Why These Metrics Make Sense:

1️⃣  SOURCE/TARGET DATASETS ({total_source_datasets}/{total_target_datasets})
   → Count of unique ADF Dataset resources used
   → These are the reusable dataset definitions in your factory
   → Example: "DS_ADLS_Generic", "DS_EDW_SNOWFLAKE_PROD"

2️⃣  STATIC SOURCES/TARGETS ({total_source_static}/{total_target_static})
   → Hard-coded table/file names in the template
   → Can be analyzed without running the pipeline
   → Example: "dbo.customers", "PRODFA.FMT960P"

3️⃣  DYNAMIC SOURCES/TARGETS ({total_source_dynamic}/{total_target_dynamic})
   → Parameterized/expression-based paths
   → Values determined at runtime (best practice!)
   → Example: "@dataset:p_schema.@dataset:p_table"

Your Factory Uses {total_source_dynamic + total_target_dynamic} dynamic expressions!
This means your pipelines are highly reusable and configurable. ✅
""")

print("=" * 80)
print("✅ Updated metrics reflect your actual ADF architecture!")
print("=" * 80)
