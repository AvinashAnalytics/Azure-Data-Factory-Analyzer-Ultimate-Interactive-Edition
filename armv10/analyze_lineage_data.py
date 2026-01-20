"""
Analyze DataLineage to understand why file detection is returning 0
"""
import pandas as pd

excel_file = "streamlit_app/data/adf_analysis_latest.xlsx"

print("=" * 80)
print("DEEP DIVE: Why are file counts zero?")
print("=" * 80)

df = pd.read_excel(excel_file, sheet_name="DataLineage")

print(f"\n📊 Total Records: {len(df)}")
print(f"📋 Columns: {', '.join(df.columns.tolist())}")

# Examine Source column
print("\n" + "=" * 80)
print("SOURCE COLUMN ANALYSIS")
print("=" * 80)

if "Source" in df.columns:
    sources = df["Source"].dropna().unique()
    print(f"\nTotal Unique Sources: {len(sources)}")
    
    print("\n🔍 First 20 Source Examples:")
    for i, src in enumerate(sources[:20], 1):
        src_str = str(src)
        has_slash = '/' in src_str or '\\' in src_str
        has_extension = any(src_str.lower().endswith(ext) for ext in ['.csv', '.json', '.parquet', '.txt', '.xml'])
        
        print(f"  {i:2}. {src[:80]}")
        print(f"      Has slash: {has_slash}, Has extension: {has_extension}")
        
        # Check what type of object it is
        if 'DS_' in src_str or 'ds_' in src_str:
            print(f"      → This is a DATASET name, not a file path")
        elif has_slash or has_extension:
            print(f"      → This WOULD be detected as a FILE")
        else:
            print(f"      → This is a TABLE/DATASET name")
        print()

# Examine SourceTable column for actual table/file names
print("\n" + "=" * 80)
print("SOURCETABLE COLUMN ANALYSIS (Actual Table/File Names)")
print("=" * 80)

if "SourceTable" in df.columns:
    source_tables = df["SourceTable"].dropna()
    unique_source_tables = source_tables.unique()
    
    print(f"\nTotal Records with SourceTable: {len(source_tables)}")
    print(f"Unique SourceTable Values: {len(unique_source_tables)}")
    
    print("\n🔍 SourceTable Examples:")
    for i, tbl in enumerate(unique_source_tables[:20], 1):
        tbl_str = str(tbl)
        has_slash = '/' in tbl_str or '\\' in tbl_str
        has_extension = any(tbl_str.lower().endswith(ext) for ext in ['.csv', '.json', '.parquet', '.txt', '.xml'])
        
        print(f"  {i:2}. {tbl[:80]}")
        if has_slash or has_extension:
            print(f"      → FILE pattern detected!")
        elif '@dataset' in tbl_str or '@{' in tbl_str:
            print(f"      → Parameterized expression")
        else:
            print(f"      → Table name")

print("\n" + "=" * 80)
print("💡 EXPLANATION")
print("=" * 80)

print("""
Why File Counts are Zero:

1. The "Source" and "Sink" columns contain DATASET NAMES (e.g., "DS_ADLS_Generic")
   - These are ADF Dataset resource names, NOT file paths
   - Dataset names typically start with "DS_" or "ds_"
   - They don't contain slashes or file extensions

2. The actual file/table paths are in "SourceTable" and "SinkTable" columns
   - These contain the ACTUAL targets like file paths or table names
   - But many are parameterized: "@dataset:p_schema.@dataset:p_table"
   - Dynamic expressions are evaluated at runtime, not in the template

3. Your ADF uses parameterized datasets (best practice!)
   - File paths and table names are passed as parameters
   - This makes pipelines reusable across environments
   - But it means we can't determine static file/table counts from the template

SOLUTION:
We should enhance the dashboard to:
  a) Check SourceTable/SinkTable columns in addition to Source/Sink
  b) Detect common file patterns in those columns
  c) Handle parameterized expressions gracefully
  d) Show "Dynamic/Parameterized" count as a separate metric
""")

print("\n" + "=" * 80)
print("🔧 RECOMMENDED METRICS")
print("=" * 80)

# Better classification
if "SourceTable" in df.columns:
    source_tables = df["SourceTable"].dropna()
    
    # Classify into categories
    files = []
    tables = []
    parameterized = []
    
    for tbl in source_tables:
        tbl_str = str(tbl)
        if '@dataset' in tbl_str or '@{' in tbl_str or 'pipeline()' in tbl_str:
            parameterized.append(tbl)
        elif '/' in tbl_str or '\\' in tbl_str or any(tbl_str.lower().endswith(ext) for ext in ['.csv', '.json', '.parquet', '.txt', '.xml', '.avro', '.orc']):
            files.append(tbl)
        else:
            tables.append(tbl)
    
    print(f"""
Improved Source Classification:
  📁 Static Files: {len(files)}
  🗄️  Static Tables: {len(tables)}
  🔧 Parameterized/Dynamic: {len(parameterized)}
  📊 Total: {len(source_tables)}

Examples of Parameterized:
""")
    for p in list(set(parameterized))[:5]:
        print(f"  - {p}")

print("\n" + "=" * 80)
