"""
Enhanced test to check SourceTable and SinkTable columns for file/table patterns
"""
import pandas as pd

excel_file = "streamlit_app/data/adf_analysis_latest.xlsx"

print("=" * 80)
print("Enhanced Source/Target Analysis - Checking Table Columns")
print("=" * 80)

try:
    df = pd.read_excel(excel_file, sheet_name="DataLineage")
    
    print(f"\n📊 DataLineage Sheet: {len(df)} records")
    
    # Analyze SourceTable column
    if "SourceTable" in df.columns:
        source_tables = df["SourceTable"].dropna().unique()
        print(f"\n📥 SourceTable Column Analysis:")
        print(f"   Unique Values: {len(source_tables)}")
        
        # Check for file patterns in SourceTable
        file_patterns = [t for t in source_tables if '/' in str(t) or '\\' in str(t) or 
                        any(str(t).lower().endswith(ext) for ext in ['.csv', '.json', '.parquet', '.txt', '.xml', '.avro', '.orc'])]
        
        print(f"   File-like patterns: {len(file_patterns)}")
        if file_patterns:
            print(f"   Examples:")
            for fp in file_patterns[:10]:
                print(f"      - {fp}")
        
        # Table patterns (no slashes, no file extensions)
        table_patterns = [t for t in source_tables if t not in file_patterns and str(t) != 'nan']
        print(f"   Table-like patterns: {len(table_patterns)}")
        if table_patterns:
            print(f"   Examples:")
            for tp in table_patterns[:10]:
                print(f"      - {tp}")
    
    # Analyze SinkTable column
    if "SinkTable" in df.columns:
        sink_tables = df["SinkTable"].dropna().unique()
        print(f"\n📤 SinkTable Column Analysis:")
        print(f"   Unique Values: {len(sink_tables)}")
        
        # Check for file patterns in SinkTable
        file_patterns = [t for t in sink_tables if '/' in str(t) or '\\' in str(t) or 
                        any(str(t).lower().endswith(ext) for ext in ['.csv', '.json', '.parquet', '.txt', '.xml', '.avro', '.orc'])]
        
        print(f"   File-like patterns: {len(file_patterns)}")
        if file_patterns:
            print(f"   Examples:")
            for fp in file_patterns[:10]:
                print(f"      - {fp}")
        
        # Table patterns
        table_patterns = [t for t in sink_tables if t not in file_patterns and str(t) != 'nan']
        print(f"   Table-like patterns: {len(table_patterns)}")
        if table_patterns:
            print(f"   Examples:")
            for tp in table_patterns[:10]:
                print(f"      - {tp}")
    
    print("\n" + "=" * 80)
    print("💡 Recommendation:")
    print("   The dashboard currently uses Source/Sink columns (dataset names).")
    print("   For more detailed file/table breakdown, we could use SourceTable/SinkTable")
    print("   columns which contain the actual table names or file paths.")
    print("=" * 80)

except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback
    traceback.print_exc()
