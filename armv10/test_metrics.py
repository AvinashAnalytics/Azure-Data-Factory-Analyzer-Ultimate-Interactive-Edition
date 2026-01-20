"""
Test script to verify the new source/target metrics calculation
"""
import pandas as pd
import openpyxl

# Load the test Excel file
excel_file = "streamlit_app/data/adf_analysis_latest.xlsx"

print("=" * 80)
print("Testing Source/Target Metrics Calculation")
print("=" * 80)

try:
    # Read DataLineage sheet
    df = pd.read_excel(excel_file, sheet_name="DataLineage")
    
    print(f"\n📊 DataLineage Sheet Loaded")
    print(f"   Total Records: {len(df)}")
    print(f"   Columns: {', '.join(df.columns.tolist())}")
    
    # Calculate source metrics
    if "Source" in df.columns:
        sources = df["Source"].dropna().unique()
        print(f"\n📥 SOURCE ANALYSIS:")
        print(f"   Total Unique Sources: {len(sources)}")
        
        # Classify as files or tables
        source_files = [s for s in sources if '/' in str(s) or '\\' in str(s) or 
                       str(s).lower().endswith(('.csv', '.json', '.parquet', '.txt', '.xml'))]
        source_tables = [s for s in sources if s not in source_files]
        
        print(f"   → Source Files: {len(source_files)}")
        if source_files[:5]:
            print(f"      Examples: {', '.join(str(s)[:50] for s in source_files[:5])}")
        
        print(f"   → Source Tables: {len(source_tables)}")
        if source_tables[:5]:
            print(f"      Examples: {', '.join(str(s)[:50] for s in source_tables[:5])}")
    
    # Calculate target metrics
    if "Sink" in df.columns:
        sinks = df["Sink"].dropna().unique()
        print(f"\n📤 TARGET (SINK) ANALYSIS:")
        print(f"   Total Unique Targets: {len(sinks)}")
        
        # Classify as files or tables
        target_files = [s for s in sinks if '/' in str(s) or '\\' in str(s) or 
                       str(s).lower().endswith(('.csv', '.json', '.parquet', '.txt', '.xml'))]
        target_tables = [s for s in sinks if s not in target_files]
        
        print(f"   → Target Files: {len(target_files)}")
        if target_files[:5]:
            print(f"      Examples: {', '.join(str(s)[:50] for s in target_files[:5])}")
        
        print(f"   → Target Tables: {len(target_tables)}")
        if target_tables[:5]:
            print(f"      Examples: {', '.join(str(s)[:50] for s in target_tables[:5])}")
    
    print("\n" + "=" * 80)
    print("✅ Metrics Calculation Test Complete")
    print("=" * 80)
    
    # Summary
    print("\n📊 DASHBOARD METRICS:")
    print(f"   Total Source Files: {len(source_files)}")
    print(f"   Total Target Files: {len(target_files)}")
    print(f"   Total Source Tables: {len(source_tables)}")
    print(f"   Total Target Tables: {len(target_tables)}")
    
except FileNotFoundError:
    print(f"\n❌ Error: Excel file not found at {excel_file}")
    print("   Please ensure the analyzer has generated output first.")
    
except ValueError as e:
    print(f"\n⚠️  Warning: DataLineage sheet not found in Excel file")
    print(f"   This is normal if the template has no Copy activities or data flows.")
    print(f"   Error: {e}")
    
except Exception as e:
    print(f"\n❌ Unexpected Error: {e}")
    import traceback
    traceback.print_exc()
