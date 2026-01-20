import pandas as pd
from pathlib import Path
import re

wb = Path(__file__).resolve().parent.parent / 'streamlit_app' / 'data' / 'adf_analysis_latest.xlsx'
print('Workbook path:', wb)

xls = pd.read_excel(wb, sheet_name=None)
print('\nSheets found:', len(xls))
for k, df in xls.items():
    print(f' - {k} : rows={len(df)} cols={len(df.columns)}')

# Normalize keys
def normalize_key(k: str) -> str:
    return re.sub(r"[_\s]+", "", str(k)).lower()

summary = None
for k, df in xls.items():
    if normalize_key(k) == 'summary':
        summary = df
        break

print('\nSummary sheet present:' , summary is not None)
if summary is not None:
    # try to coerce Triggers row
    cols = {re.sub(r"[_\s]+","",c).lower():c for c in summary.columns}
    metric_col = cols.get('metric', summary.columns[0])
    value_col = cols.get('value', summary.columns[1] if len(summary.columns)>1 else summary.columns[0])
    print('Summary columns:', summary.columns.tolist())
    vals = {}
    for _, row in summary.iterrows():
        key = str(row[metric_col]).strip()
        vals[key] = row[value_col]
    print('\nSummary Triggers value:', vals.get('Triggers', vals.get('Triggers ', vals.get('TriggersCount', None))))

# Check fallback sheets
fallbacks = ['TriggerDetails','Triggers','Trigger_Details']
for s in fallbacks:
    if s in xls:
        df = xls[s]
        print(f"\nFound sheet '{s}': rows={len(df)} cols={len(df.columns)}")
        # try to find candidate columns for trigger name
        candidates = ['TriggerName','Trigger','Name','triggername','trigger_name','Trigger_Name']
        found = None
        for c in candidates:
            if c in df.columns:
                found = c
                break
        print('Columns sample:', df.columns.tolist()[:10])
        if found:
            print('Using column for names:', found)
            unique = df[found].dropna().astype(str).str.strip()
            print('Unique names count:', len(unique.unique()))
            print('Top 20 names and counts:')
            print(unique.value_counts().head(20).to_string())
        else:
            # try to print first two columns
            if len(df.columns)>=1:
                col0 = df.columns[0]
                print(f"No known name column. Showing sample of column '{col0}':")
                print(df[col0].dropna().astype(str).head(20).to_string())
    else:
        print(f"Sheet '{s}' not present")

# Also print any sheet that looks like triggers (name contains 'trigger')
print('\nSheets matching /trigger/ (case-insensitive):')
for k in xls.keys():
    if re.search(r'trigger', k, re.IGNORECASE):
        print(' -', k, 'rows=', len(xls[k]))

print('\nDone')
