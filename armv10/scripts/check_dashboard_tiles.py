"""
Small verifier for dashboard tiles.
Reads the analyzer workbook and prints:
 - Impact severity counts (CRITICAL/HIGH/MEDIUM/LOW)
 - Orphaned counts (pipelines, datasets, linked services)
 - Broken/inactive triggers (sheet/orphan counts)
 - DataLineage: total records, unique sources, unique sinks, copy activity count

Usage: run with the workspace venv python
"""
from pathlib import Path
import sys
import re
import pandas as pd

wb = Path(__file__).resolve().parents[1] / 'output' / 'adf_analysis_latest.xlsx'
if not wb.exists():
    print('Workbook not found:', wb)
    sys.exit(2)

xls = pd.read_excel(wb, sheet_name=None)

def normalize(s):
    return re.sub(r"[_\s]+", "", str(s)).lower()

# Impact counts
impact_df = None
for k, df in xls.items():
    if normalize(k) in ('impactanalysis','impact_analysis','impact'):
        try:
            impact_df = df
            break
        except Exception:
            continue

imp_counts = { 'CRITICAL':0, 'HIGH':0, 'MEDIUM':0, 'LOW':0 }
if impact_df is None or getattr(impact_df, 'empty', True):
    imp_src = 'None'
else:
    # find impact column
    col = None
    for c in impact_df.columns:
        if 'impact' == str(c).strip().lower() or 'impact' in str(c).strip().lower():
            col = c
            break
    if col is None:
        # try values in columns
        flattened = impact_df.applymap(lambda x: str(x).upper() if pd.notna(x) else x)
        for lvl in imp_counts:
            imp_counts[lvl] = int((flattened == lvl).sum().sum())
    else:
        vals = impact_df[col].astype(str).str.strip().str.upper()
        for lvl in imp_counts:
            imp_counts[lvl] = int((vals == lvl).sum())

# Orphans
def count_sheet(names):
    for name in names:
        for k, df in xls.items():
            if normalize(k) == normalize(name) and not getattr(df, 'empty', True):
                return len(df), k
    return 0, None

orph_pipelines, kp = count_sheet(['OrphanedPipelines','Orphaned_Pipelines','Orphaned Pipelines'])
orph_datasets, kd = count_sheet(['OrphanedDatasets','Orphaned_Datasets','Orphaned Datasets'])
orph_services, ks = count_sheet(['Orphaned_LinkedServices','OrphanedLinkedServices','Orphaned LinkedServices','OrphanedServices'])

# Broken/inactive triggers: many reports write OrphanedTriggers or BrokenTriggers or "TriggerDetails"/"OrphanedTriggers"
broken_triggers = 0
for name in ('OrphanedTriggers','Orphaned_Triggers','BrokenTriggers','TriggerDetails','Orphaned Triggers'):
    for k, df in xls.items():
        if normalize(k) == normalize(name) and not getattr(df, 'empty', True):
            # If TriggerDetails, try unique trigger name
            if normalize(k) == 'triggerdetails':
                cand = None
                for c in df.columns:
                    if 'trigger' in str(c).lower() and 'name' in str(c).lower():
                        cand = c
                        break
                if cand is not None:
                    broken_triggers = int(df[cand].dropna().astype(str).str.strip().nunique())
                else:
                    broken_triggers = int(len(df))
            else:
                broken_triggers = int(len(df))
            break
    if broken_triggers>0:
        break

# DataLineage stats
dl = None
for k, df in xls.items():
    if normalize(k) in ('datalineage','datalineage'):
        dl = df
        break
if dl is None or getattr(dl, 'empty', True):
    # try variants
    for k, df in xls.items():
        if normalize(k).startswith('datalineage'):
            dl = df
            break

total_lineage = 0
unique_sources = 0
unique_sinks = 0
copy_activities = 0
if dl is not None and not getattr(dl, 'empty', True):
    total_lineage = len(dl)
    # find source/sink column
    src_col = None
    sink_col = None
    for c in dl.columns:
        lc = str(c).lower()
        if src_col is None and 'source' in lc:
            src_col = c
        if sink_col is None and ('sink' in lc or 'target' in lc):
            sink_col = c
    if src_col is not None:
        unique_sources = int(dl[src_col].dropna().astype(str).str.strip().replace('', pd.NA).nunique())
    if sink_col is not None:
        unique_sinks = int(dl[sink_col].dropna().astype(str).str.strip().replace('', pd.NA).nunique())
    # copy activities: prefer an exact 'Type' column (this contains 'Copy'/'DataFlow'),
    # otherwise fall back to Activity-name heuristics
    type_col = None
    for c in dl.columns:
        if str(c).strip().lower() == 'type':
            type_col = c
            break
    if type_col is None:
        for c in dl.columns:
            lc = str(c).lower()
            if 'activity' in lc or 'transformation' in lc:
                type_col = c
                break

    if type_col is not None:
        # If the Type column contains canonical values like 'Copy' and 'DataFlow', count exact 'Copy'
        vals = dl[type_col].astype(str).str.strip()
        copy_activities = int((vals.str.lower() == 'copy').sum())
    else:
        # Fallback: look for 'copy' tokens inside any column values (less accurate)
        found = 0
        for c in dl.columns:
            try:
                found += int(dl[c].astype(str).str.lower().str.contains('copy', na=False).sum())
            except Exception:
                continue
        copy_activities = found

# Print report
print('\nVerification report for workbook:', wb)
print('\nImpact counts:')
for k,v in imp_counts.items():
    print(f'  {k}: {v}')
print('\nOrphaned resources:')
print(f'  Orphaned Pipelines: {orph_pipelines}  (sheet: {kp})')
print(f'  Orphaned Datasets: {orph_datasets}  (sheet: {kd})')
print(f'  Orphaned Services: {orph_services}  (sheet: {ks})')
print(f'  Broken/Inactive Triggers: {broken_triggers}')
print('\nDataLineage stats:')
print(f'  Total Lineage Records: {total_lineage}')
print(f'  Unique Sources: {unique_sources}')
print(f'  Unique Sinks: {unique_sinks}')
print(f'  Copy Activities: {copy_activities}')

# exit 0
