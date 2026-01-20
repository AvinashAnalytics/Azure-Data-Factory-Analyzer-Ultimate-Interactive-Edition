import pandas as pd
import re
import sys
from pathlib import Path

excel_file = Path('output') / 'adf_analysis_latest.xlsx'
if not excel_file.exists():
    print(f"ERROR: Excel file not found: {excel_file.resolve()}")
    sys.exit(2)

print(f"Loading: {excel_file}\n")

xl = pd.ExcelFile(excel_file)
sheet_names = xl.sheet_names
print(f"Found {len(sheet_names)} sheets:\n")
for s in sheet_names:
    print(f" - {s}")

# Load sheets
print('\nReading sheets into memory...')
data = {}
for name in sheet_names:
    try:
        data[name] = pd.read_excel(xl, sheet_name=name)
    except Exception as e:
        print(f"  ⚠ Could not read {name}: {e}")

# Detect split parts
print('\nDetecting split parts (pattern: <Base>_P1, _P2 ... )')
groups = {}
for name in list(data.keys()):
    m = re.match(r"^(.+)_P(\d+)$", name, re.IGNORECASE)
    if m:
        base = m.group(1)
        idx = int(m.group(2))
        groups.setdefault(base, []).append((idx, name))

for base, parts in groups.items():
    parts.sort()
    print(f"  -> Found split for '{base}': parts = {[p for (_, p) in parts]}")
    try:
        frames = [data[p] for (_, p) in parts if p in data]
        merged = pd.concat(frames, ignore_index=True)
        if base not in data:
            data[base] = merged
            print(f"     Created merged key: '{base}' ({len(merged)} rows)")
        else:
            data[f"{base}_MERGED"] = merged
            print(f"     Created alias key: '{base}_MERGED' ({len(merged)} rows)")
    except Exception as e:
        print(f"     ⚠ Failed to merge parts for {base}: {e}")

# Normalize keys
print('\nNormalizing sheet keys (strip underscores/spaces, lowercased)')
def norm(k: str) -> str:
    return re.sub(r"[_\s]+", "", str(k)).lower()

norm_map = {}
for key in list(data.keys()):
    try:
        n = norm(key)
        if n not in norm_map:
            norm_map[n] = key
    except Exception:
        continue

# expected analyzer sheet names
expected = [
    'Summary','PipelineAnalysis','Pipelines','Activities','ActivityCount','ActivityExecutionOrder',
    'DataFlows','DataFlowLineage','DataFlowTransformations','Datasets','LinkedServices','Triggers','TriggerDetails',
    'IntegrationRuntimes','DataLineage','ImpactAnalysis','CircularDependencies','OrphanedPipelines','OrphanedDataFlows',
    'OrphanedDatasets','OrphanedLinkedServices','OrphanedTriggers','DatasetUsage','LinkedServiceUsage',
    'IntegrationRuntimeUsage','TransformationUsage','GlobalParameterUsage','Statistics','FactoryInfo','GlobalParameters',
    'Credentials','ManagedVNets','ManagedPrivateEndpoints','Errors','DataDictionary'
]

print('\nChecking expected sheets...')
missing = []
found = []
for s in expected:
    if s in data:
        found.append(s)
        print(f"  ✓ {s} (exact)")
        continue
    ns = norm(s)
    if ns in norm_map:
        found.append(s)
        print(f"  ✓ {s} (matched -> {norm_map[ns]})")
        continue
    # also try plural/singular tweaks
    alt = s.rstrip('s')
    if alt in data or norm(alt) in norm_map:
        found.append(s)
        print(f"  ✓ {s} (matched alt -> {alt})")
        continue
    missing.append(s)
    print(f"  ✗ {s}")

print('\nSummary:')
print(f"  Found: {len(found)}")
print(f"  Missing: {len(missing)}")
if missing:
    print('\nMissing sheets:')
    for m in missing:
        print(f"   - {m}")

# show top-level keys after normalization
print('\nFinal sheet keys available (sample 50):')
for i, k in enumerate(list(data.keys())[:50], 1):
    print(f" {i:02}. {k}")

print('\nDone.')
