import json
from pathlib import Path

out_dir = Path(__file__).resolve().parent.parent / 'output'

# Load per-run validator JSONs
runs = {}
for i in range(1, 11):
    p = out_dir / f'validator_summary_run_{i}.json'
    if not p.exists():
        print(f'Missing file: {p}')
        continue
    try:
        data = json.load(p.open('r', encoding='utf-8'))
    except Exception as e:
        print(f'Error parsing {p}:', e)
        data = None
    runs[i] = data

# Normalize to metric->value
metrics_per_run = {}
for i, data in runs.items():
    metrics_per_run[i] = {}
    if not isinstance(data, dict):
        metrics_per_run[i]['__parse_error__'] = True
        continue
    for k, v in data.items():
        if isinstance(v, dict) and 'value' in v:
            metrics_per_run[i][k] = v['value']
        else:
            metrics_per_run[i][k] = v

# Check consistency across runs
all_metrics = set()
for m in metrics_per_run.values():
    all_metrics.update(m.keys())

inconsistencies = {}
for metric in sorted(all_metrics):
    values = {}
    for i in sorted(metrics_per_run.keys()):
        v = metrics_per_run[i].get(metric, None)
        values.setdefault(str(v), []).append(i)
    if len(values) > 1:
        inconsistencies[metric] = values

print('Consistency check across 10 runs:')
if not inconsistencies:
    print('  All metrics identical across runs.')
else:
    print('  Found inconsistencies for these metrics:')
    for metric, valmap in inconsistencies.items():
        print(f'    {metric}:')
        for val, runs_list in valmap.items():
            print(f'      {val} -> runs {runs_list}')

# Compare to armv10/test2.json
template_p = Path(__file__).resolve().parent.parent / 'test2.json'
if not template_p.exists():
    print('\nTemplate test2.json not found at', template_p)
else:
    j = json.load(template_p.open('r', encoding='utf-8'))
    counts = {'Pipelines':0,'Datasets':0,'DataFlows':0,'Triggers':0}
    for r in j.get('resources', []):
        t = r.get('type','').lower()
        if 'factories/pipelines' in t:
            counts['Pipelines'] += 1
        if 'factories/datasets' in t:
            counts['Datasets'] += 1
        if 'factories/dataflows' in t:
            counts['DataFlows'] += 1
        if 'factories/triggers' in t:
            counts['Triggers'] += 1

    print('\nCounts from armv10/test2.json:')
    for k,v in counts.items():
        print(f'  {k}: {v}')

    # Compare to validator (take run 1 as canonical)
    if 1 in metrics_per_run:
        print('\nValidator (run 1) vs test2.json diffs:')
        for k in ['Pipelines','DataFlows','Datasets','Triggers']:
            val = metrics_per_run[1].get(k)
            print(f'  {k}: validator={val} template={counts.get(k)} diff={ (val if isinstance(val,(int,float)) else 0) - counts.get(k,0)}')
    else:
        print('\nNo validator run 1 data available to compare.')

# Print sample of totals from run 1
if 1 in metrics_per_run:
    print('\nSample totals from validator run 1:')
    for k in ['Total Source Files','Total Target Files','Total Source Tables','Total Target Tables']:
        print(f'  {k}:', metrics_per_run[1].get(k))

print('\nDone.')
