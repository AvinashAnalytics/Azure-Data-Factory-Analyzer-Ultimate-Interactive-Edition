#!/usr/bin/env python3
"""
Cross-verify analyzer workbook metrics:
- Run canonical validator (scripts/validate_tiles.py) and capture its JSON summary
- Read the workbook and compute dashboard-style metrics (summary coercion + fallbacks)
- Compare validator vs summary vs computed values and write a report

Outputs:
 - output/cross_verify_report.json
 - output/cross_verify_report.txt

Usage: python scripts/cross_verify_all.py path/to/adf_analysis_latest.xlsx
"""
import sys
import json
from pathlib import Path
import subprocess
import re
import pandas as pd

base = Path(__file__).resolve().parent
out_dir = base.parent / "output"
out_dir.mkdir(parents=True, exist_ok=True)

wb = base / "output" / "adf_analysis_latest.xlsx"
if len(sys.argv) > 1 and sys.argv[1].strip():
    wb = Path(sys.argv[1])

if not wb.exists():
    print(f"Workbook not found: {wb}")
    sys.exit(2)

def coerce_summary(df: pd.DataFrame) -> dict:
    if df is None or df.empty or 'Metric' not in df.columns or 'Value' not in df.columns:
        return {}
    raw = df.set_index('Metric')['Value'].to_dict()
    out = {}
    for k,v in raw.items():
        try:
            if pd.isna(v):
                out[k] = None
                continue
            if isinstance(v, str):
                s = v.strip().replace(',', '')
                if s.endswith('%'):
                    try:
                        out[k] = float(s.rstrip('%'))/100.0
                        continue
                    except Exception:
                        pass
            num = pd.to_numeric(v, errors='coerce')
            if not pd.isna(num):
                if float(num).is_integer():
                    out[k] = int(num)
                else:
                    out[k] = float(num)
            else:
                out[k] = v
        except Exception:
            out[k] = v
    return out

def get_dataframe_by_names(xl, names):
    for n in names:
        if n in xl:
            return xl[n]
    # try normalized
    def norm(s):
        return re.sub(r"[_\s]+", "", s).lower()
    target = [norm(n) for n in names]
    for k,df in xl.items():
        if norm(k) in target:
            return df
    return pd.DataFrame()

def sum_numeric_columns_by_keywords(df, keywords):
    if df is None or df.empty:
        return 0
    total = 0
    for col in df.columns:
        try:
            name = str(col).lower()
            if any(k.lower() in name for k in keywords):
                total += int(pd.to_numeric(df[col], errors='coerce').fillna(0).sum())
        except Exception:
            continue
    return int(total)

print(f"Loading workbook: {wb}")
xl = pd.read_excel(wb, sheet_name=None)

# summary coercion
summary_df = get_dataframe_by_names(xl, ['Summary', 'summary'])
summary_metrics = coerce_summary(summary_df)

# run canonical validator
validator = base / 'validate_tiles.py'
validator_json = None
try:
    proc = subprocess.run([sys.executable, str(validator), str(wb), '--csv', str(out_dir / 'validator_breakdowns.csv')], capture_output=True, text=True)
    stdout = proc.stdout + proc.stderr
    # find first JSON object in output
    m = re.search(r"(\{\s*\"Pipelines\"[\s\S]*\})", stdout)
    if m:
        validator_json = json.loads(m.group(1))
    else:
        # fallback: try to load from printed file
        print("Could not parse validator JSON from stdout; falling back to reading CSV and basic counts")
except Exception as e:
    print(f"Failed to run validator: {e}")

# compute dashboard-like metrics
def compute_counts(xl):
    report = {}
    # Pipelines
    pipelines = summary_metrics.get('Pipelines') if 'Pipelines' in summary_metrics else None
    if not pipelines:
        df = get_dataframe_by_names(xl, ['ImpactAnalysis','PipelineAnalysis','Pipeline_Analysis','Pipelines'])
        pipelines = len(df) if isinstance(df, pd.DataFrame) and not df.empty else 0
    report['Pipelines'] = pipelines

    # DataFlows
    dataflows = summary_metrics.get('DataFlows') if 'DataFlows' in summary_metrics else None
    if not dataflows:
        df = get_dataframe_by_names(xl, ['DataFlows','DataFlowLineage','DataFlow_Summary'])
        dataflows = len(df) if isinstance(df, pd.DataFrame) and not df.empty else 0
    report['DataFlows'] = dataflows

    # Datasets
    datasets = summary_metrics.get('Datasets') if 'Datasets' in summary_metrics else None
    if not datasets:
        df = get_dataframe_by_names(xl, ['Datasets'])
        datasets = len(df) if isinstance(df, pd.DataFrame) and not df.empty else 0
    report['Datasets'] = datasets

    # Triggers: prefer Triggers sheet, else unique name in TriggerDetails
    tr = get_dataframe_by_names(xl, ['Triggers'])
    triggers = 0
    if not tr.empty:
        triggers = len(tr)
    else:
        td = get_dataframe_by_names(xl, ['TriggerDetails'])
        if not td.empty:
            cand = None
            for c in td.columns:
                if 'trigger' in str(c).lower() and 'name' in str(c).lower():
                    cand = c
                    break
            if cand:
                triggers = int(td[cand].dropna().astype(str).str.strip().nunique())
            else:
                triggers = len(td)
    report['Triggers'] = triggers

    # Dependencies: look for ActivityExecutionOrder or ActivityExecution or count edges from ActivityExecutionOrder
    deps_df = get_dataframe_by_names(xl, ['ActivityExecutionOrder','ActivityExecutionOrder','ActivityExecution'])
    if not deps_df.empty:
        deps = len(deps_df)
    else:
        deps = 0
    report['Dependencies'] = deps

    # Orphaned pipelines
    orp = get_dataframe_by_names(xl, ['OrphanedPipelines','Orphaned_Pipelines'])
    report['OrphanedPipelines'] = len(orp) if not orp.empty else 0

    # Health
    try:
        p = int(report['Pipelines']) if report['Pipelines'] else 0
        o = int(report['OrphanedPipelines']) if report['OrphanedPipelines'] else 0
        report['Health'] = int((1 - o / p) * 100) if p>0 else 100
    except Exception:
        report['Health'] = summary_metrics.get('Health')

    # Lineage-based totals
    lineage = get_dataframe_by_names(xl, ['DataLineage','Data_Lineage'])
    dflow_lineage = get_dataframe_by_names(xl, ['DataFlowLineage','DataFlow_Lineage'])

    def agg_unique(dfs, candidates):
        vals = set()
        for d in dfs:
            if d is None or getattr(d,'empty',True):
                continue
            for c in candidates:
                if c in d.columns:
                    vals.update([str(v).strip() for v in d[c].dropna().astype(str).tolist() if str(v).strip()!=''])
                    break
        return len(vals)

    src_file_cols = ['SourceFile','Source_File','SourceFilename','SourceName','Source']
    tgt_file_cols = ['TargetFile','Target_File','TargetFilename','SinkName','Sink']
    src_table_cols = ['SourceTable','Source_Table']
    tgt_table_cols = ['SinkTable','Sink_Table']

    report['Total Source Files'] = agg_unique([lineage,dflow_lineage], src_file_cols)
    report['Total Target Files'] = agg_unique([lineage,dflow_lineage], tgt_file_cols)
    report['Total Source Tables'] = agg_unique([lineage,dflow_lineage], src_table_cols)
    report['Total Target Tables'] = agg_unique([lineage,dflow_lineage], tgt_table_cols)

    return report

computed = compute_counts(xl)

# Build cross-verify report
report = {
    'workbook': str(wb),
    'summary_metrics': summary_metrics,
    'validator': validator_json,
    'computed': computed,
}

out_json = out_dir / 'cross_verify_report.json'
out_txt = out_dir / 'cross_verify_report.txt'
out_json.write_text(json.dumps(report, indent=2, default=str), encoding='utf-8')

with out_txt.open('w', encoding='utf-8') as f:
    f.write(f"Cross-verify report for: {wb}\n\n")
    f.write("Validator (canonical)\n")
    f.write(json.dumps(validator_json or {}, indent=2))
    f.write("\n\nWorkbook Summary sheet (coerced)\n")
    f.write(json.dumps(summary_metrics, indent=2))
    f.write("\n\nComputed (dashboard heuristics)\n")
    f.write(json.dumps(computed, indent=2))

print(f"Wrote cross-verify JSON: {out_json}")
print(f"Wrote cross-verify text: {out_txt}")
