"""
Validate dashboard tile values against an ADF Analyzer Excel workbook.

Usage:
    python validate_tiles_fixed.py path/to/analysis.xlsx [--dump] [--top N]

This script loads the workbook, applies the same heuristics used by the dashboard
and prints each tile name, the raw source used and the computed value in a
human-readable and JSON summary form.

The script intentionally mirrors the dashboard logic (fallbacks, Summary coercion,
lineage table heuristics) so you can run it locally and verify what the dashboard
will display.

"""
import sys
import json
import re
from pathlib import Path
from typing import Dict, Any, List, Tuple
from collections import Counter

import argparse
import pandas as pd

def load_workbook(path: str) -> Dict[str, pd.DataFrame]:
    return pd.read_excel(path, sheet_name=None)

def normalize_key(k: str) -> str:
    return re.sub(r"[_\s]+", "", str(k)).lower()

def coerce_summary_values(summary_df: pd.DataFrame) -> Dict[str, Any]:
    raw = {}
    if summary_df is None or summary_df.empty:
        return raw
    if "Metric" not in summary_df.columns or "Value" not in summary_df.columns:
        return raw
    for _, row in summary_df.iterrows():
        key = str(row["Metric"]).strip()
        val = row["Value"]
        raw[key] = val

    metrics = {}
    for k, v in raw.items():
        try:
            if pd.isna(v):
                metrics[k] = v
                continue
            if isinstance(v, str):
                s = v.strip().replace(",", "")
                if s.endswith("%"):
                    try:
                        metrics[k] = float(s.rstrip("%")) / 100.0
                        continue
                    except Exception:
                        pass
            num = pd.to_numeric(v, errors="coerce")
            if not pd.isna(num):
                if float(num).is_integer():
                    metrics[k] = int(num)
                else:
                    metrics[k] = float(num)
            else:
                metrics[k] = v
        except Exception:
            metrics[k] = v
    return metrics

def get_summary_metric(metrics: Dict[str, Any], name: str, default=0):
    return metrics.get(name, default)

def get_count_with_fallback(excel: Dict[str, pd.DataFrame], metrics: Dict[str, Any], metric_name: str, fallback_sheets):
    val = get_summary_metric(metrics, metric_name, 0)
    try:
        if isinstance(val, (int, float)) and not isinstance(val, bool):
            if int(val) > 0:
                return int(val), f"Summary::{metric_name}"
    except Exception:
        pass
    # fallback to sheet counts
    for s in fallback_sheets:
        # try exact
        if s in excel and isinstance(excel[s], pd.DataFrame) and not excel[s].empty:
            return len(excel[s]), f"Sheet::{s}"
    # try normalized match
    target_norm = re.sub(r"[_\s]+", "", metric_name).lower()
    for key, df in excel.items():
        if normalize_key(key) == target_norm and isinstance(df, pd.DataFrame) and not df.empty:
            return len(df), f"Sheet::{key}"
    # dependency graph fallback not available here
    return 0, "None"

def aggregate_unique(dfs, candidates):
    values = set()
    for df in dfs:
        if df is None or df.empty:
            continue
        for c in candidates:
            if c in df.columns:
                vals = df[c].dropna().astype(str).str.strip()
                values.update(vals[vals != ""].unique().tolist())
                break
    return len(values)

def extract_values_counter(dfs: List[pd.DataFrame], candidates: List[str]) -> Counter:
    """Return a Counter of values found across candidate columns in given dfs."""
    ctr = Counter()
    for df in dfs:
        if df is None or df.empty:
            continue
        for c in candidates:
            if c in df.columns:
                vals = df[c].dropna().astype(str).str.strip()
                vals = vals[vals != ""]
                ctr.update(vals.tolist())
                break
    return ctr

def is_dynamic_value(v: str) -> bool:
    """Heuristic to detect dynamic/parameterized expressions in a value."""
    if not isinstance(v, str) or v.strip() == "":
        return False
    s = v.strip()
    # common parameter/expression markers in ADF: @{...}, ${...}, pipeline(...), parameters(...), concat(...)
    if re.search(r"@\{|\$\{|parameters\(|pipeline\(|concat\(|\{\{.*\}\}", s, re.IGNORECASE):
        return True
    # also treat values containing evaluation markers like '[' ']' for array indexes or expressions
    if "@" in s and "{" in s:
        return True
    return False

def dump_top_values(ctr: Counter, top_n: int) -> List[Tuple[str, int, bool]]:
    """Return list of (value, count, is_dynamic) for top_n items."""
    out = []
    for val, cnt in ctr.most_common(top_n):
        out.append((val, cnt, is_dynamic_value(val)))
    return out

def main():
    parser = argparse.ArgumentParser(description="Validate dashboard tile values from an ADF Analyzer Excel workbook.")
    parser.add_argument("workbook", help="Path to the Excel workbook")
    parser.add_argument("--dump", action="store_true", help="Dump top source/target values and counts")
    parser.add_argument("--top", type=int, default=20, help="Top N values to dump when --dump is used")
    args = parser.parse_args()

    path = args.workbook
    if not Path(path).exists():
        print("File not found:", path)
        sys.exit(1)

    excel = load_workbook(path)
    summary_df = None
    for k in excel.keys():
        if normalize_key(k) == "summary":
            summary_df = excel[k]
            break

    metrics = coerce_summary_values(summary_df)

    report = {}

    pipelines, src = get_count_with_fallback(excel, metrics, "Pipelines", ["ImpactAnalysis", "PipelineAnalysis", "Pipeline_Analysis", "Pipelines"])
    dataflows, src2 = get_count_with_fallback(excel, metrics, "DataFlows", ["DataFlows", "DataFlowLineage", "DataFlow_Summary"])
    datasets, _ = get_count_with_fallback(excel, metrics, "Datasets", ["Datasets"])
    triggers, _ = get_count_with_fallback(excel, metrics, "Triggers", ["TriggerDetails", "Triggers"])
    dependencies, _ = get_count_with_fallback(excel, metrics, "Total Dependencies", ["ActivityExecutionOrder", "DataLineage", "Pipeline_Pipeline", "Pipeline_DataFlow"])
    orphaned, _ = get_count_with_fallback(excel, metrics, "Orphaned Pipelines", ["OrphanedPipelines", "Orphaned_Pipelines"])

    if pipelines > 0:
        health = int((1 - orphaned / pipelines) * 100)
    else:
        health = 100

    report['Pipelines'] = {'value': pipelines, 'source': src}
    report['DataFlows'] = {'value': dataflows, 'source': src2}
    report['Datasets'] = {'value': datasets}
    report['Triggers'] = {'value': triggers}
    report['Dependencies'] = {'value': dependencies}
    report['OrphanedPipelines'] = {'value': orphaned}
    report['Health'] = {'value': health, 'formula': 'int((1 - orphaned/pipelines)*100) if pipelines>0 else 100'}

    # lineage dfs
    lineage = None
    dflow_lineage = None
    for k, df in excel.items():
        nk = normalize_key(k)
        if nk in ("datalineage", "datalineage"):
            lineage = df
        if nk in ("dataflowlineage", "dataflow_lineage"):
            dflow_lineage = df
    # try both common names and prefer non-empty DataFrames
    if lineage is None or (isinstance(lineage, pd.DataFrame) and lineage.empty):
        lineage = None
        for candidate in ('DataLineage', 'Data_Lineage'):
            if candidate in excel and isinstance(excel[candidate], pd.DataFrame) and not excel[candidate].empty:
                lineage = excel[candidate]
                break

    if dflow_lineage is None or (isinstance(dflow_lineage, pd.DataFrame) and dflow_lineage.empty):
        dflow_lineage = None
        for candidate in ('DataFlowLineage', 'DataFlow_Lineage'):
            if candidate in excel and isinstance(excel[candidate], pd.DataFrame) and not excel[candidate].empty:
                dflow_lineage = excel[candidate]
                break

    if lineage is None or (isinstance(lineage, pd.DataFrame) and lineage.empty):
        lineage = pd.DataFrame()
    if dflow_lineage is None or (isinstance(dflow_lineage, pd.DataFrame) and dflow_lineage.empty):
        dflow_lineage = pd.DataFrame()

    total_source_files = aggregate_unique([lineage, dflow_lineage], ["SourceFile", "Source_File", "SourceFilename", "SourceName", "Source"])
    total_target_files = aggregate_unique([lineage, dflow_lineage], ["TargetFile", "Target_File", "TargetFilename", "SinkName", "Sink"])
    total_source_tables = aggregate_unique([lineage, dflow_lineage], ["SourceTable", "Source_Table"])
    total_target_tables = aggregate_unique([lineage, dflow_lineage], ["SinkTable", "Sink_Table"])

    report['Total Source Files'] = {'value': total_source_files}
    report['Total Target Files'] = {'value': total_target_files}
    report['Total Source Tables'] = {'value': total_source_tables}
    report['Total Target Tables'] = {'value': total_target_tables}

    # optionally dump top values
    if args.dump:
        from pprint import pprint

        src_candidates = ["SourceFile", "Source_File", "SourceFilename", "SourceName", "Source", "SourceTable", "Source_Table"]
        tgt_candidates = ["TargetFile", "Target_File", "TargetFilename", "TargetName", "Target", "Sink", "SinkName", "SinkTable", "Sink_Table"]

        src_ctr = extract_values_counter([lineage, dflow_lineage], src_candidates)
        tgt_ctr = extract_values_counter([lineage, dflow_lineage], tgt_candidates)

        print("\nTop Source values:")
        for val, cnt, dyn in dump_top_values(src_ctr, args.top):
            print(f"{cnt:6d}  {'[DYN]' if dyn else '     '}  {val}")

        print("\nTop Target values:")
        for val, cnt, dyn in dump_top_values(tgt_ctr, args.top):
            print(f"{cnt:6d}  {'[DYN]' if dyn else '     '}  {val}")

    print(json.dumps(report, indent=2))

if __name__ == '__main__':
    main()
