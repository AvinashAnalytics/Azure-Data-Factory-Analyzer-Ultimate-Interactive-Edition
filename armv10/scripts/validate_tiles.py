"""
Clean validator for dashboard tiles. Mirrors the dashboard heuristics and
supports --dump, --top, and --csv.
"""

import argparse
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd


def load_workbook(path: str) -> Dict[str, pd.DataFrame]:
    return pd.read_excel(path, sheet_name=None)


def normalize_key(k: str) -> str:
    return re.sub(r"[_\s]+", "", str(k)).lower()


def coerce_summary_values(df: pd.DataFrame) -> Dict[str, Any]:
    if df is None or getattr(df, "empty", True):
        return {}

    raw = {}
    cols = {normalize_key(c): c for c in df.columns}
    metric_col = cols.get("metric", df.columns[0])
    value_col = cols.get("value", df.columns[1] if len(df.columns) > 1 else df.columns[0])

    for _, row in df.iterrows():
        key = str(row[metric_col]).strip()
        val = row[value_col]
        raw[key] = val

    metrics: Dict[str, Any] = {}
    for k, v in raw.items():
        try:
            if pd.isna(v):
                metrics[k] = None
                continue
            if isinstance(v, str):
                s = v.strip().replace(",", "")
                if s.endswith("%"):
                    try:
                        metrics[k] = float(s.rstrip("%")) / 100.0
                        continue
                    except Exception:
                        pass
                num = pd.to_numeric(s, errors="coerce")
            else:
                num = pd.to_numeric(v, errors="coerce")

            if pd.isna(num):
                metrics[k] = v
            else:
                if float(num).is_integer():
                    metrics[k] = int(num)
                else:
                    metrics[k] = float(num)
        except Exception:
            metrics[k] = v
    return metrics


def get_summary_metric(metrics: Dict[str, Any], name: str, default=0):
    return metrics.get(name, default)


def get_count_with_fallback(excel: Dict[str, pd.DataFrame], metrics: Dict[str, Any], metric_name: str, fallback_sheets: List[str]) -> Tuple[int, str]:
    val = get_summary_metric(metrics, metric_name, 0)
    try:
        if isinstance(val, (int, float)) and not isinstance(val, bool):
            if int(val) > 0:
                return int(val), f"Summary::{metric_name}"
    except Exception:
        pass

    for s in fallback_sheets:
        if s in excel and isinstance(excel[s], pd.DataFrame) and not excel[s].empty:
            return len(excel[s]), f"Sheet::{s}"

    target_norm = re.sub(r"[_\s]+", "", metric_name).lower()
    for key, df in excel.items():
        if normalize_key(key) == target_norm and isinstance(df, pd.DataFrame) and not df.empty:
            return len(df), f"Sheet::{key}"

    return 0, "None"


def aggregate_unique(dfs: List[pd.DataFrame], candidates: List[str]) -> int:
    values = set()
    for df in dfs:
        if df is None or getattr(df, "empty", True):
            continue
        for c in candidates:
            if c in df.columns:
                vals = df[c].dropna().astype(str).str.strip()
                values.update([v for v in vals if v != ""])
                break
    return len(values)


def extract_values_counter(dfs: List[pd.DataFrame], candidates: List[str]) -> Counter:
    ctr = Counter()
    for df in dfs:
        if df is None or getattr(df, "empty", True):
            continue
        for c in candidates:
            if c in df.columns:
                vals = df[c].dropna().astype(str).str.strip()
                vals = [v for v in vals if v != ""]
                ctr.update(vals)
                break
    return ctr


def is_dynamic_value(v: str) -> bool:
    if not isinstance(v, str) or v.strip() == "":
        return False
    s = v.strip()
    if re.search(r"@\{|\$\{|parameters\(|pipeline\(|concat\(|\{\{.*\}\}", s, re.IGNORECASE):
        return True
    if "@" in s and "{" in s:
        return True
    return False


def dump_top_values(ctr: Counter, top_n: int) -> List[Tuple[str, int, bool]]:
    out = []
    for val, cnt in ctr.most_common(top_n):
        out.append((val, cnt, is_dynamic_value(val)))
    return out


def counters_to_dataframe(src_ctr: Counter, tgt_ctr: Counter) -> pd.DataFrame:
    rows = []
    for val, cnt in src_ctr.items():
        rows.append({"type": "source", "value": val, "count": cnt, "dynamic": is_dynamic_value(val)})
    for val, cnt in tgt_ctr.items():
        rows.append({"type": "target", "value": val, "count": cnt, "dynamic": is_dynamic_value(val)})
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(["type", "count"], ascending=[True, False])
    return df


def main():
    parser = argparse.ArgumentParser(description="Validate dashboard tile values from an ADF Analyzer Excel workbook.")
    parser.add_argument("workbook", help="Path to the Excel workbook")
    parser.add_argument("--dump", action="store_true", help="Dump top source/target values and counts")
    parser.add_argument("--top", type=int, default=20, help="Top N values to dump when --dump is used")
    parser.add_argument("--csv", help="Optional path to write dataset breakdown CSV")
    args = parser.parse_args()

    path = args.workbook
    if not Path(path).exists():
        print("File not found:", path)
        return 2

    excel = load_workbook(path)

    # find summary sheet
    summary_df = None
    for k, df in excel.items():
        if normalize_key(k) == "summary":
            summary_df = df
            break

    metrics = coerce_summary_values(summary_df)

    report: Dict[str, Any] = {}

    pipelines, src = get_count_with_fallback(excel, metrics, "Pipelines", ["ImpactAnalysis", "PipelineAnalysis", "Pipeline_Analysis", "Pipelines"])
    dataflows, src2 = get_count_with_fallback(excel, metrics, "DataFlows", ["DataFlows", "DataFlowLineage", "DataFlow_Summary"])
    datasets, _ = get_count_with_fallback(excel, metrics, "Datasets", ["Datasets"])

    # Special handling for Triggers: prefer the canonical `Triggers` sheet (one row per trigger)
    # otherwise fall back to unique trigger names in `TriggerDetails` (which can contain many rows per trigger)
    triggers = 0
    triggers_src = "None"
    if "Triggers" in excel and not getattr(excel["Triggers"], "empty", True):
        triggers = len(excel["Triggers"])
        triggers_src = "Sheet::Triggers"
    elif "TriggerDetails" in excel and not getattr(excel["TriggerDetails"], "empty", True):
        td = excel["TriggerDetails"]
        # try to find a sensible name column
        cand = None
        for c in ['Trigger', 'TriggerName', 'triggername', 'trigger_name', 'Name']:
            if c in td.columns:
                cand = c
                break
        if cand is not None:
            triggers = td[cand].dropna().astype(str).str.strip().nunique()
            triggers_src = f"Sheet::TriggerDetails::unique({cand})"
        else:
            triggers = len(td)
            triggers_src = "Sheet::TriggerDetails"
    else:
        triggers, triggers_src = get_count_with_fallback(excel, metrics, "Triggers", ["TriggerDetails", "Triggers"])
    dependencies, _ = get_count_with_fallback(excel, metrics, "Total Dependencies", ["ActivityExecutionOrder", "DataLineage", "Pipeline_Pipeline", "Pipeline_DataFlow"])
    orphaned, _ = get_count_with_fallback(excel, metrics, "Orphaned Pipelines", ["OrphanedPipelines", "Orphaned_Pipelines"])

    if pipelines > 0:
        health = int((1 - orphaned / pipelines) * 100)
    else:
        health = 100

    report["Pipelines"] = {"value": pipelines, "source": src}
    report["DataFlows"] = {"value": dataflows, "source": src2}
    report["Datasets"] = {"value": datasets}
    report["Triggers"] = {"value": triggers}
    report["Dependencies"] = {"value": dependencies}
    report["OrphanedPipelines"] = {"value": orphaned}
    report["Health"] = {"value": health, "formula": "int((1 - orphaned/pipelines)*100) if pipelines>0 else 100"}

    lineage = None
    dflow_lineage = None
    for k, df in excel.items():
        nk = normalize_key(k)
        if nk in ("datalineage", "datalineage"):
            lineage = df
        if nk in ("dataflowlineage", "dataflow_lineage"):
            dflow_lineage = df

    if lineage is None or getattr(lineage, "empty", True):
        if "DataLineage" in excel and not getattr(excel["DataLineage"], "empty", True):
            lineage = excel["DataLineage"]
        elif "Data_Lineage" in excel and not getattr(excel["Data_Lineage"], "empty", True):
            lineage = excel["Data_Lineage"]
        else:
            lineage = pd.DataFrame()

    if dflow_lineage is None or getattr(dflow_lineage, "empty", True):
        if "DataFlowLineage" in excel and not getattr(excel["DataFlowLineage"], "empty", True):
            dflow_lineage = excel["DataFlowLineage"]
        elif "DataFlow_Lineage" in excel and not getattr(excel["DataFlow_Lineage"], "empty", True):
            dflow_lineage = excel["DataFlow_Lineage"]
        else:
            dflow_lineage = pd.DataFrame()

    total_source_files = aggregate_unique([lineage, dflow_lineage], ["SourceFile", "Source_File", "SourceFilename", "SourceName", "Source"])
    total_target_files = aggregate_unique([lineage, dflow_lineage], ["TargetFile", "Target_File", "TargetFilename", "SinkName", "Sink"])
    total_source_tables = aggregate_unique([lineage, dflow_lineage], ["SourceTable", "Source_Table"])
    total_target_tables = aggregate_unique([lineage, dflow_lineage], ["SinkTable", "Sink_Table"])

    report["Total Source Files"] = {"value": total_source_files}
    report["Total Target Files"] = {"value": total_target_files}
    report["Total Source Tables"] = {"value": total_source_tables}
    report["Total Target Tables"] = {"value": total_target_tables}

    src_candidates = ["SourceFile", "Source_File", "SourceFilename", "SourceName", "Source", "SourceTable", "Source_Table"]
    tgt_candidates = ["TargetFile", "Target_File", "TargetFilename", "TargetName", "Target", "Sink", "SinkName", "SinkTable", "Sink_Table"]

    src_ctr = extract_values_counter([lineage, dflow_lineage], src_candidates)
    tgt_ctr = extract_values_counter([lineage, dflow_lineage], tgt_candidates)

    if args.dump:
        print("\nTop Source values:")
        for val, cnt, dyn in dump_top_values(src_ctr, args.top):
            print(f"{cnt:6d}  {'[DYN]' if dyn else '     '}  {val}")

        print("\nTop Target values:")
        for val, cnt, dyn in dump_top_values(tgt_ctr, args.top):
            print(f"{cnt:6d}  {'[DYN]' if dyn else '     '}  {val}")

    if args.csv:
        out_path = Path(args.csv)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df = counters_to_dataframe(src_ctr, tgt_ctr)
        df.to_csv(out_path, index=False)
        print(f"\nWrote dataset breakdown CSV to: {out_path}")

    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()

