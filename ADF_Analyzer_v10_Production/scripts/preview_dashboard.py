#!/usr/bin/env python3
"""
Simple preview generator for the analyzer workbook.
Writes armv10/output/preview_report.md and prints a short summary to stdout.
"""
import json
from pathlib import Path
import pandas as pd
import sys

base = Path(__file__).resolve().parent
output_dir = base / "output"
output_dir.mkdir(parents=True, exist_ok=True)

# default workbook path
wb_path = base / "output" / "adf_analysis_latest.xlsx"
if len(sys.argv) > 1 and sys.argv[1].strip():
    wb_path = Path(sys.argv[1])

out_md = output_dir / "preview_report.md"

if not wb_path.exists():
    msg = f"Workbook not found: {wb_path}\n"
    print(msg)
    sys.exit(2)

try:
    x = pd.read_excel(wb_path, sheet_name=None)
except Exception as e:
    print(f"Failed to read workbook: {e}")
    sys.exit(3)

lines = []
lines.append(f"# Analyzer Preview Report\n")
lines.append(f"Generated: {pd.Timestamp.now()}\n")
lines.append(f"Workbook: `{wb_path}`\n")
lines.append("\n## Sheets\n")
for s in x.keys():
    lines.append(f"- {s} (rows: {len(x[s])})\n")

# Summary sheet
lines.append("\n## Summary (top metrics)\n")
if "Summary" in x:
    try:
        s = x["Summary"]
        if "Metric" in s.columns and "Value" in s.columns:
            top = s.head(20)[["Metric","Value"]]
            for _, r in top.iterrows():
                lines.append(f"- **{r['Metric']}**: {r['Value']}\n")
        else:
            lines.append("Summary sheet found but no Metric/Value columns; showing first 5 rows:\n")
            lines.append(s.head().to_markdown() + "\n")
    except Exception as e:
        lines.append(f"Failed to summarize Summary sheet: {e}\n")
else:
    lines.append("No Summary sheet found.\n")

# pick a few key sheets to preview
for name in ["DataLineage", "DataFlowLineage", "ImpactAnalysis", "Pipelines", "DataFlows", "Datasets"]:
    if name in x:
        lines.append(f"\n## {name} (first 5 rows)\n")
        try:
            df = x[name].head(5)
            lines.append(df.to_markdown() + "\n")
        except Exception as e:
            lines.append(f"Failed to preview {name}: {e}\n")

# Basic metrics: pipeline/dflow/dataset/trigger counts (sheet fallbacks)
lines.append("\n## Derived metrics\n")

def count_sheet(preferred):
    for p in preferred:
        if p in x:
            return len(x[p]), p
    return 0, None

p_count, p_src = count_sheet(["ImpactAnalysis","Pipelines","PipelineAnalysis","Pipeline_Analysis"])
df_count, df_src = count_sheet(["DataFlows","DataFlowLineage"])
set_count, set_src = count_sheet(["Datasets"])
tr_count, tr_src = count_sheet(["Triggers","TriggerDetails"])

lines.append(f"- Pipelines: {p_count} (source: {p_src})\n")
lines.append(f"- DataFlows: {df_count} (source: {df_src})\n")
lines.append(f"- Datasets: {set_count} (source: {set_src})\n")
lines.append(f"- Triggers: {tr_count} (source: {tr_src})\n")

# Write file
out_md.write_text("\n".join(lines), encoding='utf-8')
print(f"Preview written: {out_md}")
print('---')
print('\n'.join(lines[:40]))
print('---')
print('Preview complete')
