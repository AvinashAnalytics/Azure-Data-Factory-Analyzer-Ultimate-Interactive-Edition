#!/usr/bin/env python3
"""
Generate an expert HTML report from the analyzer workbook with Plotly charts.
Writes armv10/output/preview_report.html
"""
import sys
from pathlib import Path
import pandas as pd
import json
import plotly.graph_objects as go
import plotly.express as px

base = Path(__file__).resolve().parent
output_dir = base / "output"
output_dir.mkdir(parents=True, exist_ok=True)

wb_path = base / "output" / "adf_analysis_latest.xlsx"
if len(sys.argv) > 1 and sys.argv[1].strip():
    wb_path = Path(sys.argv[1])

if not wb_path.exists():
    print(f"Workbook not found: {wb_path}")
    sys.exit(2)

xl = pd.read_excel(wb_path, sheet_name=None)

# helper to safe get
def df(name):
    return xl.get(name, pd.DataFrame())

summary = df('Summary')
pipelines = df('Pipelines')
dataflows = df('DataFlows')
datasets = df('Datasets')
lineage = df('DataLineage')
dflow_lineage = df('DataFlowLineage')

# build top sources/targets
def value_counts(dfs, col):
    s = []
    for d in dfs:
        if col in d.columns:
            s += d[col].dropna().astype(str).str.strip().tolist()
    return pd.Series(s).value_counts()

src_counts = value_counts([lineage, dflow_lineage], 'Source')
tgt_counts = value_counts([lineage, dflow_lineage], 'Sink')

html_parts = []

html_parts.append('<html><head><meta charset="utf-8"><title>ADF Analyzer Preview</title></head><body>')
html_parts.append('<h1>ADF Analyzer - Expert Preview</h1>')

# tiles
tiles = {
    'Pipelines': len(pipelines),
    'DataFlows': len(dataflows),
    'Datasets': len(datasets),
}
html_parts.append('<div style="display:flex;gap:24px;margin-bottom:20px">')
for k,v in tiles.items():
    html_parts.append(f'<div style="padding:12px;border-radius:8px;background:#f4f6fb;min-width:160px"><h3>{k}</h3><div style="font-size:28px">{v}</div></div>')
html_parts.append('</div>')

# top sources chart
if not src_counts.empty:
    top_src = src_counts.head(10)
    fig_src = go.Figure(go.Bar(x=top_src.values.tolist(), y=top_src.index.tolist(), orientation='h', marker=dict(color='#667eea')))
    fig_src.update_layout(margin=dict(l=120, r=10, t=20, b=20), height=360)
    html_parts.append(fig_src.to_html(full_html=False, include_plotlyjs='cdn'))

if not tgt_counts.empty:
    top_tgt = tgt_counts.head(10)
    fig_tgt = go.Figure(go.Bar(x=top_tgt.values.tolist(), y=top_tgt.index.tolist(), orientation='h', marker=dict(color='#4facfe')))
    fig_tgt.update_layout(margin=dict(l=120, r=10, t=20, b=20), height=360)
    html_parts.append(fig_tgt.to_html(full_html=False, include_plotlyjs=False))

# Sankey
def build_sankey(df_list):
    rows = []
    for d in df_list:
        if 'Source' in d.columns and 'Sink' in d.columns:
            rows += list(d[['Source','Sink']].dropna().itertuples(index=False, name=None))
    if not rows:
        return None
    link_counts = pd.DataFrame(rows, columns=['Source','Sink']).groupby(['Source','Sink']).size().reset_index(name='count').sort_values('count', ascending=False)
    src_top = link_counts.groupby('Source')['count'].sum().nlargest(10).index.tolist()
    tgt_top = link_counts.groupby('Sink')['count'].sum().nlargest(10).index.tolist()
    nodes = list(dict.fromkeys(src_top + tgt_top))
    if not nodes:
        return None
    node_idx = {n:i for i,n in enumerate(nodes)}
    filtered = link_counts[link_counts['Source'].isin(nodes) & link_counts['Sink'].isin(nodes)].head(50)
    source_idx = [node_idx[s] for s in filtered['Source']]
    target_idx = [node_idx[t] for t in filtered['Sink']]
    values = filtered['count'].tolist()
    sankey_fig = go.Figure(data=[go.Sankey(node=dict(label=nodes, pad=15, thickness=15), link=dict(source=source_idx, target=target_idx, value=values))])
    sankey_fig.update_layout(height=600, margin=dict(l=10,r=10,t=20,b=20))
    return sankey_fig

sankey = build_sankey([lineage, dflow_lineage])
if sankey is not None:
    html_parts.append('<h2>Top Source → Target Flows</h2>')
    html_parts.append(sankey.to_html(full_html=False, include_plotlyjs=False))

html_parts.append('</body></html>')

out = output_dir / 'preview_report.html'
out.write_text('\n'.join(html_parts), encoding='utf-8')
print(f'Wrote HTML preview: {out}')
