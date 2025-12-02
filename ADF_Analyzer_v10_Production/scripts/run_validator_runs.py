#!/usr/bin/env python3
"""Run the validator repeatedly and capture outputs.

Writes per-run CSV and JSON summary files into armv10/output:
 - dataset_breakdown_run_<i>.csv
 - validator_summary_run_<i>.json
Also writes runs_summary.json with per-run metadata.
"""
import subprocess
import sys
import time
import json
from pathlib import Path

validator = Path(__file__).resolve().parent / "validate_tiles.py"
workbook = Path(__file__).resolve().parent.parent / "streamlit_app" / "data" / "adf_analysis_latest.xlsx"
out_dir = Path(__file__).resolve().parent.parent / "output"
out_dir.mkdir(parents=True, exist_ok=True)

runs = []
for i in range(1, 11):
    csv_out = out_dir / f"dataset_breakdown_run_{i}.csv"
    json_out = out_dir / f"validator_summary_run_{i}.json"
    start = time.time()
    proc = subprocess.run([sys.executable, str(validator), str(workbook), "--csv", str(csv_out)], capture_output=True, text=True)
    duration = time.time() - start

    stdout = proc.stdout or ""
    # validator prints a CSV write message then JSON; try to extract the JSON blob
    idx = stdout.find('{')
    json_text = stdout[idx:] if idx >= 0 else stdout
    try:
        # validate JSON
        parsed = json.loads(json_text)
    except Exception:
        parsed = {"raw_stdout": stdout, "parse_error": True}

    with open(json_out, 'w', encoding='utf-8') as f:
        if isinstance(parsed, dict):
            json.dump(parsed, f, indent=2)
        else:
            f.write(json_text)

    runs.append({
        "run": i,
        "returncode": proc.returncode,
        "duration_seconds": round(duration, 3),
        "csv_out": str(csv_out),
        "json_out": str(json_out),
    })
    print(f"Run {i}: returncode={proc.returncode}, duration={duration:.2f}s, csv={csv_out.name}, json={json_out.name}")

with open(out_dir / "runs_summary.json", 'w', encoding='utf-8') as f:
    json.dump(runs, f, indent=2)

print("All runs finished. Summary written to:", out_dir / "runs_summary.json")
