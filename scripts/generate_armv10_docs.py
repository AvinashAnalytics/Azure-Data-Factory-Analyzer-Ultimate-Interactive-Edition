"""Generate detailed documentation pages for main Python files in armv10/.

Creates one markdown file per module in `docs/armv10/` with:
- module docstring (if any)
- short list of classes, functions, and top-level variables
- a short 'contract' section (inputs/outputs/data shapes) derived heuristically
- first 20 lines of source as a preview
- notes and placeholders for manual editing

Usage:
  python scripts/generate_armv10_docs.py

This script is conservative and will skip files inside `bak`, `__pycache__`, and files with names containing 'copy' or 'bak'.
"""
import ast
import os
from pathlib import Path

ROOT = Path(r"d:/armtemp")
PKG_DIR = ROOT / 'armv10'
OUT_DIR = ROOT / 'docs' / 'armv10'
OUT_DIR.mkdir(parents=True, exist_ok=True)

SKIP_PATTERNS = ('__pycache__', 'bak', 'copy')

def safe_read(p: Path):
    try:
        return p.read_text(encoding='utf-8')
    except Exception as e:
        return None

files = []
for p in sorted(PKG_DIR.glob('*.py')):
    name = p.name.lower()
    if any(tok in name for tok in SKIP_PATTERNS):
        continue
    files.append(p)

count = 0
for p in files:
    src = safe_read(p)
    if src is None:
        print(f"Skipping {p} (read error)")
        continue
    try:
        tree = ast.parse(src)
    except Exception as e:
        print(f"Skipping {p} (parse error: {e})")
        continue

    mod_doc = ast.get_docstring(tree) or ''
    classes = []
    funcs = []
    top_vars = []

    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            classes.append((node.name, (ast.get_docstring(node) or '').split('\n',1)[0]))
        elif isinstance(node, ast.FunctionDef):
            funcs.append((node.name, (ast.get_docstring(node) or '').split('\n',1)[0]))
        elif isinstance(node, ast.Assign):
            # collect simple top-level names
            for target in node.targets:
                if isinstance(target, ast.Name):
                    top_vars.append(target.id)

    out_lines = []
    out_lines.append(f"# {p.name}\n\n")
    out_lines.append("> Auto-generated description — please expand with architecture notes, data models, and examples.\n\n")

    if mod_doc:
        out_lines.append("## Overview\n\n")
        out_lines.append(mod_doc.strip() + "\n\n")

    # tiny contract heuristics
    out_lines.append("## Contract (heuristic)\n\n")
    out_lines.append("- Inputs: likely Azure Data Factory JSON templates, analyzer workbook, or pipeline metadata (see module usage).\n")
    out_lines.append("- Outputs: reports (Excel/HTML/JSON), dashboard tiles, or transformed datasets.\n")
    out_lines.append("- Error modes: missing workbook, malformed pipeline definitions, unsupported activities.\n\n")

    if classes:
        out_lines.append("## Classes\n\n")
        for n,d in classes:
            out_lines.append(f"- **{n}** — {d}\n")
        out_lines.append('\n')

    if funcs:
        out_lines.append("## Functions\n\n")
        for n,d in funcs:
            out_lines.append(f"- **{n}()** — {d}\n")
        out_lines.append('\n')

    if top_vars:
        out_lines.append("## Top-level variables\n\n")
        for v in top_vars[:20]:
            out_lines.append(f"- {v}\n")
        out_lines.append('\n')

    out_lines.append("## Source preview (first 20 lines)\n\n```")
    preview = '\n'.join(src.splitlines()[:20])
    out_lines.append(preview)
    out_lines.append("\n```\n\n")

    out_lines.append("## Notes\n\nAdd: examples, how to run, expected input file names (e.g., `output/adf_analysis_latest.xlsx`), related modules, and common failure modes.\n")

    target = OUT_DIR / (p.stem + '.md')
    target.write_text(''.join(out_lines), encoding='utf-8')
    count += 1

print(f"Wrote {count} armv10 docs to {OUT_DIR}")
