"""Generate per-file documentation summaries for Python files in the repo.

Usage:
  python scripts/generate_docs.py

Outputs markdown files into `docs/file_summaries/`.

This script is conservative: it skips directories like .venv, venv, __pycache__, armv10/output, node_modules, and docs.
"""
import ast
import os
from pathlib import Path

ROOT = Path(r"d:/armtemp")
OUT_DIR = ROOT / 'docs' / 'file_summaries'
SKIP_DIRS = {'venv', '.venv', '__pycache__', 'node_modules', 'output', 'docs', '.git'}

OUT_DIR.mkdir(parents=True, exist_ok=True)

py_files = []
for dirpath, dirnames, filenames in os.walk(ROOT):
    # relative path parts
    parts = Path(dirpath).relative_to(ROOT).parts
    if any(p in SKIP_DIRS for p in parts):
        continue
    for fn in filenames:
        if fn.endswith('.py'):
            # skip this generator itself
            full = Path(dirpath) / fn
            if full.resolve() == Path(__file__).resolve():
                continue
            py_files.append(full)

index_lines = ["# Files index\n", "\n", "Generated file summaries. Edit the ones that need richer documentation.\n", "\n"]

for p in sorted(py_files):
    rel = p.relative_to(ROOT)
    safe_name = str(rel).replace(os.sep, '__')
    out_md = OUT_DIR / (safe_name + '.md')

    try:
        src = p.read_text(encoding='utf-8')
    except Exception as e:
        print(f"Skipping {p}: read error {e}")
        continue

    try:
        tree = ast.parse(src)
    except Exception as e:
        print(f"Skipping {p}: parse error {e}")
        continue

    mod_doc = ast.get_docstring(tree) or ''

    # collect top-level defs
    funcs = []
    classes = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef):
            doc = ast.get_docstring(node) or ''
            funcs.append((node.name, doc.split('\n', 1)[0] if doc else ''))
        elif isinstance(node, ast.ClassDef):
            doc = ast.get_docstring(node) or ''
            classes.append((node.name, doc.split('\n', 1)[0] if doc else ''))

    md = []
    md.append(f"# {rel}\n")
    md.append(f"\n> Auto-generated summary. Improve this page with architecture notes, examples and references.\n\n")
    if mod_doc:
        md.append("## Module summary\n\n")
        md.append(mod_doc.strip() + "\n\n")

    if classes:
        md.append("## Classes\n\n")
        for name, docline in classes:
            md.append(f"- **{name}** — {docline}\n")
        md.append('\n')

    if funcs:
        md.append("## Functions\n\n")
        for name, docline in funcs:
            md.append(f"- **{name}()** — {docline}\n")
        md.append('\n')

    md.append("## Notes\n\nAdd usage, examples, cross-references, data shapes, and important edge cases here.\n")

    out_md.write_text(''.join(md), encoding='utf-8')
    index_lines.append(f"- [{rel}]({out_md.relative_to(ROOT.parent)})\n")

# write index
INDEX = ROOT / 'docs' / 'FILES_INDEX.md'
INDEX.write_text(''.join(index_lines), encoding='utf-8')
print(f"Wrote {len(py_files)} summaries to {OUT_DIR} and index to {INDEX}")
