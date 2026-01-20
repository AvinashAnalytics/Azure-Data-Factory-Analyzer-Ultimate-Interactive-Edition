# Project Documentation

This folder contains generated and curated documentation for the Azure Data Factory Analyzer project.

What you'll find here:

- `FILES_INDEX.md` — an index of generated file summaries (auto-generated).
- `file_summaries/` — per-file markdown summaries created by `scripts/generate_docs.py`.
- `CONTRIBUTING.md` — guidance for adding/updating documentation (skeleton created).

Project-specific docs

- `armv10/` — detailed auto-generated pages and a small manifest for the `armv10` distribution. See `docs/armv10/REQUIRED_FILES.md` for a prioritized list of the files to maintain.

How the generator works

- Run `scripts/generate_docs.py` (from repository root) to scan the repo for Python files and create/update the markdown summaries.
- The generator attempts to extract module docstrings and top-level functions/classes with their docstrings.

Notes

- The generator is conservative: it skips virtual environments, `__pycache__`, and common data/output folders.
- The generated summaries are a starting point — please manually enhance the important module pages with usage examples, design decisions, and references to related files.

Next steps

- Review `docs/file_summaries/` and add human-written details for critical modules (e.g., `armv10/*`, `adf_analyzer.py`, `adf_parser.py`).
- When satisfied, commit the `docs/` folder and the generator script and consider adding a CI job to keep docs up-to-date.
