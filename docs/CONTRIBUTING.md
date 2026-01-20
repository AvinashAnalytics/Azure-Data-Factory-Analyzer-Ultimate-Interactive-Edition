# Contributing to Documentation

Please follow these simple rules when adding or editing documentation in `docs/`:

- Keep generated files (in `docs/file_summaries/`) limited to machine-created content; add manual narrative below a `## Notes` section.
- For design/logic explanations, create a human-written md file in `docs/` (for example `docs/ARCHITECTURE.md`) and link to related file summaries.
- When editing the generator (`scripts/generate_docs.py`), ensure it remains idempotent and includes a safe backup mechanism for edited README files.
- If you update the docs, add a short entry to `armv10/CHANGES_SUMMARY.md` describing the change.

Style recommendations

- Use clear headings, short paragraphs, and examples.
- Prefer code blocks for any command or snippet.
- Document assumptions and edge cases where relevant.
