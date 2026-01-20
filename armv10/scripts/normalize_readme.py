# normalize_readme.py
# Usage: python scripts/normalize_readme.py
# - Removes decorative "🎉 END OF PART" banner text while keeping navigation links
# - Normalizes "Last updated" lines to the canonical date
# - Preserves content inside fenced code blocks (```...```) and mermaid fences

import re
from pathlib import Path

README = Path(r"d:/armtemp/armv10/readme_v10.md")
BACKUP = README.with_suffix('.md.bak')
CHANGES = Path(r"d:/armtemp/armv10/CHANGES_SUMMARY.md")
CANON_DATE = "2025-11-07"

text = README.read_text(encoding='utf-8')
# Make a backup
BACKUP.write_text(text, encoding='utf-8')

lines = text.splitlines(keepends=True)
out_lines = []

in_fence = False
fence_delim = None

# regex to remove bolded END OF PART sections up to the bullet (•) if present
end_of_part_re = re.compile(r"\*\*.*?END OF PART.*?\*\*\s*•\s*", flags=re.IGNORECASE)
# also match non-bold raw variants
end_of_part_plain_re = re.compile(r"🎉\s*END OF PART[^•]*\s*•\s*", flags=re.IGNORECASE)

# regex to find Last updated lines (case-insensitive)
last_updated_re = re.compile(r"(?i)^(?P<prefix>\s*\*?\s*Last\s*(?:updated|Updated)\s*:\s*)(?P<date>[^\n\r*<]{1,50})(?P<suffix>\*?\s*)$")

for line in lines:
    # detect fence start/end
    stripped = line.lstrip()
    if stripped.startswith('```'):
        # toggle fence state
        if not in_fence:
            in_fence = True
            fence_delim = stripped[:stripped.find('\n')] if '\n' in stripped else stripped.strip()
        else:
            # exiting fence
            in_fence = False
            fence_delim = None
        out_lines.append(line)
        continue

    if in_fence:
        out_lines.append(line)
        continue

    # Outside fenced blocks: apply transformations
    new_line = line

    # Remove bolded END OF PART patterns that precede a bullet and keep the remainder
    if 'END OF PART' in line or '🎉' in line:
        # first remove bolded variant up to the bullet
        new_line = end_of_part_re.sub('', new_line)
        new_line = end_of_part_plain_re.sub('', new_line)
        # If line becomes empty or only bullets/whitespace/dashes, remove leading separators
        if new_line.strip() == '' or new_line.strip() in ('-', '---'):
            # keep a single horizontal rule if it was there before
            # If original line had a navigation link after the bullet split across next token, leave as-is
            new_line = '\n' if new_line.strip() == '' else new_line

    # Normalize Last updated lines (short lines only)
    m = last_updated_re.match(new_line)
    if m:
        prefix = m.group('prefix')
        suffix = m.group('suffix')
        new_line = f"{prefix}{CANON_DATE}{suffix}\n"

    out_lines.append(new_line)

new_text = ''.join(out_lines)

# Quick safety check: ensure we didn't remove any triple-backticks count
orig_fence_count = text.count('```')
new_fence_count = new_text.count('```')
if orig_fence_count != new_fence_count:
    print('FENCE COUNT MISMATCH: aborting write (safety).')
else:
    README.write_text(new_text, encoding='utf-8')
    print(f'Updated {README} (backup at {BACKUP})')

    # Append a one-line changelog entry
    note = f"{__file__}: Normalized 'Last updated' to {CANON_DATE} and removed decorative 'END OF PART' banners on {CANON_DATE}.\n"
    if CHANGES.exists():
        CHANGES.write_text(CHANGES.read_text(encoding='utf-8') + '\n' + note, encoding='utf-8')
    else:
        CHANGES.write_text(note, encoding='utf-8')
    print(f'Appended changelog note to {CHANGES}')
