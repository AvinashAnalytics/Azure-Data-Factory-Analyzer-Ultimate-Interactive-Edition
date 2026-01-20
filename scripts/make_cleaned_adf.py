import re
from pathlib import Path

src = Path(r"d:\armtemp\armv10\adf_analyzer_v10_complete.py")
dst = Path(r"d:\armtemp\armv10\adf_analyzer_v10_cleaned_fixed.py")

text = src.read_text(encoding='utf-8')

# Find all occurrences of the class definition line
pattern = re.compile(r'^class\s+UltimateEnterpriseADFAnalyzer\b', flags=re.MULTILINE)
matches = list(pattern.finditer(text))

if len(matches) <= 1:
    print("No duplicate class found or only one occurrence. Creating a straight copy.")
    dst.write_text(text, encoding='utf-8')
else:
    first_start = matches[0].start()
    last_start = matches[-1].start()

    # Keep everything before the first occurrence, and everything from the last occurrence to EOF
    head = text[:first_start]
    tail = text[last_start:]

    # Assemble cleaned content
    cleaned = head + '\n' + tail

    dst.write_text(cleaned, encoding='utf-8')
    print(f"Created cleaned file: {dst}\nRemoved duplicate occurrences: kept occurrence at {last_start}")

print('Done')
