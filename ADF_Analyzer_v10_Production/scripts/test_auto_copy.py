from pathlib import Path
import pandas as pd
import sys

# Make repo importable
repo_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(repo_root / 'core'))

# Import the analyzer class
from adf_analyzer_v10_complete import UltimateEnterpriseADFAnalyzer

# Create a tiny Excel file
out_dir = repo_root / 'output'
out_dir.mkdir(parents=True, exist_ok=True)
excel_file = out_dir / 'test_autocopy.xlsx'

pd.DataFrame({'A':[1]}).to_excel(excel_file, index=False)
print(f"Created test workbook: {excel_file}")

# Instantiate analyzer (logger will capture messages)
analyzer = UltimateEnterpriseADFAnalyzer(json_path='test_placeholder.json', enable_discovery=False)

# Call the protected auto-copy method
try:
    analyzer._auto_copy_to_streamlit(excel_file)
    print("_auto_copy_to_streamlit() invoked")
except Exception as e:
    print(f"_auto_copy_to_streamlit() raised: {e}")

# Try to discover where the file was copied (check a few likely locations)
candidates = [
    repo_root / 'streamlit_app' / 'data' / excel_file.name,
    repo_root / 'config' / 'streamlit_app' / excel_file.name,
    Path.cwd() / 'streamlit_app' / 'data' / excel_file.name,
]

for c in candidates:
    print(f"Checking: {c} -> exists={c.exists()}")

# If present, print absolute path of first found copy
for root in [repo_root, Path.cwd()]:
    candidate = root / 'streamlit_app' / 'data' / excel_file.name
    if candidate.exists():
        print(f"Found copy at: {candidate}")
        break
else:
    print("No Streamlit copy found by quick checks. Check analyzer logs for details.")
