# test9.py

> Auto-generated description — please expand with architecture notes, data models, and examples.

## Contract (heuristic)

- Inputs: likely Azure Data Factory JSON templates, analyzer workbook, or pipeline metadata (see module usage).
- Outputs: reports (Excel/HTML/JSON), dashboard tiles, or transformed datasets.
- Error modes: missing workbook, malformed pipeline definitions, unsupported activities.

## Top-level variables

- test_template
- analyzer

## Source preview (first 20 lines)

```# Force fresh import
import sys
import importlib

# Remove any cached imports
if 'adf_analyzer_v10_complete' in sys.modules:
    del sys.modules['adf_analyzer_v10_complete']

# Now import fresh
from adf_analyzer_v10_complete import UltimateEnterpriseADFAnalyzer
import json

print("Testing Gap 9: Global Parameter Usage Tracking (FRESH)\n" + "="*70)

# Create test template
test_template = {
    "$schema": "http://schema.management.azure.com/schemas/2015-01-01/deploymentTemplate.json#",
    "contentVersion": "1.0.0.0",
    "parameters": {
        "factoryName": {"type": "string", "defaultValue": "TestFactory"},
```

## Notes

Add: examples, how to run, expected input file names (e.g., `output/adf_analysis_latest.xlsx`), related modules, and common failure modes.
