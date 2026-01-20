# test8.py

> Auto-generated description — please expand with architecture notes, data models, and examples.

## Contract (heuristic)

- Inputs: likely Azure Data Factory JSON templates, analyzer workbook, or pipeline metadata (see module usage).
- Outputs: reports (Excel/HTML/JSON), dashboard tiles, or transformed datasets.
- Error modes: missing workbook, malformed pipeline definitions, unsupported activities.

## Top-level variables

- test_template
- analyzer

## Source preview (first 20 lines)

```import json
from adf_analyzer_v10_complete import UltimateEnterpriseADFAnalyzer

# Create ARM template with enhanced triggers
test_template = {
    "$schema": "http://schema.management.azure.com/schemas/2015-01-01/deploymentTemplate.json#",
    "contentVersion": "1.0.0.0",
    "resources": [
        # Base Tumbling Window Trigger
        {
            "type": "Microsoft.DataFactory/factories/triggers",
            "name": "TestFactory/HourlyTrigger",
            "properties": {
                "type": "TumblingWindowTrigger",
                "runtimeState": "Started",
                "typeProperties": {
                    "frequency": "Hour",
                    "interval": 1,
                    "startTime": "2024-01-01T00:00:00Z"
                },
```

## Notes

Add: examples, how to run, expected input file names (e.g., `output/adf_analysis_latest.xlsx`), related modules, and common failure modes.
