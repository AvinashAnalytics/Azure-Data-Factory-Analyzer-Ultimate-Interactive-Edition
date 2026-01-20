# test7.py

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

# Create ARM template with MERGE statement in Script activity
test_template = {
    "$schema": "http://schema.management.azure.com/schemas/2015-01-01/deploymentTemplate.json#",
    "contentVersion": "1.0.0.0",
    "resources": [
        {
            "type": "Microsoft.DataFactory/factories/pipelines",
            "name": "TestFactory/MergePipeline",
            "properties": {
                "activities": [
                    {
                        "name": "MergeCustomers",
                        "type": "Script",
                        "typeProperties": {
                            "scripts": [
                                {
                                    "type": "Query",
```

## Notes

Add: examples, how to run, expected input file names (e.g., `output/adf_analysis_latest.xlsx`), related modules, and common failure modes.
