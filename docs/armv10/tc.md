# tc.py

> Auto-generated description — please expand with architecture notes, data models, and examples.

## Contract (heuristic)

- Inputs: likely Azure Data Factory JSON templates, analyzer workbook, or pipeline metadata (see module usage).
- Outputs: reports (Excel/HTML/JSON), dashboard tiles, or transformed datasets.
- Error modes: missing workbook, malformed pipeline definitions, unsupported activities.

## Top-level variables

- analyzer

## Source preview (first 20 lines)

```import inspect
from adf_analyzer_v10_complete import UltimateEnterpriseADFAnalyzer

print("Verifying code structure...\n")

# Check if method exists
if hasattr(UltimateEnterpriseADFAnalyzer, '_extract_parameters_from_activity'):
    print("✅ _extract_parameters_from_activity method EXISTS")
    
    # Check if it's callable
    method = getattr(UltimateEnterpriseADFAnalyzer, '_extract_parameters_from_activity')
    if callable(method):
        print("✅ Method is callable")
        
        # Get signature
        sig = inspect.signature(method)
        print(f"   Signature: {sig}")
    else:
        print("❌ Method exists but is not callable!")
else:
```

## Notes

Add: examples, how to run, expected input file names (e.g., `output/adf_analysis_latest.xlsx`), related modules, and common failure modes.
