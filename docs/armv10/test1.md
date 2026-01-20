# test1.py

> Auto-generated description — please expand with architecture notes, data models, and examples.

## Contract (heuristic)

- Inputs: likely Azure Data Factory JSON templates, analyzer workbook, or pipeline metadata (see module usage).
- Outputs: reports (Excel/HTML/JSON), dashboard tiles, or transformed datasets.
- Error modes: missing workbook, malformed pipeline definitions, unsupported activities.

## Top-level variables

- activity
- result

## Source preview (first 20 lines)

```from adf_analyzer_v10_complete import ParsedActivity

# Test 1: Create activity with new fields
activity = ParsedActivity(
    pipeline="TestPipeline",
    name="TestActivity",
    activity_type="Copy",
    sequence=1,
    depth=0,
    timeout="00:10:00",
    retry_count=3,
    retry_interval=60,
    secure_input=True,
    user_properties=["env=prod", "team=data"],
    state="Enabled"
)

# Test 2: Convert to dict
result = activity.to_dict()

```

## Notes

Add: examples, how to run, expected input file names (e.g., `output/adf_analysis_latest.xlsx`), related modules, and common failure modes.
