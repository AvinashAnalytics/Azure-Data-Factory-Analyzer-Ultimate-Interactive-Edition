# Azure Data Factory ARM Template Parser

A comprehensive Python tool to parse, analyze, and export Azure Data Factory ARM templates into structured formats.

## Features

✅ **Complete Resource Parsing**
- Integration Runtimes (Self-Hosted, Managed, Azure-SSIS)
- Linked Services (44+ types)
- Datasets (Parquet, CSV, JSON, SQL, etc.)
- Pipelines with all activity types
- Dataflows (Mapping & Wrangling)
- Triggers (Schedule, Tumbling Window, Blob Events)
- Managed Virtual Networks & Private Endpoints
- Factory-level configuration (Global Parameters, Git Integration)

✅ **Advanced Analysis**
- Dependency graph generation
- Circular dependency detection
- Security audit with risk scoring
- Data lineage tracking
- SQL query extraction
- Table and column discovery

✅ **Rich Excel Export**
- 15+ formatted worksheets
- Auto-sized columns
- Color-coded headers
- Frozen panes
- Interactive filtering

✅ **Multiple Export Formats**
- Excel (.xlsx) - Primary format
- JSON - Machine-readable
- Markdown - Documentation

## Installation

### Prerequisites
- Python 3.8 or higher
- pip

### Setup

```bash
# Clone repository
git clone https://github.com/your-org/adf-arm-parser.git
cd adf-arm-parser

# Create virtual environment
python -m venv venv

# Activate virtual environment
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt