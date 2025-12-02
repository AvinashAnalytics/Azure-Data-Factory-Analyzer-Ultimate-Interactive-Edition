#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Safe wrapper for ADF analyzer that handles Unicode encoding properly
"""

import sys
import os
import subprocess
from pathlib import Path

def main():
    """Run the patched runner with proper encoding"""

    # Ensure UTF-8 encoding
    if os.name == 'nt':  # Windows
        os.environ['PYTHONIOENCODING'] = 'utf-8'
        os.environ['PYTHONLEGACYWINDOWSFSENCODING'] = '1'

    # Get the script directory
    script_dir = Path(__file__).parent
    core_dir = script_dir / 'core'

    # Find the patched runner (look in core folder first)
    runner_candidates = [
        core_dir / 'adf_analyzer_v10_patched_runner.py',
        core_dir / 'adf_analyzer_v10_patch.py',
        core_dir / 'adf_analyzer_v10_complete.py',
        script_dir / 'adf_analyzer_v10_patched_runner.py',  # fallback to current dir
        script_dir / 'adf_analyzer_v10_complete.py'
    ]

    runner_path = None
    for candidate in runner_candidates:
        if candidate.exists():
            runner_path = candidate
            print(f"Using runner: {candidate}")
            break

    if not runner_path:
        print(" No suitable runner found")
        sys.exit(1)

    if len(sys.argv) < 2:
        print(" Usage: python adf_runner_wrapper.py <template.json>")
        sys.exit(1)

    json_file = sys.argv[1]

    try:
        # Execute the runner with proper encoding
        result = subprocess.run(
            [sys.executable, str(runner_path), json_file],
            cwd=script_dir,
            encoding='utf-8',
            errors='replace',
            text=True,
            capture_output=False  # Let output stream directly
        )

        sys.exit(result.returncode)

    except Exception as e:
        print(f" Error running analyzer: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()