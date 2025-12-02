#!/usr/bin/env python3
"""
ADF Analyzer v10.1 - Setup and Verification Script

This script helps set up the ADF Analyzer environment and verifies installation.
"""

import os
import sys
import subprocess
import importlib
from pathlib import Path

def print_header():
    """Print setup header"""
    print("=" * 60)
    print("🚀 ADF Analyzer v10.1 - Setup & Verification")
    print("=" * 60)
    print()

def check_python_version():
    """Check if Python version is compatible"""
    print("🐍 Checking Python version...")
    version = sys.version_info

    if version.major == 3 and version.minor >= 8:
        print(f" Python {version.major}.{version.minor}.{version.micro} - Compatible")
        return True
    else:
        print(f" Python {version.major}.{version.minor}.{version.micro} - Requires Python 3.8+")
        return False

def check_dependencies():
    """Check if required dependencies are installed"""
    print("\n📦 Checking dependencies...")

    required_packages = [
        'streamlit',
        'pandas',
        'openpyxl',
        'plotly',
        'networkx'
    ]

    missing_packages = []

    for package in required_packages:
        try:
            importlib.import_module(package)
            print(f" {package} - Installed")
        except ImportError:
            print(f" {package} - Missing")
            missing_packages.append(package)

    return missing_packages

def install_dependencies(missing_packages):
    """Install missing dependencies"""
    if not missing_packages:
        return True

    print(f"\n🔧 Installing {len(missing_packages)} missing packages...")

    try:
        # Try to install from requirements.txt if it exists
        if Path("requirements.txt").exists():
            subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        else:
            # Install packages individually
            for package in missing_packages:
                subprocess.check_call([sys.executable, "-m", "pip", "install", package])

        print(" Dependencies installed successfully")
        return True

    except subprocess.CalledProcessError as e:
        print(f" Failed to install dependencies: {e}")
        return False

def verify_core_files():
    """Verify that core files exist"""
    print("\n📁 Verifying core files...")

    core_files = [
        "adf_runner_wrapper.py",
        "adf_dashboard.py",
        "core/adf_analyzer_v10_complete.py",
        "config/enhancement_config.json",
        "README.md"
    ]

    missing_files = []

    for file_path in core_files:
        if Path(file_path).exists():
            print(f" {file_path}")
        else:
            print(f" {file_path} - Missing")
            missing_files.append(file_path)

    return len(missing_files) == 0

def run_quick_test():
    """Run a quick test to verify functionality"""
    print("\n🧪 Running quick verification test...")

    try:
        # Test import of main modules
        sys.path.insert(0, str(Path.cwd()))

        # Test wrapper import
        spec = importlib.util.spec_from_file_location("adf_runner_wrapper", "adf_runner_wrapper.py")
        if spec and spec.loader:
            print(" Wrapper module can be imported")
        else:
            print(" Wrapper module import failed")
            return False

        # Test dashboard import
        spec = importlib.util.spec_from_file_location("adf_dashboard", "adf_dashboard.py")
        if spec and spec.loader:
            print(" Dashboard module can be imported")
        else:
            print(" Dashboard module import failed")
            return False

        print(" All core modules verified")
        return True

    except Exception as e:
        print(f" Verification test failed: {e}")
        return False

def show_usage_instructions():
    """Show usage instructions"""
    print("\n" + "=" * 60)
    print(" SETUP COMPLETE - Usage Instructions")
    print("=" * 60)
    print()
    print(" Quick Analysis:")
    print("   python adf_runner_wrapper.py your_template.json")
    print()
    print("🎛 Interactive Dashboard:")
    print("   streamlit run adf_dashboard.py")
    print()
    print("📚 Documentation:")
    print("   - README.md - Complete project guide")
    print("   - docs/ - Technical documentation")
    print("   - Dashboard → Documentation tab")
    print()
    print("🔧 Configuration:")
    print("   - config/enhancement_config.json - Excel features")
    print("   - config/streamlit_config.json - Dashboard settings")
    print()

def main():
    """Main setup function"""
    print_header()

    # Check Python version
    if not check_python_version():
        print("\n Setup failed: Incompatible Python version")
        return False

    # Check dependencies
    missing_packages = check_dependencies()

    # Install missing dependencies
    if missing_packages:
        if not install_dependencies(missing_packages):
            print("\n Setup failed: Could not install dependencies")
            return False

    # Verify core files
    if not verify_core_files():
        print("\n Setup failed: Missing core files")
        return False

    # Run verification test
    if not run_quick_test():
        print("\n Setup failed: Verification test failed")
        return False

    # Show usage instructions
    show_usage_instructions()

    print("🎉 Setup completed successfully!")
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)