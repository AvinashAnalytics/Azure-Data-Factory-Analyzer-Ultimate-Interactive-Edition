"""
Test script to verify all patches work correctly
"""

def test_patches():
    """Test that all patches apply correctly"""
    
    print("\n" + "="*80)
    print("🧪 TESTING COMPREHENSIVE PATCHES")
    print("="*80 + "\n")
    
    # Import and patch
    from adf_analyzer_v10_patch import apply_all_patches
    from adf_analyzer_v10_complete import UltimateEnterpriseADFAnalyzer
    
    # Apply patches
    success = apply_all_patches(UltimateEnterpriseADFAnalyzer)
    
    if not success:
        print("❌ Patch application failed")
        return False
    
    # Verify new methods exist
    print("\n🔍 Verifying patched methods...\n")
    
    tests = [
        ('_parse_databricks_activity', 'Databricks activity parser'),
        ('_parse_azure_function_activity', 'Azure Function parser'),
        ('_parse_hdinsight_mapreduce_activity', 'HDI MapReduce parser'),
        ('_parse_salesforce_activity', 'Salesforce parser'),
    ]
    
    all_passed = True
    
    for method_name, description in tests:
        if hasattr(UltimateEnterpriseADFAnalyzer, method_name):
            print(f"  ✅ {description}: FOUND")
        else:
            print(f"  ❌ {description}: MISSING")
            all_passed = False
    
    # Test instantiation
    print("\n🔍 Testing analyzer instantiation...\n")
    
    try:
        # Create a dummy JSON file for testing
        import json
        from pathlib import Path
        
        test_template = {
            "$schema": "http://schema.management.azure.com/schemas/2015-01-01/deploymentTemplate.json#",
            "contentVersion": "1.0.0.0",
            "parameters": {},
            "variables": {},
            "resources": []
        }
        
        test_file = Path('test_template.json')
        with open(test_file, 'w') as f:
            json.dump(test_template, f)
        
        # Try to create analyzer
        analyzer = UltimateEnterpriseADFAnalyzer(str(test_file))
        print("  ✅ Analyzer instantiation: SUCCESS")
        
        # Cleanup
        test_file.unlink()
        
    except Exception as e:
        print(f"  ❌ Analyzer instantiation: FAILED - {e}")
        all_passed = False
    
    print("\n" + "="*80)
    if all_passed:
        print("✅ ALL TESTS PASSED")
    else:
        print("❌ SOME TESTS FAILED")
    print("="*80 + "\n")
    
    return all_passed


if __name__ == "__main__":
    test_patches()