#!/usr/bin/env python3
"""
Test script for cancel reasons functionality in mysapogo_com.py

This script helps verify that the cancel reasons mapping works correctly
and demonstrates the new functionality.
"""

import asyncio
import sys
import os

# Add the src directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

async def test_cancel_reasons():
    """Test the cancel reasons functionality."""
    from sapo_sync.mysapogo_com import (
        initialize_cancel_reasons,
        get_cancel_reasons_lookup,
        get_fallback_cancel_reasons
    )
    
    print("=== Testing Cancel Reasons Functionality ===\n")
    
    # Test 1: Test fallback cancel reasons
    print("1. Testing fallback cancel reasons:")
    fallback_reasons = get_fallback_cancel_reasons()
    print(f"   - Fallback reasons count: {len(fallback_reasons)}")
    print(f"   - Sample entries:")
    for i, (reason_id, reason_name) in enumerate(list(fallback_reasons.items())[:3]):
        print(f"     {reason_id}: {reason_name}")
    print()
    
    # Test 2: Test API initialization
    print("2. Testing API initialization:")
    success = await initialize_cancel_reasons()
    print(f"   - Initialization successful: {success}")
    
    # Test 3: Test lookup functionality
    print("3. Testing lookup functionality:")
    current_lookup = get_cancel_reasons_lookup()
    print(f"   - Current lookup count: {len(current_lookup)}")
    print(f"   - Sample entries:")
    for i, (reason_id, reason_name) in enumerate(list(current_lookup.items())[:3]):
        print(f"     {reason_id}: {reason_name}")
    print()
    
    # Test 4: Test specific lookups
    print("4. Testing specific lookups:")
    test_ids = [13870901, 13877599, 999999]  # Valid IDs and one invalid
    for test_id in test_ids:
        result = current_lookup.get(test_id, "")
        print(f"   - ID {test_id}: '{result}'")
    print()
    
    # Test 5: Demonstrate the fix
    print("5. Demonstrating the fix:")
    print("   Before fix: lookup.get(unknown_id, unknown_id) would return the ID")
    print("   After fix:  lookup.get(unknown_id, '') returns empty string")
    
    unknown_id = 999999
    old_way = current_lookup.get(unknown_id, unknown_id)  # Old behavior
    new_way = current_lookup.get(unknown_id, "")          # New behavior
    
    print(f"   - Unknown ID {unknown_id}:")
    print(f"     Old way result: '{old_way}'")
    print(f"     New way result: '{new_way}'")
    
    print("\n=== Test completed ===")


if __name__ == "__main__":
    asyncio.run(test_cancel_reasons())
