#!/usr/bin/env python3
"""
Debug script to test CRM data flow
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.crm.flow import process_data

if __name__ == "__main__":
    print("Testing CRM data processing...")
    try:
        result = process_data()
        print(f"Result: {result}")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()