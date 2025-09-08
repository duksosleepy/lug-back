#!/usr/bin/env python3
"""Debug script to check what accounts are returned for '3840'"""

import sys
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.accounting.fast_search import search_accounts

def debug_3840_accounts():
    """Debug what accounts are found for '3840'"""
    print("Searching for accounts with '3840' in name:")
    results = search_accounts("3840", field_name="name", limit=10)
    for i, result in enumerate(results):
        print(f"{i+1}. Code: {result['code']} | Name: {result['name']}")
    
    print("\nSearching for accounts with '3840' in code:")
    results = search_accounts("3840", field_name="code", limit=10)
    for i, result in enumerate(results):
        print(f"{i+1}. Code: {result['code']} | Name: {result['name']}")

if __name__ == "__main__":
    debug_3840_accounts()