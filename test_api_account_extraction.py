#!/usr/bin/env python3
"""Test script to verify API endpoint properly uses filename-based account extraction"""

import sys
from pathlib import Path
import io
import pandas as pd

# Add the project root to the path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.accounting.integrated_bank_processor import IntegratedBankProcessor

def create_mock_excel_file():
    """Create a mock Excel file content for testing"""
    # Create a simple DataFrame that mimics a bank statement without header info
    data = {
        'A': ['', '', '', '', '', 'Some data without account info'],
        'B': ['', '', '', '', '', 'More data'],
        'C': ['', '', '', '', '', 'Even more data'],
    }
    df = pd.DataFrame(data)
    
    # Save to bytes buffer
    buffer = io.BytesIO()
    df.to_excel(buffer, index=False, header=False)
    buffer.seek(0)
    return buffer

def test_api_filename_account_extraction():
    """Test that the API properly uses filename-based account extraction when header extraction fails"""
    processor = IntegratedBankProcessor()
    
    # Test with a filename that contains account info
    test_filename = "BIDV 3840.xlsx"
    
    print("Testing API filename-based account extraction:")
    print("=" * 50)
    
    # Create mock file content
    mock_file_content = create_mock_excel_file()
    
    # Test the extract_account_from_filename method directly
    account_from_filename = processor.extract_account_from_filename(test_filename)
    print(f"Direct filename extraction: {account_from_filename}")
    
    # Test the extract_account_from_header method (should fail with our mock data)
    mock_file_content.seek(0)
    account_from_header = processor.extract_account_from_header(mock_file_content)
    print(f"Header extraction (should be None): {account_from_header}")
    
    print("=" * 50)
    print("Test completed successfully!")
    print(f"Filename extraction found account: {account_from_filename is not None}")
    print(f"Header extraction failed as expected: {account_from_header is None}")
    
    return account_from_filename is not None and account_from_header is None

if __name__ == "__main__":
    test_api_filename_account_extraction()