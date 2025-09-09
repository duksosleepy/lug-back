#!/usr/bin/env python3
"""
Test script to verify the BC statement KL counterparty fix.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '.'))

from src.accounting.integrated_bank_processor import IntegratedBankProcessor
from src.accounting.bank_statement_reader import BankStatementReader
from unittest.mock import Mock
import pandas as pd
from datetime import datetime


def test_bc_statement_with_kl_counterparty():
    """Test BC statement processing with KL counterparty code."""
    print("Testing BC statement with KL counterparty...")
    
    # Create a mock transaction with KL counterparty
    processor = IntegratedBankProcessor()
    
    # Mock the database connection
    processor.connect = Mock(return_value=True)
    processor.close = Mock()
    
    # Create a test transaction that should have KL counterparty
    class MockTransaction:
        def __init__(self):
            self.reference = "TEST-BC-KL"
            self.datetime = datetime(2025, 6, 15, 10, 30, 0)
            self.debit_amount = 0
            self.credit_amount = 5000000  # Credit transaction (BC)
            self.balance = 15000000
            self.description = "Thu tiền bán hàng KL-GOBARIA1"
    
    transaction = MockTransaction()
    
    # Mock the rule to be BC document type
    class MockRule:
        def __init__(self):
            self.document_type = "BC"
            self.transaction_type = "SALE"
            self.counterparty_code = None
            self.department_code = None
            self.cost_code = None
            self.description_template = "Thu tiền bán hàng khách lẻ"
            
    rule = MockRule()
    
    # Mock the counterparty extractor
    processor.counterparty_extractor = Mock()
    processor.counterparty_extractor.extract_and_match_all = Mock(return_value={
        "counterparties": [
            {
                "code": "KL-GOBARIA1",
                "name": "Khach Le GO BARIA1",
                "address": "Some address",
                "source": "test",
                "condition_applied": "test_bc_kl"
            }
        ],
        "accounts": [],
        "pos_machines": [],
        "departments": []
    })
    
    # Process the transaction
    result = processor.process_transaction_with_rule(transaction, rule)
    
    if result:
        print(f"SUCCESS: BC transaction processed")
        print(f"  Counterparty code: {result.counterparty_code}")
        print(f"  Counterparty name: {result.counterparty_name}")
        print(f"  Description: {result.description}")
        assert "KL" in str(result.counterparty_code), "Counterparty code should contain 'KL'"
        print("  ✓ Counterparty code contains 'KL'")
        return True
    else:
        print("FAILED: BC transaction not processed")
        return False


def test_bc_statement_without_kl_counterparty():
    """Test BC statement processing without KL counterparty - should create default."""
    print("\nTesting BC statement without KL counterparty...")
    
    # Create a mock transaction without KL counterparty
    processor = IntegratedBankProcessor()
    
    # Mock the database connection
    processor.connect = Mock(return_value=True)
    processor.close = Mock()
    
    # Create a test transaction that should get default KL counterparty
    class MockTransaction:
        def __init__(self):
            self.reference = "TEST-BC-NO-KL"
            self.datetime = datetime(2025, 6, 15, 11, 30, 0)
            self.debit_amount = 0
            self.credit_amount = 3000000  # Credit transaction (BC)
            self.balance = 18000000
            self.description = "Thu tiền bán hàng khách lẻ không có mã KL"
    
    transaction = MockTransaction()
    
    # Mock the rule to be BC document type
    class MockRule:
        def __init__(self):
            self.document_type = "BC"
            self.transaction_type = "SALE"
            self.counterparty_code = None
            self.department_code = None
            self.cost_code = None
            self.description_template = "Thu tiền bán hàng khách lẻ"
            
    rule = MockRule()
    
    # Mock the counterparty extractor - returns a counterparty WITHOUT KL in code
    processor.counterparty_extractor = Mock()
    processor.counterparty_extractor.extract_and_match_all = Mock(return_value={
        "counterparties": [
            {
                "code": "31754",  # This is SANG TAM company code, not KL
                "name": "Cong Ty TNHH Sang Tam",
                "address": "Some address",
                "source": "test",
                "condition_applied": "test_bc_no_kl"
            }
        ],
        "accounts": [],
        "pos_machines": [],
        "departments": []
    })
    
    # Process the transaction
    result = processor.process_transaction_with_rule(transaction, rule)
    
    if result:
        print(f"SUCCESS: BC transaction processed")
        print(f"  Counterparty code: {result.counterparty_code}")
        print(f"  Counterparty name: {result.counterparty_name}")
        print(f"  Description: {result.description}")
        
        # Should have created a default KL counterparty
        assert str(result.counterparty_code) == "KL", f"Expected 'KL' counterparty code, got '{result.counterparty_code}'"
        print("  ✓ Default KL counterparty created")
        return True
    else:
        print("FAILED: BC transaction not processed")
        return False


def main():
    """Run all tests."""
    print("Running BC statement KL counterparty tests...\n")
    
    success_count = 0
    total_tests = 2
    
    try:
        if test_bc_statement_with_kl_counterparty():
            success_count += 1
    except Exception as e:
        print(f"ERROR in test 1: {e}")
    
    try:
        if test_bc_statement_without_kl_counterparty():
            success_count += 1
    except Exception as e:
        print(f"ERROR in test 2: {e}")
    
    print(f"\nTest Results: {success_count}/{total_tests} tests passed")
    
    if success_count == total_tests:
        print("✓ All tests PASSED!")
        return True
    else:
        print("✗ Some tests FAILED!")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)