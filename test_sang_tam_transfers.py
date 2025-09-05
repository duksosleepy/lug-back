#!/usr/bin/env python3
"""
Test script to verify Sang Tam transfer account handling
"""

import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.accounting.integrated_bank_processor import IntegratedBankProcessor, RawTransaction


def test_sang_tam_transfers():
    """Test Sang Tam transfer account handling"""
    
    processor = IntegratedBankProcessor()
    
    if not processor.connect():
        print("❌ Failed to connect to processor")
        return
        
    print("🧪 Testing Sang Tam Transfer Account Handling")
    print("=" * 50)
    
    # Test Case 1: BIDV to ACB transfer (as receipt/BC)
    print("\n💳 TEST CASE 1: BIDV to ACB Transfer (BC - Receipt)")
    transaction1 = RawTransaction(
        reference="TEST-001",
        datetime=datetime(2025, 6, 14, 18, 11, 30),
        debit_amount=0,  # This will be a receipt (BC)
        credit_amount=5000000,
        balance=55065443,
        description="CHUYEN TIEN TU TK BIDV 3840 SANG TAM QUA TK ACB 8368 SANG TAM GD 966035-061425 18:11:30",
    )
    
    print(f"\n  🔍 Test: {transaction1.description}")
    entry1 = processor.process_transaction(transaction1)
    
    if entry1:
        print(f"    ✅ Transaction Processed")
        print(f"    📝 Description: {entry1.description}")
        print(f"    💳 Debit Account: {entry1.debit_account}")
        print(f"    💳 Credit Account: {entry1.credit_account}")
        print(f"    📄 Document Type: {entry1.document_type}")
        
        # Expected for BC: debit_account should be destination account (1121124), credit_account should be 1311
        if entry1.debit_account == "1121124" and entry1.credit_account == "1311":
            print(f"    🎯 ✅ CORRECT: Debit={entry1.debit_account}, Credit={entry1.credit_account}")
        else:
            print(f"    ❌ INCORRECT: Expected Dr=1121124, Cr=1311")
    else:
        print(f"    ❌ Failed to process")
    
    # Test Case 2: VCB to ACB transfer (with parentheses) (as payment/BN)
    print("\n\n💳 TEST CASE 2: VCB to ACB Transfer (BN - Payment)")
    transaction2 = RawTransaction(
        reference="TEST-002",
        datetime=datetime(2025, 6, 14, 18, 11, 43),
        debit_amount=2000000,  # This will be a payment (BN)
        credit_amount=0,
        balance=55065443,
        description="CHUYEN TIEN TU TK VCB (7803) SANG TAM QUA TK ACB (8368) SANG TAM GD 023763-061425 18:11:43",
    )
    
    print(f"\n  🔍 Test: {transaction2.description}")
    entry2 = processor.process_transaction(transaction2)
    
    if entry2:
        print(f"    ✅ Transaction Processed")
        print(f"    📝 Description: {entry2.description}")
        print(f"    💳 Debit Account: {entry2.debit_account}")
        print(f"    💳 Credit Account: {entry2.credit_account}")
        print(f"    📄 Document Type: {entry2.document_type}")
        
        # Expected for BN: debit_account should be 3311, credit_account should be source account
        if entry2.debit_account == "3311" and entry2.credit_account == "1121117":
            print(f"    🎯 ✅ CORRECT: Debit={entry2.debit_account}, Credit={entry2.credit_account}")
        else:
            print(f"    ❌ INCORRECT: Expected Dr=3311, Cr=1121117")
    else:
        print(f"    ❌ Failed to process")
    
    # Test Case 3: Another VCB to ACB transfer (as receipt/BC)\n    print(\"\\n\\n\\ud83d\\udcb3 TEST CASE 3: Another VCB to ACB Transfer (BC - Receipt)\")\n    transaction3 = RawTransaction(\n        reference=\"TEST-003\",\n        datetime=datetime(2025, 6, 14, 18, 11, 43),\n        debit_amount=0,  # This will be a receipt (BC)\n        credit_amount=2500000,\n        balance=55065443,\n        description=\"CHUYEN TIEN TU TK VCB (6868) SANG TAM QUA TK ACB (8368) SANG TAM GD 023764-061425 18:11:43\",\n    )\n    \n    print(f\"\\n  \\ud83d\\udd0d Test: {transaction3.description}\")\n    entry3 = processor.process_transaction(transaction3)\n    \n    if entry3:\n        print(f\"    \\u2705 Transaction Processed\")\n        print(f\"    \\ud83d\\udcdd Description: {entry3.description}\")\n        print(f\"    \\ud83d\\udcb3 Debit Account: {entry3.debit_account}\")\n        print(f\"    \\ud83d\\udcb3 Credit Account: {entry3.credit_account}\")\n        print(f\"    \\ud83d\\udcc4 Document Type: {entry3.document_type}\")\n        \n        # Expected for BC: debit_account should be destination account (1121124), credit_account should be 1311\n        if entry3.debit_account == \"1121124\" and entry3.credit_account == \"1311\":\n            print(f\"    \\ud83c\\udfaf \\u2705 CORRECT: Debit={entry3.debit_account}, Credit={entry3.credit_account}\")\n        else:\n            print(f\"    \\u274c INCORRECT: Expected Dr=1121124, Cr=1311\")\n    else:\n        print(f\"    \\u274c Failed to process\")
    
    processor.close()
    
    print("\n" + "=" * 50)
    print("✅ Sang Tam Transfer Testing Complete!")


if __name__ == "__main__":
    test_sang_tam_transfers()
