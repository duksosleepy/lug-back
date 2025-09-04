#!/usr/bin/env python3
"""
Integration test to verify VCB enhancement with integrated bank processor

This script tests the VCB enhancements when used through the integrated bank processor.
"""

import sys
from pathlib import Path
from datetime import datetime
import pandas as pd

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.accounting.integrated_bank_processor import IntegratedBankProcessor, RawTransaction


def test_vcb_integration():
    """Test VCB enhancement integration with full processor"""
    
    print("🧪 Testing VCB Enhancement Integration")
    print("=" * 50)
    
    processor = IntegratedBankProcessor()
    
    if not processor.connect():
        print("❌ Failed to connect to processor")
        return
        
    # Set current bank to VCB
    processor.current_bank_name = "VCB"
    processor.current_bank_info = {
        "code": "VCB",
        "name": "NGÂN HÀNG TMCP NGOẠI THƯƠ NG VIỆT NAM",
        "short_name": "VCB",
        "address": "198 Trần Quang Khải, Hoàn Kiếm, Hà Nội"
    }
    processor.default_bank_account = "1121120"  # Set bank account to test value
    
    print(f"🏦 Current Bank: {processor.current_bank_name}")
    print(f"💳 Default Bank Account: {processor.default_bank_account}")
    
    # Create test transactions as a DataFrame (simulating what would come from Excel)
    test_data = [
        {
            "reference": "INT-001",
            "date": datetime(2025, 6, 15),
            "debit": 0,
            "credit": 500000,
            "balance": 10500000,
            "description": "INTEREST PAYMENT"
        },
        {
            "reference": "FEE-001",
            "date": datetime(2025, 6, 15),
            "debit": 150000,
            "credit": 0,
            "balance": 10350000,
            "description": "THU PHI QLTK TO CHUC-VND"
        },
        {
            "reference": "TRF-001",
            "date": datetime(2025, 6, 15),
            "debit": 10000000,
            "credit": 0,
            "balance": 350000,
            "description": "IBVCB.1706250930138002.034244.IBTC.Chuyen tien tu TK VCB (6868) Sang Tam qua TK BIDV (7655) Sang Tam"
        }
    ]
    
    transactions_df = pd.DataFrame(test_data)
    
    # Process using the integrated processor
    print("\n🔄 Processing transactions through IntegratedBankProcessor...")
    result_df = processor.process_to_saoke(transactions_df)
    
    if result_df.empty:
        print("❌ No results returned from processing")
        return
        
    print(f"✅ Processed {len(result_df)} records")
    
    # Check results
    for idx, row in result_df.iterrows():
        print(f"\n📝 Record {idx + 1}:")
        print(f"  Reference: {row.get('reference', 'N/A')}")
        print(f"  Description: {row.get('description', 'N/A')}")
        print(f"  Counterparty: {row.get('counterparty_code', 'N/A')} - {row.get('counterparty_name', 'N/A')}")
        print(f"  Debit Account: {row.get('debit_account', 'N/A')}")
        print(f"  Credit Account: {row.get('credit_account', 'N/A')}")
        print(f"  Amount: {row.get('amount1', 0):,.0f}")
        
        # Verify specific business requirements
        if "INTEREST PAYMENT" in row.get("original_description", ""):
            if row.get("debit_account") == "1121120" and row.get("credit_account") == "5154":
                print("  ✅ Interest Payment - Business Logic CORRECT")
            else:
                print("  ❌ Interest Payment - Business Logic INCORRECT")
                
        elif "THU PHI QLTK TO CHUC-VND" in row.get("original_description", ""):
            if row.get("debit_account") == "6427" and row.get("credit_account") == "1121120":
                print("  ✅ Account Fee - Business Logic CORRECT")
            else:
                print("  ❌ Account Fee - Business Logic INCORRECT")
                
        elif "IBVCB" in row.get("original_description", ""):
            # Check if this is the main record or fee record
            if "Phí chuyển tiền ST" in row.get("description", ""):
                # Fee record
                if (row.get("debit_account") == "6427" and 
                    row.get("credit_account") == "1121120" and
                    row.get("counterparty_code") == "31754"):
                    print("  ✅ Transfer Fee Record - Business Logic CORRECT")
                else:
                    print("  ❌ Transfer Fee Record - Business Logic INCORRECT")
            else:
                # Main record
                if row.get("debit_account") == "1131" and row.get("credit_account") == "1121120":
                    print("  ✅ Transfer Main Record - Business Logic CORRECT")
                else:
                    print("  ❌ Transfer Main Record - Business Logic INCORRECT")
    
    processor.close()
    
    print("\n" + "=" * 50)
    print("✅ VCB Enhancement Integration Testing Complete!")


if __name__ == "__main__":
    test_vcb_integration()