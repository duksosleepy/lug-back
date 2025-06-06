#!/usr/bin/env python3
"""
Migration Script for Integrated Bank Processor

This script helps migrate from the old processor to the new integrated processor.
It installs any necessary dependencies and updates the database schema.
"""

import os
import sys
from pathlib import Path
import subprocess
import sqlite3

# Add the project root to the path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.util.logging import get_logger

logger = get_logger(__name__)


def check_dependencies():
    """Check and install required dependencies"""
    logger.info("Checking dependencies...")
    
    required_packages = [
        "limbo",
        "pandas",
        "tantivy",
        "openpyxl",
        "odfpy"
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
            logger.info(f"✓ {package} is installed")
        except ImportError:
            missing_packages.append(package)
            logger.warning(f"✗ {package} is not installed")
    
    if missing_packages:
        logger.info(f"Installing missing packages: {', '.join(missing_packages)}")
        subprocess.check_call([sys.executable, "-m", "pip", "install"] + missing_packages)
        logger.info("All dependencies installed successfully")
    else:
        logger.info("All dependencies are already installed")


def update_database_schema():
    """Update the database schema to support counterparty-based account mapping"""
    logger.info("Updating database schema...")
    
    db_path = Path(__file__).parent / "banking_enterprise.db"
    
    if not db_path.exists():
        logger.error(f"Database not found at {db_path}")
        return False
    
    try:
        # Connect to SQLite database
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        
        # Check if we need to add COUNTERPARTY rule_type to account_mapping_rules
        cursor.execute("""
            SELECT COUNT(*) FROM account_mapping_rules 
            WHERE rule_type = 'COUNTERPARTY'
        """)
        count = cursor.fetchone()[0]
        
        if count == 0:
            logger.info("Adding counterparty-based account mapping rules...")
            
            # Add sample counterparty mapping rules
            cursor.execute("""
                INSERT INTO account_mapping_rules 
                (rule_type, entity_code, document_type, transaction_type, 
                 debit_account, credit_account, priority, is_active) 
                VALUES
                ('COUNTERPARTY', 'BIDV', 'BC', 'INTEREST', '1121114', '5154', 95, 1),
                ('COUNTERPARTY', 'BIDV', 'BN', 'FEE', '6278', '1121114', 95, 1)
            """)
            
            conn.commit()
            logger.info("Added sample counterparty mapping rules")
        else:
            logger.info("Counterparty mapping rules already exist")
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Error updating database schema: {e}")
        return False


def create_integrated_test():
    """Create a test file for the integrated processor"""
    test_file_path = Path(__file__).parent / "test_integrated.py"
    
    if test_file_path.exists():
        logger.info(f"Test file already exists at {test_file_path}")
        return
    
    test_code = """#!/usr/bin/env python3
# Simple test for the integrated processor
from src.accounting.integrated_processor import IntegratedBankProcessor

# Initialize processor
processor = IntegratedBankProcessor()

if processor.connect():
    try:
        # Process a sample transaction
        result = processor.process_to_saoke("BIDV 3840.ods", "test_output.ods")
        print(f"Processed {len(result)} transactions")
    finally:
        processor.close()
"""
    
    with open(test_file_path, "w") as f:
        f.write(test_code)
        
    logger.info(f"Created test file at {test_file_path}")


def main():
    """Main migration function"""
    logger.info("Starting migration to integrated processor...")
    
    # Check dependencies
    check_dependencies()
    
    # Update database schema
    if update_database_schema():
        logger.info("Database schema updated successfully")
    else:
        logger.warning("Database schema update failed")
    
    # Create test file
    create_integrated_test()
    
    logger.info("Migration completed")
    
    print("\nMigration to integrated processor completed!")
    print("You can now use the integrated processor with:")
    print("  from src.accounting.integrated_processor import IntegratedBankProcessor")
    print("\nTest with:")
    print("  python src/accounting/test_integrated_processor.py")


if __name__ == "__main__":
    main()
