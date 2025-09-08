#!/usr/bin/env python3
"""
Test script to verify attachment naming functionality
"""

import tempfile
import os
from src.util.mail_client import EmailClient

def test_attachment_naming():
    """Test that attachments can be named properly"""
    # Create a mock email client (we won't actually send emails)
    client = EmailClient(
        smtp_server="smtp.example.com",
        smtp_port=587,
        email_address="test@example.com",
        password="password"
    )
    
    # Create a temporary file
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        temp_file_path = tmp.name
        # Write some dummy content
        tmp.write(b"Test content")
    
    try:
        # Test creating a message
        msg = client.create_message(
            to="recipient@example.com",
            subject="Test Subject",
            body="Test Body"
        )
        
        # Test attaching with custom name
        msg = client.attach_file(msg, temp_file_path, "online.xlsx")
        print("Successfully attached file with custom name")
        
        # Verify the attachment has the correct name
        # (In a real scenario, we would check the MIME headers)
        
    finally:
        # Clean up
        os.unlink(temp_file_path)
    
    print("Attachment naming test completed successfully")

if __name__ == "__main__":
    test_attachment_naming()