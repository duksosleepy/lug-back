import re

def _format_onl_kdv_po_description(description: str):
    # More precise pattern to capture only PO numbers (alphanumeric codes starting with letters)
    # This pattern looks for PO codes that start with letters and may contain numbers
    pattern = r"ONL\s+KDV\s+PO\s+([A-Z][A-Z0-9]*(?:\s+[A-Z][A-Z0-9]*)*)"
    match = re.search(pattern, description, re.IGNORECASE)

    if match:
        # Extract all PO numbers and take the last one
        po_numbers_text = match.group(1).strip()
        po_numbers = po_numbers_text.split()

        if po_numbers:
            # Always use the last PO number (for both single and multiple cases)
            last_po_number = po_numbers[-1]
            return f"Thu tiền KH ONLINE thanh toán cho PO: {last_po_number}"

    return None

# Test with the example
description = "ONL KDV PO SON19448 SON19538   Ma g iao dich  Trace372024 Trace 372024"
result = _format_onl_kdv_po_description(description)
print(f"Result: {result}")

# Let's also test the regex pattern
pattern = r"ONL\s+KDV\s+PO\s+([A-Z][A-Z0-9]*(?:\s+[A-Z][A-Z0-9]*)*)"
match = re.search(pattern, description, re.IGNORECASE)
if match:
    print(f"Regex match group 1: '{match.group(1)}'")
    po_numbers_text = match.group(1).strip()
    po_numbers = po_numbers_text.split()
    print(f"PO numbers: {po_numbers}")
    if po_numbers:
        last_po_number = po_numbers[-1]
        print(f"Last PO number: {last_po_number}")