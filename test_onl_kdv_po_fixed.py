import re

def _format_onl_kdv_po_description(description: str):
    # More precise pattern to capture only PO numbers (alphanumeric codes)
    # This pattern looks for PO codes that are alphanumeric and start with letters
    pattern = r"ONL\s+KDV\s+PO\s+([A-Z0-9]+(?:\s+[A-Z0-9]+)*)"
    match = re.search(pattern, description, re.IGNORECASE)

    if match:
        # Extract all PO numbers and take the last one
        po_numbers_text = match.group(1).strip()
        # Split by whitespace and filter for valid PO number patterns
        po_numbers = po_numbers_text.split()
        
        # Filter to only include valid PO numbers (not text words)
        valid_po_numbers = []
        for po_num in po_numbers:
            # Check if it's a valid PO number (contains both letters and numbers or just numbers)
            if re.match(r"^[A-Z0-9]+$", po_num) and (any(c.isdigit() for c in po_num) or len(po_num) > 3):
                valid_po_numbers.append(po_num)

        if valid_po_numbers:
            # Always use the last valid PO number (for both single and multiple cases)
            last_po_number = valid_po_numbers[-1]
            return f"Thu tiền KH online thanh toán cho PO: {last_po_number}"

    # If we can't extract PO numbers properly, try to extract trace/ACSP numbers
    # Simulate trace extraction for testing
    trace_matches = re.findall(r"[Tt]race\s*(\d+)", description)
    if trace_matches:
        return f"Thu tiền KH online thanh toán cho PO: {trace_matches[0]}"
        
    return None

# Test with the example
description = "ONL KDV PO SON19448 SON19538   Ma g iao dich  Trace372024 Trace 372024"
result = _format_onl_kdv_po_description(description)
print(f"Result: {result}")

# Let's also test the regex pattern
pattern = r"ONL\s+KDV\s+PO\s+([A-Z0-9]+(?:\s+[A-Z0-9]+)*)"
match = re.search(pattern, description, re.IGNORECASE)
if match:
    print(f"Regex match group 1: '{match.group(1)}'")
    po_numbers_text = match.group(1).strip()
    po_numbers = po_numbers_text.split()
    print(f"All PO numbers: {po_numbers}")
    
    # Filter to only include valid PO numbers (not text words)
    valid_po_numbers = []
    for po_num in po_numbers:
        # Check if it's a valid PO number (contains both letters and numbers or just numbers)
        if re.match(r"^[A-Z0-9]+$", po_num) and (any(c.isdigit() for c in po_num) or len(po_num) > 3):
            valid_po_numbers.append(po_num)
    
    print(f"Valid PO numbers: {valid_po_numbers}")
    if valid_po_numbers:
        last_po_number = valid_po_numbers[-1]
        print(f"Last valid PO number: {last_po_number}")
        
    # Also test trace extraction
    trace_matches = re.findall(r"[Tt]race\s*(\d+)", description)
    print(f"Trace matches: {trace_matches}")