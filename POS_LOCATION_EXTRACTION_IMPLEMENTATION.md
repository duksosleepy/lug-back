# POS Location Extraction Implementation Summary

## Problem Statement
The requirement was to modify the POS transaction description formatting to include the location information extracted from the Ma_Dt field.

For POS statements like:
- "TT POS 14100373 21 4200 222264 VCI"

The Dien_Giai should be formatted as:
- "Thu tiền bán hàng khách lẻ (POS {pos_code} - {pos_location})"

Where `pos_location` is derived from the Ma_Dt field by:
1. Splitting the Ma_Dt by "-"
2. Taking the last element
3. Example: "KL-BARIA1" → "BARIA1" → "BARIA" (after removing trailing digits)

## Solution Implemented

### 1. Completed the `get_description_department_code` Method

The method was previously declared but not implemented. I've implemented it to:

1. Split the department code by "-" or "_" 
2. Take the last element
3. Remove trailing digits for cleaner formatting
4. Handle various edge cases

```python
def get_description_department_code(self, department_code: str) -> str:
    """Get department code for description formatting (different from counterparty search)

    For POS description formatting, we want a cleaner format:
    - Input: "DD.TINH_GO BRVT1"
    - Output: "GO BRVT" (removes trailing digits)

    This is different from clean_department_code which is used for counterparty search.
    
    For the new requirement:
    - Input: "KL-BARIA1" (from Ma_Dt field)
    - Output: "BARIA1" (split by "-" and get last element)
    """
    if not department_code or not isinstance(department_code, str):
        return ""

    # Strip whitespace
    original_code = department_code.strip()
    
    # For the new requirement: split by "-" and get the last element
    # Example: "KL-BARIA1" -> "BARIA1"
    if "-" in original_code:
        parts = original_code.split("-")
        if parts:
            last_element = parts[-1]  # Get the last element
            
            # Remove trailing digits for cleaner format
            # Example: "BARIA1" -> "BARIA"
            cleaned_code = re.sub(r"\d+$", "", last_element).strip()
            return cleaned_code
    
    # Fallback to existing logic for other formats
    # Split by "_" if present
    if "_" in original_code:
        parts = original_code.split("_")
        if parts:
            last_element = parts[-1].strip()
            # Remove trailing digits
            cleaned_code = re.sub(r"\d+$", "", last_element).strip()
            return cleaned_code
    
    # If no separators, try to clean the original code
    # Remove trailing digits
    cleaned_code = re.sub(r"\d+$", "", original_code).strip()
    return cleaned_code
```

### 2. Test Results

The implementation was tested with various examples:

| Input (Ma_Dt)     | Output (POS Location) | Resulting Description Format                  |
|-------------------|-----------------------|-----------------------------------------------|
| "KL-BARIA1"       | "BARIA"               | "Thu tiền bán hàng khách lẻ (POS {pos_code} - BARIA)" |
| "KL-VUNGTAU2"     | "VUNGTAU"             | "Thu tiền bán hàng khách lẻ (POS {pos_code} - VUNGTAU)" |
| "KL-HANOI"        | "HANOI"               | "Thu tiền bán hàng khách lẻ (POS {pos_code} - HANOI)" |
| "DD.TINH_GO BRVT1"| "GO BRVT"             | "Thu tiền bán hàng khách lẻ (POS {pos_code} - GO BRVT)" |
| "KL-DANANG3"      | "DANANG"              | "Thu tiền bán hàng khách lẻ (POS {pos_code} - DANANG)" |

All tests passed successfully, demonstrating that the implementation correctly extracts and formats the POS location from the Ma_Dt field.

### 3. Integration

The method is now properly integrated into the POS description formatting logic in the `process_transaction` method, where it's used to format the description for POS transactions:

```python
# Get cleaned department code for description
description_dept_code = self.get_description_department_code(
    pos_department_code
)

# Build the complete description in new format
if description_dept_code:
    description = f"{base_description} (POS {pos_code} - {description_dept_code})"
else:
    description = f"{base_description} (POS {pos_code})"
```

## Conclusion

The implementation successfully addresses the business requirement to include location information in POS transaction descriptions. The solution is robust, handles various edge cases, and maintains backward compatibility with existing formats.