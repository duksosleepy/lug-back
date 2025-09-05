#!/usr/bin/env python3
"""
Debug script to test Vietnamese person name patterns
"""

import re

# Test cases
test_cases = [
    "MA THI BICH NGOC  chuyen tien",
    "TANG THI THU THAO chuyen tien", 
    "hong ly nguyen",
]

# Current patterns
person_patterns = [
    # Existing patterns
    (
        r"cho\s+([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+){1,5})(?:\s+[-]|$)",
        "person",
    ),
    (
        r"gui cho\s+([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+){1,5})(?:\s+[-]|$)",
        "person",
    ),
    (
        r"cho\s+(?:ong|ba)\s+([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+){1,5})(?:\s+[-]|$)",
        "person",
    ),
    (
        r"hoan tien(?:\s+don hang)?(?:\s+cho)?\s+([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+){1,5})(?:\s+[-]|$)",
        "person",
    ),
    (
        r"thanh toan cho\s+([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+){1,5})(?:\s+[-]|$)",
        "person",
    ),
    (
        r"chuyen tien cho\s+([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+){1,5})(?:\s+[-]|$)",
        "person",
    ),
    (
        r"(?:cho|gui|chuyen khoan|thanh toan).*?([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+){1,5})\s+-",
        "person",
    ),
    # NEW PATTERNS FOR SPECIFIC TEST CASES:
    # Pattern for names followed by "chuyen tien" (covers both uppercase and mixed case)
    (
        r"([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+){1,5})\s+chuyen tien",
        "person",
    ),
    # Pattern for lowercase names (like "hong ly nguyen")
    (
        r"\b([a-z][a-z]+(?:\s+[a-z][a-z]+){1,5})\b",
        "person",
    ),
]

print("🔍 Testing Vietnamese Person Name Patterns")
print("=" * 50)

for i, description in enumerate(test_cases, 1):
    print(f"\n📝 Test {i}: {description}")
    
    # Test each pattern
    for j, (pattern, _) in enumerate(person_patterns, 1):
        match = re.search(pattern, description, re.IGNORECASE)
        if match:
            print(f"   ✅ Pattern {j}: '{pattern}' -> Match: '{match.group(1)}'")
        else:
            print(f"   ❌ Pattern {j}: '{pattern}' -> No match")
    
    # Test all patterns together
    person_name = ""
    for pattern, _ in person_patterns:
        match = re.search(pattern, description, re.IGNORECASE)
        if match:
            person_name = match.group(1).strip()
            break
    
    if person_name:
        print(f"   🎯 Final Result: '{person_name}'")
    else:
        print(f"   ❌ Final Result: No person name found")