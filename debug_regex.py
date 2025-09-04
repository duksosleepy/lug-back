#!/usr/bin/env python3
import re

# Test the regex patterns
description = "IBVCB.1806250063454006.043183.IBTC.Chuyen tien tu TK VCB (6868) Sang Tam qua TK ACB (9139) Sang Tam"

print(f"Testing description: {description}")

# Pattern 1: Parentheses format
pattern1 = r"tu\s+tk\s+([A-Z]+)\s*$(\d+)$\s+sang\s+tam.*?qua\s+tk\s+([A-Z]+)\s*$(\d+)$\s+sang\s+tam"
match1 = re.search(pattern1, description, re.IGNORECASE)

if match1:
    print(f"Pattern 1 match: {match1.groups()}")
else:
    print("Pattern 1: No match")

# Pattern 2: Space format
pattern2 = r"tu\s+tk\s+([A-Z]+)\s+(\d+)\s+sang\s+tam.*?qua\s+tk\s+([A-Z]+)\s+(\d+)\s+sang\s+tam"
match2 = re.search(pattern2, description, re.IGNORECASE)

if match2:
    print(f"Pattern 2 match: {match2.groups()}")
else:
    print("Pattern 2: No match")

# Pattern 3: More flexible
pattern3 = r"tu\s+tk\s+([A-Z]+).*?(\d+).*?sang\s+tam.*?qua\s+tk\s+([A-Z]+).*?(\d+).*?sang\s+tam"
match3 = re.search(pattern3, description, re.IGNORECASE)

if match3:
    print(f"Pattern 3 match: {match3.groups()}")
else:
    print("Pattern 3: No match")