import re

# Test the regex patterns with the example
description = "ONL KDV PO SON19448 SON19538   Ma g iao dich  Trace372024 Trace 372024"

# New improved pattern
trace_matches = re.findall(r"[Tt]race\s*(\d+)", description)
print("New pattern results:", trace_matches)

# Old pattern
trace_match = re.search(r"[Tt]race[^\d]*(\d+)", description)
if trace_match:
    print("Old pattern result:", trace_match.group(1))
else:
    print("Old pattern result: None")

# ACSP pattern
acsp_matches = re.findall(r"[Aa]\s*[Cc]\s*[Ss]?\s*[Pp]\s*(\d+)", description)
print("ACSP pattern results:", acsp_matches)