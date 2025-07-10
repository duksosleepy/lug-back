#!/usr/bin/env python3
"""
Enhanced Counterparty Extractor Module

This module provides functionality to extract counterparty names, account numbers,
and POS machine codes from bank transaction descriptions. It uses pattern matching
and fuzzy search to identify entities in complex transaction descriptions.

Enhanced to avoid redundant searches by detecting statement content type and
searching in the appropriate index directly.
"""

import re
from typing import Any, Dict, List

from src.accounting.fast_search import (
    search_accounts,
    search_counterparties,
    search_departments,
    search_exact_counterparties,
    search_pos_machines,
)
from src.util.logging import get_logger

logger = get_logger(__name__)


class CounterpartyExtractor:
    """
    Enhanced extractor that detects and extracts entities from bank transaction descriptions
    using pattern matching and fuzzy search, searching directly in the appropriate index.
    """

    def __init__(self, db_path: str = "banking_enterprise.db"):
        """Initialize the extractor"""
        self.db_path = db_path
        self.logger = logger

        # Business entity indicators to remove from counterparty names
        self.business_entity_indicators = [
            # Vietnamese indicators
            "CTY",
            "CONG TY",
            "TNHH",
            "CO PHAN",
            "CP",
            "TONG CONG TY",
            "CONG TY TNHH",
            "CONG TY CO PHAN",
            # English indicators
            "JSC",
            "LLC",
            "INC",
            "CORP",
            "CORPORATION",
            "LTD",
            "LIMITED",
            "COMPANY",
            "ENTERPRISE",
            "GROUP",
            # Vietnamese variations
            "DOANH NGHIEP",
            "TONG CONG NGHI",
            "CONG NGHI",
        ]

        # Department code replacement mapping for counterparty search
        # This allows mapping of short codes to full names for better matching
        self.department_code_replacements = {
            "BRVT": "BARIA",
            "CTHO": "CANTHO",
            # Add more mappings here in the future as needed
            # "SHORT": "FULL_NAME",
        }

        # Common patterns where counterparty names appear
        self.counterparty_patterns = [
            # Person name patterns - for individual beneficiaries (case insensitive matching)
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
            # More generic pattern to catch names after common phrases
            (
                r"(?:cho|gui|chuyen khoan|thanh toan).*?([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+){1,5})\s+-",
                "person",
            ),
            # Direct pattern for the example case
            (
                r"hoan tien don hang [A-Z0-9]+\d{2}\.\d{2}\.\d{2} cho ([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+){1,5})\s+-",
                "person",
            ),
            # B/O (By Order of) pattern - typically followed by counterparty
            (
                r"B/O\s+(?:\d+\s+)?([A-Z][A-Z\s]+(?:TNHH|CO PHAN|CP|JSC)[A-Z\s]+)",
                "ordering",
            ),
            # F/O (For Order of / in Favor of) pattern - typically followed by counterparty
            (
                r"F/O\s+(?:\d+\s+)?([A-Z][A-Z\s]+(?:TNHH|CO PHAN|CP|JSC)[A-Z\s]+)",
                "beneficiary",
            ),
            # CTY patterns with corporate type suffix (TNHH, CP, etc.)
            (
                r"CTY\s+(?:TNHH|CO PHAN|CP)[\s\w]+?(?=\s+(?:TT|THANH TOAN|CHUYEN KHOAN|CK|CHI|F/O|B/O|\d{6}))",
                "company",
            ),
            (r"CTY\s+(?:TNHH|CO PHAN|CP)[\s\w]+", "company"),
            # CTY patterns without corporate type suffix
            (
                r"CTY\s+([A-Z][A-Z\s]{2,}?)(?=\s+(?:TT|THANH TOAN|CHUYEN KHOAN|CK|CHI|F/O|B/O|\d{6}|T\d{1,2}|THANG))",
                "company",
            ),
            (r"CHO\s+CTY\s+([A-Z][A-Z\s]{2,})", "beneficiary"),
            (r"TAI\s+CRM\s+CHO\s+CTY\s+([A-Z][A-Z\s]{2,})", "beneficiary"),
            # CONG TY patterns
            (
                r"CONG TY\s+(?:TNHH|CO PHAN|CP)[\s\w]+?(?=\s+(?:TT|THANH TOAN|CHUYEN KHOAN|CK|CHI|F/O|B/O|\d{6}))",
                "company",
            ),
            (r"CONG TY\s+(?:TNHH|CO PHAN|CP)[\s\w]+", "company"),
            # Company name after numbers - common pattern in transfers
            (
                r"\d{5,}(?:\s+|-)([A-Z][A-Z\s]+(?:TNHH|CO PHAN|CP|JSC)[A-Z\s]+)(?=\s+(?:TT|THANH TOAN|CK|CHI))",
                "after_number",
            ),
            # NHH pattern (often used for company code)
            (
                r"NHH\d+\s+([A-Z][A-Z\s]+(?:TNHH|CO PHAN|CP)[A-Z\s]+)",
                "nhh_code",
            ),
        ]

        # Account number patterns
        self.account_patterns = [
            # Bank account numbers (10+ digits)
            (r"\b(\d{10,})\b", "bank_account"),
            # Account numbers with TK prefix
            (r"TK\s+(\d{5,})", "account_ref"),
            (r"STK\s+(\d{5,})", "account_ref"),
            (r"TAI KHOAN\s+(\d{5,})", "account_ref"),
            (r"SO TK\s+(\d{5,})", "account_ref"),
            # BIDV specific patterns
            (r"BIDV\s+(\d{4})", "bidv_account"),
            # Transfer patterns
            (r"TU\s+(?:TK\s+)?(\d{5,}).*?(?:DEN|QUA|SANG)", "from_account"),
            (r"(?:DEN|QUA|SANG)\s+(?:TK\s+)?(\d{5,})", "to_account"),
        ]

        # POS machine patterns
        self.pos_patterns = [
            (r"POS\s*(\d{7,8})", "pos_code"),
            (r"TT POS\s*(\d{7,8})", "pos_code"),
            (r"THANH TOAN POS\s*(\d{7,8})", "pos_code"),
        ]

        # Department patterns
        self.department_patterns = [
            (r"BP\s*(\w{2,6})", "department_code"),
            (r"BO PHAN\s*(\w{2,6})", "department_code"),
            (r"MA BP\s*(\w{2,6})", "department_code"),
            (r"PHONG BAN\s*(\w{2,6})", "department_code"),
        ]

        # Specific ending words that might indicate the end of a company name
        self.company_end_markers = [
            r"TT",
            r"THANH TOAN",
            r"CHUYEN KHOAN",
            r"CK",
            r"CHI",
            r"F/O",
            r"B/O",
            r"LAI",
            r"PHI",
            r"TIEN",
            r"NAP",
            r"RUT",
            r"VAT",
            r"HOA DON",
            r"HD",
            r"THANG",
            r"QUY",
            r"NAM",
            r"T\d{1,2}",
            r"Q\d{1,2}",
            r"\d{4}",
        ]

        # Common words that indicate a company name might follow
        self.company_indicators = [
            "CTY",
            "CONG TY",
            "TNHH",
            "CO PHAN",
            "CP",
            "JSC",
            "LLC",
            "CHO CTY",
        ]

        # Words that should be ignored/removed when cleaning company names
        self.stopwords = [
            "TT",
            "THANH TOAN",
            "CHUYEN KHOAN",
            "CHI",
            "THU",
            "TIEN",
            "CK DEN",
            "CK DI",
            "CK",
            "REF",
            "FROM",
            "TO",
            "CHO",
            "TAI CRM",
            "TAI",
            "CRM",
        ]

    def clean_counterparty_name(self, name: str) -> str:
        """
        Clean counterparty name by removing business entity indicators and common words.

        This function removes:
        - Business entity indicators (CTY, TNHH, CO PHAN, etc.)
        - Extra whitespace and formatting
        - Common transaction-related words

        Args:
            name: Raw counterparty name

        Returns:
            Cleaned counterparty name
        """
        if not name or not isinstance(name, str):
            return name

        # Start with the original name
        cleaned = name.strip().upper()

        # Log the original name for debugging
        self.logger.debug(f"Cleaning counterparty name: '{name}'")

        # Remove business entity indicators from the beginning and end
        for indicator in sorted(
            self.business_entity_indicators, key=len, reverse=True
        ):
            # Remove from beginning (with word boundary)
            pattern_start = rf"^{re.escape(indicator)}\s+"
            cleaned = re.sub(pattern_start, "", cleaned)

            # Remove from end (with word boundary)
            pattern_end = rf"\s+{re.escape(indicator)}$"
            cleaned = re.sub(pattern_end, "", cleaned)

            # Remove standalone indicators between spaces
            pattern_standalone = rf"\s+{re.escape(indicator)}\s+"
            cleaned = re.sub(pattern_standalone, " ", cleaned)

        # Remove stopwords that might interfere with the business name
        for stopword in self.stopwords:
            # Remove stopwords at the beginning or end
            pattern_start = rf"^{re.escape(stopword)}\s+"
            cleaned = re.sub(pattern_start, "", cleaned, flags=re.IGNORECASE)

            pattern_end = rf"\s+{re.escape(stopword)}$"
            cleaned = re.sub(pattern_end, "", cleaned, flags=re.IGNORECASE)

        # Clean up extra whitespace and special characters
        cleaned = re.sub(
            r"\s+", " ", cleaned
        )  # Multiple spaces to single space
        cleaned = re.sub(r"^[\s\-_,.:;]+", "", cleaned)  # Leading punctuation
        cleaned = re.sub(r"[\s\-_,.:;]+$", "", cleaned)  # Trailing punctuation
        cleaned = cleaned.strip()

        # Ensure we don't return an empty string
        if not cleaned:
            # If cleaning removed everything, use a more conservative approach
            cleaned = name.strip()
            # Just remove the most common indicators
            for indicator in ["CTY", "CONG TY", "TNHH", "CO PHAN"]:
                cleaned = re.sub(
                    rf"^{re.escape(indicator)}\s+",
                    "",
                    cleaned,
                    flags=re.IGNORECASE,
                )
                cleaned = re.sub(
                    rf"\s+{re.escape(indicator)}$",
                    "",
                    cleaned,
                    flags=re.IGNORECASE,
                )
            cleaned = cleaned.strip()

        # Final check - if still empty, return original
        if not cleaned:
            cleaned = name.strip()

        # Convert to title case for better readability
        cleaned = cleaned.title()

        # Log the result
        if cleaned != name:
            self.logger.debug(f"Cleaned '{name}' -> '{cleaned}'")

        return cleaned

    def extract_entity_info(self, description: str) -> Dict[str, List[Dict]]:
        """
        Extract all types of entities from a transaction description.
        This is the main enhancement - a unified method to extract multiple entity types.

        Args:
            description: The transaction description text

        Returns:
            Dictionary with keys 'counterparties', 'accounts', 'pos_machines', 'departments'
            each containing a list of extracted entities with relevant metadata
        """
        # Create result structure
        results = {
            "counterparties": [],
            "accounts": [],
            "pos_machines": [],
            "departments": [],
        }

        # Save original case for person name extraction
        original_desc = description

        # Normalize description to uppercase for pattern matching
        normalized_desc = description.upper()

        # Extract counterparties
        counterparties = self.extract_counterparties(description)
        if counterparties:
            results["counterparties"] = counterparties

        # Extract account numbers
        accounts = self._extract_accounts(normalized_desc)
        if accounts:
            results["accounts"] = accounts

        # Extract POS machines
        pos_machines = self._extract_pos_machines(normalized_desc)
        if pos_machines:
            results["pos_machines"] = pos_machines

        # Extract departments
        departments = self._extract_departments(normalized_desc)
        if departments:
            results["departments"] = departments

        return results

    def extract_counterparties(self, description: str) -> List[Dict[str, str]]:
        """
        Extract potential counterparty names from a transaction description.

        Args:
            description: The transaction description text

        Returns:
            List of dicts with counterparty info (name, type, confidence)
        """
        # Save original case for person name extraction
        original_desc = description

        # Normalize description to uppercase for company pattern matching
        normalized_desc = description.upper()

        counterparties = []
        matched_spans = []  # Keep track of text spans that have been matched

        # First try to find person names using original case (for proper capitalization)
        person_patterns = [
            pattern
            for pattern, party_type in self.counterparty_patterns
            if party_type == "person"
        ]
        for pattern in person_patterns:
            for match in re.finditer(pattern, original_desc, re.IGNORECASE):
                name = match.group(1) if match.lastindex else match.group(0)
                span = match.span()

                # Check if this span overlaps with any previously matched span
                overlap = False
                for prev_span in matched_spans:
                    if max(prev_span[0], span[0]) < min(prev_span[1], span[1]):
                        overlap = True
                        break

                if not overlap:
                    # Keep original capitalization for person names but clean them
                    name = name.strip()

                    # Validate that this is a person name
                    if self._is_valid_person_name(name):
                        # Clean person names (less aggressive than company names)
                        cleaned_name = self._clean_person_name(name)
                        counterparties.append(
                            {
                                "name": cleaned_name,
                                "type": "person",
                                "confidence": 0.9,  # High confidence for person name match
                                "span": span,
                            }
                        )
                        matched_spans.append(span)

        # Then try company patterns using uppercase
        company_patterns = [
            item for item in self.counterparty_patterns if item[1] != "person"
        ]
        for pattern, party_type in company_patterns:
            for match in re.finditer(pattern, normalized_desc):
                name = match.group(1) if match.lastindex else match.group(0)
                # If the pattern doesn't have a capturing group, extract the part after the indicator
                if not match.lastindex and "CTY" in name:
                    parts = name.split("CTY", 1)
                    if len(parts) > 1:
                        name = "CTY" + parts[1]
                span = match.span()

                # Check if this span overlaps with any previously matched span
                overlap = False
                for prev_span in matched_spans:
                    if max(prev_span[0], span[0]) < min(prev_span[1], span[1]):
                        overlap = True
                        break

                if not overlap:
                    # Clean up the extracted company name using the new cleaning function
                    cleaned_name = self.clean_counterparty_name(name)
                    if (
                        cleaned_name and len(cleaned_name) > 3
                    ):  # Minimum length to be considered valid after cleaning
                        counterparties.append(
                            {
                                "name": cleaned_name,
                                "type": party_type,
                                "confidence": 0.8,  # Base confidence for pattern match
                                "span": span,
                            }
                        )
                        matched_spans.append(span)

        # If no counterparties found with patterns, try a more generic extraction
        if not counterparties:
            # Look for company indicators
            for indicator in self.company_indicators:
                idx = normalized_desc.find(indicator)
                while idx >= 0:  # Find all occurrences
                    # Extract text after the indicator until a common end marker
                    start_pos = idx
                    end_pos = len(normalized_desc)

                    for marker in self.company_end_markers:
                        marker_match = re.search(marker, normalized_desc[idx:])
                        if marker_match:
                            marker_pos = idx + marker_match.start()
                            if marker_pos > idx and marker_pos < end_pos:
                                end_pos = marker_pos

                    potential_name = normalized_desc[start_pos:end_pos].strip()
                    cleaned_name = self.clean_counterparty_name(potential_name)

                    if cleaned_name and len(cleaned_name) > 3:
                        counterparties.append(
                            {
                                "name": cleaned_name,
                                "type": "generic",
                                "confidence": 0.6,  # Lower confidence for generic extraction
                                "span": (start_pos, end_pos),
                            }
                        )

                    # Look for next occurrence
                    idx = normalized_desc.find(indicator, idx + len(indicator))

        # Sort by confidence (highest first)
        counterparties.sort(key=lambda x: x["confidence"], reverse=True)

        # Remove duplicate names
        unique_counterparties = []
        seen_names = set()

        for party in counterparties:
            if party["name"] not in seen_names:
                seen_names.add(party["name"])
                # Remove the span info before returning
                party.pop("span", None)
                unique_counterparties.append(party)

        return unique_counterparties

    def _clean_person_name(self, name: str) -> str:
        """
        Clean person names with less aggressive cleaning than company names

        Args:
            name: Raw person name

        Returns:
            Cleaned person name
        """
        if not name or not isinstance(name, str):
            return name

        # Just clean up whitespace and basic formatting for person names
        cleaned = name.strip()
        cleaned = re.sub(
            r"\s+", " ", cleaned
        )  # Multiple spaces to single space
        cleaned = re.sub(r"^[\s\-_,.:;]+", "", cleaned)  # Leading punctuation
        cleaned = re.sub(r"[\s\-_,.:;]+$", "", cleaned)  # Trailing punctuation

        return cleaned.strip()

    def _extract_accounts(self, description: str) -> List[Dict[str, Any]]:
        """
        Extract account numbers from a transaction description.

        Args:
            description: The normalized (uppercase) transaction description

        Returns:
            List of dicts with account info (code, type, position, confidence)
        """
        accounts = []
        matched_spans = []

        for pattern, acc_type in self.account_patterns:
            for match in re.finditer(pattern, description):
                code = match.group(1)
                span = match.span()

                # Check for overlap
                overlap = False
                for prev_span in matched_spans:
                    if max(prev_span[0], span[0]) < min(prev_span[1], span[1]):
                        overlap = True
                        break

                if not overlap and code:
                    # Set confidence based on type
                    confidence = (
                        0.9
                        if acc_type in ["bank_account", "account_ref"]
                        else 0.8
                    )

                    accounts.append(
                        {
                            "code": code,
                            "type": acc_type,
                            "position": span[0],
                            "confidence": confidence,
                            "span": span,
                        }
                    )
                    matched_spans.append(span)

        # Sort by position in text
        accounts.sort(key=lambda x: x["position"])

        # Remove duplicate account numbers and span info
        unique_accounts = []
        seen_codes = set()

        for account in accounts:
            if account["code"] not in seen_codes:
                seen_codes.add(account["code"])
                account.pop("span", None)
                unique_accounts.append(account)

        return unique_accounts

    def _extract_pos_machines(self, description: str) -> List[Dict[str, Any]]:
        """
        Extract POS machine codes from a transaction description.

        Args:
            description: The normalized (uppercase) transaction description

        Returns:
            List of dicts with POS info (code, type, position, confidence)
        """
        pos_machines = []
        matched_spans = []

        for pattern, pos_type in self.pos_patterns:
            for match in re.finditer(pattern, description):
                code = match.group(1)
                span = match.span()

                # Check for overlap
                overlap = False
                for prev_span in matched_spans:
                    if max(prev_span[0], span[0]) < min(prev_span[1], span[1]):
                        overlap = True
                        break

                if not overlap and code:
                    pos_machines.append(
                        {
                            "code": code,
                            "type": pos_type,
                            "position": span[0],
                            "confidence": 0.9,  # High confidence for POS codes
                            "span": span,
                        }
                    )
                    matched_spans.append(span)

        # Remove duplicate POS codes and span info
        unique_pos = []
        seen_codes = set()

        for pos in pos_machines:
            if pos["code"] not in seen_codes:
                seen_codes.add(pos["code"])
                pos.pop("span", None)
                unique_pos.append(pos)

        return unique_pos

    def _extract_departments(self, description: str) -> List[Dict[str, Any]]:
        """
        Extract department codes from a transaction description.

        Args:
            description: The normalized (uppercase) transaction description

        Returns:
            List of dicts with department info (code, type, position, confidence)
        """
        departments = []
        matched_spans = []

        for pattern, dept_type in self.department_patterns:
            for match in re.finditer(pattern, description):
                code = match.group(1)
                span = match.span()

                # Check for overlap
                overlap = False
                for prev_span in matched_spans:
                    if max(prev_span[0], span[0]) < min(prev_span[1], span[1]):
                        overlap = True
                        break

                if not overlap and code:
                    departments.append(
                        {
                            "code": code,
                            "type": dept_type,
                            "position": span[0],
                            "confidence": 0.8,  # Moderate confidence for department codes
                            "span": span,
                        }
                    )
                    matched_spans.append(span)

        # Remove duplicate department codes and span info
        unique_departments = []
        seen_codes = set()

        for dept in departments:
            if dept["code"] not in seen_codes:
                seen_codes.add(dept["code"])
                dept.pop("span", None)
                unique_departments.append(dept)

        return unique_departments

    def _clean_company_name(self, name: str) -> str:
        """
        Clean and normalize a company name (legacy method, use clean_counterparty_name instead)

        Args:
            name: Raw company name extracted from text

        Returns:
            Cleaned company name
        """
        # Use the new cleaning function for consistency
        return self.clean_counterparty_name(name)

    def _is_valid_person_name(self, name: str) -> bool:
        """
        Check if a string looks like a valid Vietnamese person name

        Args:
            name: The name to check

        Returns:
            True if it appears to be a valid person name
        """
        # Clean the name first
        name = name.strip()

        # Check minimum length
        if len(name) < 6:  # Most Vietnamese full names are at least 6 chars
            return False

        # Split into words
        parts = name.split()

        # Check if it has 2-5 parts (Vietnamese names typically have 2-4 words)
        if len(parts) < 2 or len(parts) > 5:
            return False

        # Check if each part starts with a capital letter
        if not all(part[0].isupper() for part in parts if part):
            return False

        # Check if the name contains any company indicators
        company_indicators = [
            "CTY",
            "TNHH",
            "CO PHAN",
            "CP",
            "JSC",
            "LLC",
            "CONG TY",
            "COMPANY",
            "CORPORATION",
            "CORP",
            "INC",
            "ENTERPRISE",
        ]
        if any(indicator in name.upper() for indicator in company_indicators):
            return False

        # Make sure it doesn't contain numbers
        if any(char.isdigit() for char in name):
            return False

        return True

    def search_entities(
        self, entity_info: Dict[str, List[Dict]]
    ) -> Dict[str, List[Dict]]:
        """
        Search for all detected entities in their respective indexes.
        This is a key enhancement that eliminates redundant searches.
        Implements proper two-condition logic for counterparties.

        Args:
            entity_info: Dictionary with extracted entity info from extract_entity_info

        Returns:
            Dictionary with matched entities from database
        """
        results = {
            "counterparties": [],
            "accounts": [],
            "pos_machines": [],
            "departments": [],
        }

        # Search for counterparties with two-condition logic
        if entity_info.get("counterparties"):
            for counterparty in entity_info["counterparties"]:
                extracted_name = counterparty["name"]
                self.logger.info(
                    f"Searching for counterparty: '{extracted_name}'"
                )

                matches = search_counterparties(extracted_name, limit=2)

                if matches and matches[0].get("code"):
                    # Condition 1: Found in index - get code, name, address
                    for match in matches:
                        match["extracted_name"] = extracted_name
                        match["match_type"] = counterparty["type"]
                        match["extraction_confidence"] = counterparty[
                            "confidence"
                        ]
                        match["search_condition"] = "found_in_index"
                        results["counterparties"].append(match)
                        self.logger.info(
                            f"Condition 1: Found counterparty '{extracted_name}' in index with code: {match['code']}"
                        )
                else:
                    # Condition 2: Not found in index - return extracted name with null code/address
                    not_found_result = {
                        "extracted_name": extracted_name,
                        "name": extracted_name,
                        "code": None,
                        "address": None,
                        "phone": None,
                        "tax_id": None,
                        "match_type": counterparty["type"],
                        "extraction_confidence": counterparty["confidence"],
                        "search_condition": "not_found_in_index",
                        "score": 0.0,
                    }
                    results["counterparties"].append(not_found_result)
                    self.logger.info(
                        f"Condition 2: Counterparty '{extracted_name}' not found in index, using extracted name with null code/address"
                    )

        # Search for accounts
        if entity_info.get("accounts"):
            for account in entity_info["accounts"]:
                matches = search_accounts(
                    account["code"], field_name="name", limit=2
                )
                if matches:
                    for match in matches:
                        match["extracted_code"] = account["code"]
                        match["match_type"] = account["type"]
                        match["extraction_confidence"] = account["confidence"]
                        results["accounts"].append(match)

        # Search for POS machines
        if entity_info.get("pos_machines"):
            for pos in entity_info["pos_machines"]:
                matches = search_pos_machines(
                    pos["code"], field_name="code", limit=2
                )
                if matches:
                    for match in matches:
                        match["extracted_code"] = pos["code"]
                        match["extraction_confidence"] = pos["confidence"]
                        results["pos_machines"].append(match)

        # Search for departments
        if entity_info.get("departments"):
            for dept in entity_info["departments"]:
                matches = search_departments(
                    dept["code"], field_name="code", limit=2
                )
                if matches:
                    for match in matches:
                        match["extracted_code"] = dept["code"]
                        match["extraction_confidence"] = dept["confidence"]
                        results["departments"].append(match)

        # Sort each category by score
        for category in results:
            if results[category]:
                results[category].sort(
                    key=lambda x: x.get("score", 0), reverse=True
                )

        return results

    def match_counterparty_in_db(
        self, name: str, max_results: int = 5
    ) -> List[Dict]:
        """
        Match an extracted counterparty name against the database
        using fuzzy search to find the best match.

        Args:
            name: Extracted counterparty name
            max_results: Maximum number of matches to return

        Returns:
            List of matching counterparties from database
        """
        # First try exact match
        results = search_counterparties(name, limit=max_results)

        return results

    def extract_and_match(
        self, description: str, max_results: int = 2
    ) -> List[Dict]:
        """
        Extract counterparty names from description and match against database

        Args:
            description: Transaction description
            max_results: Maximum number of final results to return

        Returns:
            List of matched counterparties with database info
        """
        # Extract potential counterparties from the description
        extracted = self.extract_counterparties(description)

        if not extracted:
            return []

        # Match each extracted counterparty against the database
        matched_results = []

        for party in extracted:
            matches = self.match_counterparty_in_db(party["name"])

            if matches:
                # Add the best match with original extraction info
                best_match = matches[0]
                best_match["extracted_name"] = party["name"]
                best_match["match_type"] = party["type"]
                best_match["extraction_confidence"] = party["confidence"]
                matched_results.append(best_match)

        # Return top results, sorted by score
        matched_results.sort(key=lambda x: x.get("score", 0), reverse=True)
        return matched_results[:max_results]

    def clean_department_code(self, department_code: str) -> str:
        """
        Clean department code for counterparty search.

        Process:
        1. Split by "-" (if no "-", try "_")
        2. Take the last element
        3. Remove spaces
        4. Apply text replacements (e.g., BRVT -> BARIA)

        Example: "DD.TINH_GO BRVT1" -> split by "_" -> ["DD.TINH", "GO BRVT1"] -> take last -> "GO BRVT1" -> remove space -> "GOBRVT1" -> replace -> "GOBARIA1"

        Args:
            department_code: Raw department code from POS machine

        Returns:
            Cleaned and mapped department code for counterparty search
        """
        if not department_code or not isinstance(department_code, str):
            return department_code

        original_code = department_code.strip()
        self.logger.debug(f"Cleaning department code: '{original_code}'")

        # Step 1: Try splitting by "-" first
        if "-" in original_code:
            parts = original_code.split("-")
        # If no "-", try splitting by "_"
        elif "_" in original_code:
            parts = original_code.split("_")
        # If no separators, use the original code
        else:
            parts = [original_code]

        # Step 2: Take the last element
        last_element = parts[-1].strip() if parts else original_code

        # Step 3: Remove spaces
        no_spaces = re.sub(r"\s+", "", last_element)

        # Step 4: Apply text replacements
        final_code = no_spaces
        for old_text, new_text in self.department_code_replacements.items():
            if old_text in final_code:
                final_code = final_code.replace(old_text, new_text)
                self.logger.debug(
                    f"Applied replacement: '{old_text}' -> '{new_text}'"
                )

        self.logger.debug(
            f"Cleaned department code: '{original_code}' -> '{final_code}'"
        )

        return final_code

    def handle_pos_machine_counterparty_logic(
        self, extracted_pos_machines: List[Dict]
    ) -> Dict[str, any]:
        """
        Implement NEW POS machine counterparty logic with bonus condition:
        1. Get POS machine code and search in pos_machines index
        2. Get "department_code" from POS machine record
        3. Clean department_code (split by "-" or "_", take last element, remove spaces)
        4. Search counterparties by cleaned code using "code" field
        5. BONUS CONDITION: Filter results where name = "KHÁCH LẺ KHÔNG LẤY HOÁ ĐƠN"
        6. Return counterparty code, name, address

        Args:
            extracted_pos_machines: List of POS machines from search_entities

        Returns:
            Dictionary with counterparty info found via POS machine logic
        """
        if not extracted_pos_machines:
            return None

        # Get the best POS machine match (first one, highest score)
        best_pos_match = extracted_pos_machines[0]
        pos_code = best_pos_match.get("extracted_code") or best_pos_match.get(
            "code"
        )

        if not pos_code:
            self.logger.warning("No POS code found in POS machine match")
            return None

        self.logger.info(
            f"Processing NEW POS machine logic for code: {pos_code}"
        )

        # Step 1: Get POS machine details (department_code)
        pos_department_code = best_pos_match.get("department_code")

        if not pos_department_code:
            self.logger.warning(
                f"No department_code found for POS machine {pos_code}"
            )
            return None

        self.logger.info(
            f"POS machine {pos_code} - Raw department_code: '{pos_department_code}'"
        )

        # Step 2: Clean the department code
        cleaned_dept_code = self.clean_department_code(pos_department_code)

        if not cleaned_dept_code:
            self.logger.warning(
                f"Department code cleaning resulted in empty string for: '{pos_department_code}'"
            )
            return None

        self.logger.info(
            f"Cleaned department code: '{pos_department_code}' -> '{cleaned_dept_code}'"
        )

        # Step 3: Search counterparties by cleaned department code
        counterparty_matches = search_exact_counterparties(
            cleaned_dept_code, field_name="code", limit=5
        )

        if not counterparty_matches:
            self.logger.info(
                f"No counterparties found with code: '{cleaned_dept_code}'"
            )
            return None

        self.logger.info(
            f"Found {len(counterparty_matches)} counterparties with code '{cleaned_dept_code}'"
        )

        # Step 4: Apply BONUS CONDITION - Filter by name = "KHÁCH LẺ KHÔNG LẤY HOÁ ĐƠN"
        bonus_condition_name = "KHÁCH LẺ KHÔNG LẤY HOÁ ĐƠN"
        filtered_matches = []
        
        for match in counterparty_matches:
            match_name = match.get("name", "").strip()
            if match_name == bonus_condition_name:
                filtered_matches.append(match)
                self.logger.info(
                    f"POS BONUS CONDITION: Found counterparty with code '{match['code']}' and name '{match_name}'"
                )
        
        # Step 5: Use filtered results if available, otherwise fallback to original results
        if filtered_matches:
            self.logger.info(
                f"POS BONUS CONDITION: Using {len(filtered_matches)} filtered matches (name = '{bonus_condition_name}')"
            )
            best_counterparty = filtered_matches[0]  # Use first filtered match
            condition_applied = "pos_machine_counterparty_logic_with_bonus_condition"
        else:
            self.logger.info(
                f"POS BONUS CONDITION: No matches found with name '{bonus_condition_name}', using original results"
            )
            best_counterparty = counterparty_matches[0]  # Use original best match
            condition_applied = "pos_machine_counterparty_logic_original"

        # Step 6: Return the best counterparty match
        result = {
            "code": best_counterparty["code"],
            "name": self.clean_counterparty_name(best_counterparty["name"]),
            "address": best_counterparty.get("address") or "",
            "phone": best_counterparty.get("phone") or "",
            "tax_id": best_counterparty.get("tax_id") or "",
            "source": "pos_machine_lookup",
            "condition_applied": condition_applied,
            "pos_code": pos_code,
            "pos_department_code": pos_department_code,
            "cleaned_department_code": cleaned_dept_code,
            "bonus_condition_applied": len(filtered_matches) > 0,
            "bonus_condition_name": bonus_condition_name,
        }

        self.logger.info(
            f"NEW POS machine logic result: Found counterparty '{result['name']}' (code: {result['code']}) "
            f"for POS {pos_code} using cleaned department code '{cleaned_dept_code}' "
            f"(bonus condition applied: {result['bonus_condition_applied']})"
        )

        return result

    def handle_counterparty_with_all_logic(
        self, extracted_entities: Dict[str, List[Dict]]
    ) -> Dict[str, any]:
        """
        Unified counterparty handling that includes:
        1. POS machine logic (if POS machines detected)
        2. Two-condition logic for regular counterparties
        3. Fallback to default

        Args:
            extracted_entities: All extracted entities from extract_and_match_all

        Returns:
            Dictionary with final counterparty info
        """
        # Priority 1: Check for POS machine logic
        extracted_pos_machines = extracted_entities.get("pos_machines", [])
        if extracted_pos_machines:
            self.logger.info("Applying POS machine counterparty logic")
            pos_result = self.handle_pos_machine_counterparty_logic(
                extracted_pos_machines
            )
            if pos_result:
                return pos_result
            else:
                self.logger.info(
                    "POS machine logic failed, falling back to regular counterparty logic"
                )

        # Priority 2: Regular counterparty two-condition logic
        extracted_counterparties = extracted_entities.get("counterparties", [])
        if extracted_counterparties:
            self.logger.info(
                "Applying regular counterparty two-condition logic"
            )
            return self.handle_counterparty_two_conditions(
                extracted_counterparties
            )

        # Priority 3: Fallback to default
        self.logger.info(
            "No counterparties or POS machines found, using default"
        )
        return {
            "code": "KL",
            "name": "Khách Lẻ Không Lấy Hóa Đơn",
            "address": "",
            "source": "default",
            "condition_applied": "no_extraction",
        }

    def handle_counterparty_two_conditions(
        self, extracted_counterparties: List[Dict]
    ) -> Dict[str, any]:
        """
        Implement the explicit two-condition business logic for counterparty handling.
        Now includes counterparty name cleaning for better results.

        Condition 1: If counterparty found in index, get code, name, address
        Condition 2: If not found, return extracted name with null code/address

        Args:
            extracted_counterparties: List of counterparties from search_entities

        Returns:
            Dictionary with counterparty info based on business conditions
        """
        if not extracted_counterparties:
            return {
                "code": "KL",  # Default customer code
                "name": "Khách Lẻ Không Lấy Hóa Đơn",
                "address": "",
                "source": "default",
                "condition_applied": "no_extraction",
            }

        # Get the best match (first one, highest score)
        best_match = extracted_counterparties[0]

        if best_match.get(
            "search_condition"
        ) == "found_in_index" and best_match.get("code"):
            # Condition 1: Found in index
            # Clean the name from database as well for consistency
            db_name = best_match.get("name", "")
            cleaned_db_name = (
                self.clean_counterparty_name(db_name) if db_name else db_name
            )

            result = {
                "code": best_match["code"],
                "name": cleaned_db_name
                or db_name,  # Use cleaned name if available
                "address": best_match.get("address") or "",
                "phone": best_match.get("phone") or "",
                "tax_id": best_match.get("tax_id") or "",
                "source": "database",
                "condition_applied": "found_in_index",
                "extraction_confidence": best_match.get(
                    "extraction_confidence", 0
                ),
                "match_type": best_match.get("match_type", "unknown"),
            }
            self.logger.info(
                f"Applied Condition 1: Found '{result['name']}' with code '{result['code']}' in database"
            )
            return result
        else:
            # Condition 2: Not found in index, use extracted name with null code/address
            extracted_name = best_match.get("extracted_name") or best_match.get(
                "name"
            )
            # The extracted name should already be cleaned during extraction

            result = {
                "code": None,
                "name": extracted_name,
                "address": None,
                "phone": None,
                "tax_id": None,
                "source": "extracted",
                "condition_applied": "not_found_in_index",
                "extraction_confidence": best_match.get(
                    "extraction_confidence", 0
                ),
                "match_type": best_match.get("match_type", "unknown"),
            }
            self.logger.info(
                f"Applied Condition 2: Using extracted name '{result['name']}' with null code/address"
            )
            return result

    def extract_and_match_all(self, description: str) -> Dict[str, List[Dict]]:
        """
        Extract all entities from a description and match against appropriate databases.
        This is the main entry point that should be used instead of multiple separate searches.

        Args:
            description: Transaction description

        Returns:
            Dictionary with matched entities from all relevant categories
        """
        # Extract all entity types from the description
        entity_info = self.extract_entity_info(description)

        # Search for matches in the appropriate indexes
        matched_entities = self.search_entities(entity_info)

        return matched_entities
