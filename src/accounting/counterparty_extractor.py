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

from tantivy import Filter, TextAnalyzerBuilder, Tokenizer

from src.accounting.fast_search import (
    search_accounts,
    search_counterparties,
    search_departments,
    search_pos_machines,
)
from src.util.logging import get_logger

logger = get_logger(__name__)

# Create custom analyzer with ASCII folding for Vietnamese text
vietnamese_analyzer = (
    TextAnalyzerBuilder(Tokenizer.simple())
    .filter(Filter.ascii_fold())
    .filter(Filter.lowercase())
    .build()
)


class CounterpartyExtractor:
    """
    Enhanced extractor that detects and extracts entities from bank transaction descriptions
    using pattern matching and fuzzy search, searching directly in the appropriate index.
    """

    def __init__(self, db_path: str = "banking_enterprise.db"):
        """Initialize the extractor"""
        self.db_path = db_path
        self.logger = logger

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
                    # Keep original capitalization for person names
                    name = name.strip()

                    # Validate that this is a person name
                    if self._is_valid_person_name(name):
                        counterparties.append(
                            {
                                "name": name,
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
                    # Clean up the extracted name
                    cleaned_name = self._clean_company_name(name)
                    if (
                        cleaned_name and len(cleaned_name) > 5
                    ):  # Minimum length to be considered valid
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
                    cleaned_name = self._clean_company_name(potential_name)

                    if cleaned_name and len(cleaned_name) > 5:
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
        Clean and normalize a company name

        Args:
            name: Raw company name extracted from text

        Returns:
            Cleaned company name
        """
        # Remove any leading/trailing whitespace
        cleaned = name.strip()

        # Remove stop words and common prefixes/suffixes
        for word in self.stopwords:
            cleaned = re.sub(r"\b" + word + r"\b", "", cleaned)

        # Replace multiple spaces with a single space
        cleaned = re.sub(r"\s+", " ", cleaned).strip()

        return cleaned

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

        # Check for common Vietnamese family names, but don't require it
        # as we might encounter less common surnames
        common_surnames = [
            "Nguyen",
            "Tran",
            "Le",
            "Pham",
            "Hoang",
            "Huynh",
            "Phan",
            "Vu",
            "Vo",
            "Bui",
            "Do",
            "Ho",
            "Ngo",
            "Duong",
            "Ly",
            "Dang",
            "Truong",
            "Dinh",
            "Mai",
            "Trinh",
            "Ha",
        ]

        # If first word is a common surname, increase confidence
        has_common_surname = any(
            parts[0] == surname for surname in common_surnames
        )

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

        # Check for common Vietnamese syllables in names
        vietnamese_syllables = [
            "Thi",
            "Van",
            "Minh",
            "Thanh",
            "Tuan",
            "Anh",
            "Duc",
            "Thao",
            "Hung",
        ]
        has_common_syllable = any(
            syllable in parts for syllable in vietnamese_syllables
        )

        # If it has a common surname or common syllable, it's more likely to be a valid name
        return True

    def search_entities(
        self, entity_info: Dict[str, List[Dict]]
    ) -> Dict[str, List[Dict]]:
        """
        Search for all detected entities in their respective indexes.
        This is a key enhancement that eliminates redundant searches.

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

        # Search for counterparties
        if entity_info.get("counterparties"):
            for counterparty in entity_info["counterparties"]:
                matches = search_counterparties(counterparty["name"], limit=2)
                if matches:
                    for match in matches:
                        match["extracted_name"] = counterparty["name"]
                        match["match_type"] = counterparty["type"]
                        match["extraction_confidence"] = counterparty[
                            "confidence"
                        ]
                        results["counterparties"].append(match)

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
