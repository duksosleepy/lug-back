            # **PRIORITY 0 (HIGHEST): MBB Phone Number Detection for Online Transactions**
            # Check if current bank is MBB and transaction description contains phone number
            if (self.current_bank_name and
                self.current_bank_name.upper() == "MBB" and
                self._detect_phone_number_in_description(transaction.description)):

                # Extract the phone number for logging
                phone_number = self._extract_phone_number_from_description(transaction.description)

                self.logger.info(
                    f"MBB Online Transaction Detected: Bank={self.current_bank_name}, "
                    f"Phone={phone_number}, Description={transaction.description}"
                )

                # Apply MBB online transaction business logic
                counterparty_info = {
                    "code": "KLONLINE",
                    "name": "KHÁCH LẺ KHÔNG LẤY HÓA ĐƠN (ONLINE)",
                    "address": "4 Grand Canal Square, Grand Canal Harbour, Dublin 2, Ireland",
                    "source": "mbb_phone_number_detection",
                    "condition_applied": "mbb_online_phone_detected",
                    "phone": phone_number,
                    "tax_id": "",
                }

                # Modify description for MBB phone transactions
                # Extract PO number if available
                po_match = re.search(r"PO\s*[:-]?\s*([A-Z0-9]+)", transaction.description, re.IGNORECASE)
                po_number = po_match.group(1) if po_match else ""

                # Set description
                if po_number:
                    description = f"Thu tiền KH online thanh toán cho PO: {po_number}"
                else:
                    description = f"Thu tiền KH online thanh toán"

                self.logger.info(
                    f"Applied MBB online counterparty logic: Code={counterparty_info['code']}, "
                    f"Name={counterparty_info['name']}, Phone={phone_number}"
                )
            else:
                # Only apply existing counterparty logic if MBB phone number detection didn't trigger

                # Priority 1: If extracted object is an account
                if extracted_accounts:
                    self.logger.info(
                        "Detected account object, using Sáng Tâm company info"
                    )
                    counterparty_info = {
                        "code": "31754",
                        "name": "Công Ty TNHH Sáng Tâm",
                        "address": "32-34 Đường 74, Phường 10, Quận 6, Tp. Hồ Chí Minh",
                        "source": "hardcoded_account_rule",
                        "condition_applied": "account_detected",
                        "phone": "",
                        "tax_id": "",
                    }
                # Priority 2: If extracted object is POS machine or counterparty
                elif extracted_pos_machines or extracted_counterparties:
                    if extracted_pos_machines:
                        # Use POS machine counterparty logic (gets counterparty from department_code)
                        self.logger.info(
                            "Detected POS machine object, applying POS machine counterparty logic"
                        )

                        # Determine current address for POS machine logic
                        current_address = None

                        # Option 1: Use bank address from current_bank_info
                        if self.current_bank_info and self.current_bank_info.get(
                            "address"
                        ):
                            current_address = self.current_bank_info["address"]
                            self.logger.info(
                                f"Using bank address as current_address: {current_address}"
                            )

                        # Call the enhanced POS machine logic with address parameter
                        counterparty_info = self.counterparty_extractor.handle_pos_machine_counterparty_logic(
                            extracted_pos_machines, current_address=current_address
                        )

                        # If POS machine logic failed, fall back to default
                        if not counterparty_info:
                            self.logger.warning(
                                "POS machine counterparty logic failed, using default counterparty"
                            )
                            counterparty_info = {
                                "code": "KL",
                                "name": "Khách Lẻ Không Lấy Hóa Đơn",
                                "address": "",
                                "source": "default_after_pos_failure",
                                "condition_applied": "pos_machine_failed",
                                "phone": "",
                                "tax_id": "",
                            }
                    else:
                        # Use counterparty info directly
                        cp_info = extracted_counterparties[
                            0
                        ]  # Take first/best match
                        self.logger.info(
                            f"Detected counterparty object, using extracted info: {cp_info.get('name', cp_info.get('code', ''))}"
                        )
                        counterparty_info = {
                            "code": cp_info.get("code"),
                            "name": cp_info.get(
                                "name", cp_info.get("extracted_name", "")
                            ),
                            "address": cp_info.get("address", ""),
                            "source": "counterparty_extracted",
                            "condition_applied": "counterparty_detected",
                            "phone": cp_info.get("phone", ""),
                            "tax_id": cp_info.get("tax_id", ""),
                        }
                # Priority 3: Fall back to existing two-condition logic
                else:
                    self.logger.info(
                        "No specific object detected, using existing two-condition logic"
                    )
                    counterparty_info = self.counterparty_extractor.handle_counterparty_two_conditions(
                        extracted_counterparties
                    )