    def _load_bank_info(self):
        """Load bank information from banks.json"""
        try:
            banks_file = Path(__file__).parent / "banks.json"
            if banks_file.exists():
                with open(banks_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self.banks_data = data.get("banks", [])
                self.logger.info(
                    f"Loaded {len(self.banks_data)} banks from banks.json"
                )
            else:
                self.logger.warning(
                    "banks.json not found, using empty banks data"
                )
                self.banks_data = []
        except Exception as e:
            self.logger.error(f"Error loading bank information: {e}")
            self.banks_data = []

    def extract_bank_from_filename(self, filename: str) -> str:
        """Extract bank name from filename (e.g., 'BIDV' from 'BIDV 3840.xlsx')

        Args:
            filename: The filename to extract bank name from

        Returns:
            Bank short name if found, empty string otherwise
        """
        if not filename:
            return ""

        # Extract bank name from filename (e.g., BIDV from 'BIDV 3840.xlsx')
        filename_parts = Path(filename).stem.split()
        if not filename_parts:
            return ""

        # Take the first part as potential bank name
        potential_bank_name = filename_parts[0].upper()
        self.logger.info(
            f"Attempting to extract bank from filename: '{filename}', potential name: '{potential_bank_name}'"
        )

        # Check if this matches any known bank code
        for bank in self.banks_data:
            if bank.get("code", "").upper() == potential_bank_name:
                self.logger.info(f"Successfully identified bank: {bank.get('shortName', potential_bank_name)}")
                return potential_bank_name

        # If no exact match, check for partial matches
        for bank in self.banks_data:
            bank_code = bank.get("code", "").upper()
            if bank_code and potential_bank_name.startswith(bank_code):
                self.logger.info(f"Partial match found for bank: {bank.get('shortName', bank_code)}")
                return bank_code

        self.logger.warning(f"No bank found for filename pattern: {potential_bank_name}")
        return ""

    def get_bank_info_by_name(self, bank_name: str) -> Optional[Dict]:
        """Get bank information by bank name/code

        Args:
            bank_name: Bank name or code to search for

        Returns:
            Bank information dictionary if found, None otherwise
        """
        if not bank_name or not self.banks_data:
            return None

        bank_name_upper = bank_name.upper()

        # First try exact code match
        for bank in self.banks_data:
            if bank.get("code", "").upper() == bank_name_upper:
                self.logger.info(f"Found bank by code: {bank.get('shortName', bank_name)}")
                return bank

        # Then try short name match
        for bank in self.banks_data:
            if bank.get("shortName", "").upper() == bank_name_upper:
                self.logger.info(f"Found bank by shortName: {bank.get('shortName', bank_name)}")
                return bank

        # Finally try name contains
        for bank in self.banks_data:
            if bank_name_upper in bank.get("name", "").upper():
                self.logger.info(f"Found bank by name pattern: {bank.get('shortName', bank_name)}")
                return bank

        self.logger.warning(f"Bank not found: {bank_name}")
        return None

    def get_bank_config_for_bank(self, bank_code: str):
        """Get bank-specific configuration for processing"""
        try:
            from src.accounting.bank_configs import get_bank_config
            return get_bank_config(bank_code)
        except ImportError:
            self.logger.warning("Bank configs not available, using default BIDV config")
            from src.accounting.bank_statement_reader import BankStatementConfig
            return BankStatementConfig()  # Default BIDV config
