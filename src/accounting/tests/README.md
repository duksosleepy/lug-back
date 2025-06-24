# POS Machine Logic Tests

This directory contains comprehensive tests for the POS machine business logic implementation.

## Business Logic Being Tested

The POS machine logic implements this specific business flow:

### NEW LOGIC (Updated):
1. **Extract POS code** from transaction description (e.g., "TT POS 14100333")
2. **Search POS machine** in `pos_machines` index using the extracted code
3. **Get department_code** from the POS machine record
4. **Clean department_code** by splitting by "-" (or "_"), taking last element, removing spaces
   - Example: "DD.TINH_GO BRVT1" → "GOBRVT1"
5. **Search counterparties** using the cleaned code in `code` field (not `address`)
6. **Return best match** with `counterparty.code`, `counterparty.name`, `counterparty.address`

### OLD LOGIC (Previous):
1. ~~Extract POS code → Search POS machine → Get department_code + address~~
2. ~~Search counterparties by address~~
3. ~~Filter counterparties where code ≠ department_code~~
4. ~~Return filtered result~~

## Test Files

### 1. `test_new_pos_logic.py` 🔥 **NEW LOGIC TEST**
- **Purpose**: Tests the NEW department code cleaning logic
- **What it tests**: Department code cleaning, direct counterparty search by code
- **When to use**: To validate the updated business logic
- **Runtime**: ~15 seconds

### 2. `test_minimum_records.py` 🔥 **QUICK TEST**
- **Purpose**: Validates the minimum records requirement for POS logic
- **What it tests**: Why limit=10 is needed (not limit=1) when searching counterparties
- **When to use**: Quick validation of the business logic requirement (OLD LOGIC)
- **Runtime**: ~10 seconds

### 3. `test_pos_focused.py` 📝 **OLD LOGIC**
- **Purpose**: Step-by-step validation of the exact business logic
- **What it tests**: Each step of the POS machine logic individually
- **When to use**: First comprehensive test to run to verify core business logic
- **Runtime**: ~30 seconds

### 3. `test_pos_machine_logic.py`
- **Purpose**: Comprehensive test suite covering all aspects
- **What it tests**: Edge cases, performance, integration, error handling
- **When to use**: After focused test passes, for thorough validation
- **Runtime**: ~2-3 minutes

### 3. `test_two_condition_logic.py`
- **Purpose**: Tests the two-condition counterparty logic
- **What it tests**: Fallback logic when POS machine logic fails
- **When to use**: To verify counterparty handling works correctly
- **Runtime**: ~1 minute

### 4. `run_pos_tests.py`
- **Purpose**: Interactive test runner
- **What it does**: Easy way to run any or all tests
- **When to use**: When you want guided test execution
- **Runtime**: Variable

## How to Run Tests

### Quick Start (Recommended)
```bash
cd /home/khoi/code/lug-back

# Test NEW logic with department code cleaning
python src/accounting/tests/test_new_pos_logic.py

# Test OLD logic for comparison (optional)
python src/accounting/tests/test_pos_focused.py
```

### Using the Test Runner (Interactive)
```bash
cd /home/khoi/code/lug-back
python src/accounting/tests/run_pos_tests.py
```

### Run All Tests
```bash
cd /home/khoi/code/lug-back

# Run focused test first
python src/accounting/tests/test_pos_focused.py

# Run comprehensive test suite
python src/accounting/tests/test_pos_machine_logic.py

# Run two-condition logic test
python src/accounting/tests/test_two_condition_logic.py
```

### Individual Test Commands
```bash
cd /home/khoi/code/lug-back

# Test individual steps of POS logic
python src/accounting/tests/test_pos_focused.py

# Test with real data and edge cases
python src/accounting/tests/test_pos_machine_logic.py

# Test counterparty fallback logic
python src/accounting/tests/test_two_condition_logic.py
```

## Expected Test Results

### ✅ Successful Output Examples:
```
STEP 1 SUCCESS: Extracted POS code = '14100333'
STEP 2 SUCCESS: Found POS machine in index
STEP 3 SUCCESS: Got department_code = 'CN01', address = 'AEON MALL BINH DUONG'
STEP 4 SUCCESS: Found 3 counterparties with address 'AEON MALL BINH DUONG'
STEP 5 SUCCESS: Found 2 valid counterparties
STEP 6 SUCCESS: Final result
   Final Code: KH001
   Final Name: Cong Ty Abc Trading
   Final Address: AEON MALL BINH DUONG
🎉 ALL STEPS COMPLETED SUCCESSFULLY!
```

### ❌ Failure Scenarios:
- **Step 1 Fails**: POS code pattern not recognized
- **Step 2 Fails**: POS code not found in database
- **Step 3 Fails**: POS machine missing department_code or address
- **Step 4 Fails**: No counterparties found with POS machine address
- **Step 5 Fails**: All counterparties filtered out (same code as department)

## Test Data Requirements

The tests expect your database to have:

### POS Machines (`pos_machines` index)
- Records with `code`, `department_code`, `address` fields
- POS codes like "14100333", "14100414", etc.

### Counterparties (`counterparties` index)
- Records with `code`, `name`, `address` fields
- Some counterparties sharing addresses with POS machines
- Some counterparties with different codes than POS department_codes

### Sample Test Data
If tests fail due to missing data, you may need:
```sql
-- Example POS machine
INSERT INTO pos_machines (code, department_code, address, name)
VALUES ('14100333', 'CN01', 'AEON MALL BINH DUONG', 'POS Terminal 333');

-- Example counterparty (different code than CN01)
INSERT INTO counterparties (code, name, address)
VALUES ('KH001', 'Cong Ty ABC Trading', 'AEON MALL BINH DUONG');
```

## Troubleshooting

### Common Issues:

1. **Import Errors**
   ```bash
   # Make sure you're in the right directory
   cd /home/khoi/code/lug-back

   # Check Python path
   python -c "import sys; print(sys.path)"
   ```

2. **Database Connection Issues**
   ```bash
   # Check if fast_search indexes exist
   ls -la src/accounting/index/
   ```

3. **No Test Data**
   ```bash
   # Run data import if needed
   python src/accounting/import_data.py
   ```

4. **Permission Issues**
   ```bash
   # Make test files executable
   chmod +x src/accounting/tests/*.py
   ```

## Test Output Interpretation

### 🎯 **Focus on Step-by-Step Test First**
The `test_pos_focused.py` test shows exactly which step fails:
- If all steps pass → Your business logic is implemented correctly
- If specific step fails → Check that specific part of the implementation

### 📊 **Performance Benchmarks**
The comprehensive test includes performance metrics:
- Normal: <50ms per operation
- Good: <20ms per operation
- Excellent: <10ms per operation

### 🔍 **Error Patterns**
- **High extraction failures**: Check POS code patterns in `counterparty_extractor.py`
- **High database lookup failures**: Check index integrity
- **High filtering failures**: Verify department_code logic

## Integration Test

After all unit tests pass, test with real data:

```bash
cd /home/khoi/code/lug-back
python src/accounting/api.py

# Upload a real BIDV file with POS transactions
# Check logs for "POS machine logic" messages
```

## Next Steps

1. **Run focused test first**: `python src/accounting/tests/test_pos_focused.py`
2. **If it passes**: Your business logic is working correctly ✅
3. **If it fails**: Check the specific step that failed and fix the implementation
4. **After fixing**: Run comprehensive test suite for full validation

## Questions or Issues?

If tests fail or you see unexpected behavior:
1. Check the specific error messages in test output
2. Verify your database has the required test data
3. Review the business logic implementation in:
   - `src/accounting/counterparty_extractor.py` (POS machine logic)
   - `src/accounting/fast_search.py` (database search functions)
   - `src/accounting/integrated_bank_processor.py` (integration logic)
