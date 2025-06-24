import json

# Load original JSON (replace this with your actual JSON string or file)
with open("bank_raw.json", "r", encoding="utf-8") as f:
    original_data = json.load(f)

# Extract only the necessary fields
simplified_data = []
for bank in original_data.get("data", []):
    simplified_bank = {
        "id": bank.get("id"),
        "name": bank.get("name"),
        "code": bank.get("code"),
        "bin": bank.get("bin"),
        "shortName": bank.get("shortName"),
        "swift_code": bank.get("swift_code"),
    }
    simplified_data.append(simplified_bank)

# Wrap in a new structure if needed
output = {"banks": simplified_data}

# Save the simplified JSON
with open("banks.json", "w", encoding="utf-8") as f:
    json.dump(output, f, ensure_ascii=False, indent=2)

print("Simplified JSON saved to simplified_banks.json")
