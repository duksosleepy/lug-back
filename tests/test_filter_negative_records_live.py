"""Test filter_negative_records with actual data from CRM_ONLINE_DATA_URL."""

import json
from datetime import datetime

import httpx

from src.crm.flow import (
    AUTH_CREDENTIALS,
    AUTH_URL,
    BATCH_TIMEOUT,
    ONLINE_DATA_URL,
    filter_negative_records,
)


def get_access_token() -> str:
    """Authenticate with the API and return the access token."""
    with httpx.Client(timeout=BATCH_TIMEOUT) as client:
        response = client.post(AUTH_URL, json=AUTH_CREDENTIALS)
        response.raise_for_status()
        return response.json()["data"]["access_token"]


def fetch_online_data_for_date(token: str, target_date: str) -> list[dict]:
    """Fetch online data for a specific date.

    Args:
        token: API access token
        target_date: Date in YYYY-MM-DD format (e.g., "2026-01-05")

    Returns:
        List of records for that date
    """
    # Parse the target date
    target_dt = datetime.strptime(target_date, "%Y-%m-%d")
    date_gte = target_dt.strftime("%Y-%m-%dT00:00:00")
    # Next day for the less-than filter
    next_day = target_dt.replace(day=target_dt.day + 1)
    date_lt = next_day.strftime("%Y-%m-%dT00:00:00")

    date_filter = {
        "_and": [
            {"Ngay_Ct": {"_gte": date_gte}},
            {"Ngay_Ct": {"_lt": date_lt}},
        ]
    }

    headers = {"authorization": f"Bearer {token}"}
    params = {
        "limit": -1,
        "filter": json.dumps(date_filter),
    }

    print(f"Fetching online data for {target_date}")
    print(f"Filter: {date_filter}")

    with httpx.Client(timeout=BATCH_TIMEOUT) as client:
        response = client.get(ONLINE_DATA_URL, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()["data"]
        print(f"Retrieved {len(data)} records")
        return data


def test_actual_data_january_5_2026():
    """Test with actual data from CRM_ONLINE_DATA_URL for January 5, 2026."""
    # Get access token
    token = get_access_token()
    print(f"Got access token: {token[:20]}...")

    # Fetch data for January 5, 2026
    online_data = fetch_online_data_for_date(token, "2026-01-05")

    print(f"\nTotal records fetched: {len(online_data)}")

    # Count TLO records
    tlo_records = [r for r in online_data if r.get("Ma_Ct") == "TLO"]
    print(f"TLO records: {len(tlo_records)}")

    # Show TLO records details
    print("\nTLO records details:")
    for i, record in enumerate(tlo_records, 1):
        print(
            f"  {i}. So_Ct={record.get('So_Ct')}, "
            f"Doanh_Thu={record.get('Doanh_Thu')}, "
            f"So_Luong={record.get('So_Luong')}, "
            f"Ten_Khach_Hang={record.get('Ten_Khach_Hang')}"
        )

    # Apply filter_negative_records
    negative_records = filter_negative_records(online_data)
    print(f"\nNegative records found: {len(negative_records)}")

    # Show negative records details
    print("\nNegative records details:")
    for i, record in enumerate(negative_records, 1):
        print(
            f"  {i}. Ma_Ct={record.get('Ma_Ct')}, "
            f"So_Ct={record.get('So_Ct')}, "
            f"Doanh_Thu={record.get('Doanh_Thu')}, "
            f"So_Luong={record.get('So_Luong')}"
        )

    # Check if TLO records with negative values are in the result
    tlo_negative = [r for r in negative_records if r.get("Ma_Ct") == "TLO"]
    print(f"\nTLO records in negative results: {len(tlo_negative)}")

    # The assertion - user expects 9 negative records
    # We'll print the actual count and let the user verify
    print(f"\n{'='*50}")
    print(f"RESULT: Found {len(negative_records)} negative records")
    print(f"Expected: 9 negative records")
    print(f"Match: {len(negative_records) == 9}")
    print(f"{'='*50}")

    # Assert that we found 9 negative records as expected
    assert len(negative_records) == 9, (
        f"Expected 9 negative records, got {len(negative_records)}"
    )


if __name__ == "__main__":
    test_actual_data_january_5_2026()
