# Data Flow: Detail Structure Consistency

This document traces the exact data structure of the `detail` array through the entire pipeline.

## Example Input Data

Two order items with the **same** `maDonHang`:

```python
# Item 1
{
    "Ma_Don_Hang": "BL/LUG_VHM/20260330/0002",
    "Ma_Hang_Old": "MP030_28_PURPLE-SHINY",
    "Ten_Hang": "VALY NHUA 8 BANH HIEU HOLDALL",
    "Imei": "DDLHQL21752503000416",
    "So_Luong": 1,
    "Doanh_Thu": 2026000,
    # ... other master fields
}

# Item 2
{
    "Ma_Don_Hang": "BL/LUG_VHM/20260330/0002",  # SAME maDonHang
    "Ma_Hang_Old": "BL118_PURPLE",
    "Ten_Hang": "BALO SANTABARBARA",
    "Imei": "DBLSBP21392511000013",
    "So_Luong": 1,
    "Doanh_Thu": 0,
    # ... other master fields
}
```

---

## Step 1: `transform_data()` in lug-back/src/crm/flow.py

**Output** (what gets sent to Restate BATCH_URL):

```python
[
    {
        "url": "http://restate-server:9080/...",
        "data": {
            "apikey": "...",
            "data": [
                {
                    "master": {
                        "ngayCT": "2026-03-30",
                        "maCT": "BL",
                        "soCT": "0002",
                        "maBoPhan": "LUG_VHM",
                        "maDonHang": "BL/LUG_VHM/20260330/0002",
                        "tenKhachHang": "CHI THU",
                        "soDienThoai": "0903059876",
                        "tinhThanh": "",
                        "quanHuyen": "",
                        "phuongXa": "",
                        "diaChi": "Q10"
                    },
                    "detail": [
                        {
                            "maHang": "MP030_28_PURPLE-SHINY",
                            "tenHang": "VALY NHUA 8 BANH HIEU HOLDALL",
                            "imei": "DDLHQL21752503000416",
                            "soLuong": 1,
                            "doanhThu": 2026000
                        },
                        {
                            "maHang": "BL118_PURPLE",
                            "tenHang": "BALO SANTABARBARA",
                            "imei": "DBLSBP21392511000013",
                            "soLuong": 1,
                            "doanhThu": 0
                        }
                    ]
                }
            ]
        }
    }
]
```

**Key Points:**
- ✅ `detail` is an **array** with **2 items** (combined)
- ✅ Each detail item has exactly these keys: `maHang`, `tenHang`, `imei`, `soLuong`, `doanhThu`
- ✅ Only **1 request** in the list (not 2 separate requests)

---

## Step 2: Restate Server Receives (main-sqlite.py `submit_batch`)

**Input** to `submit_batch(ctx, requests)`:

```python
requests = [
    {
        "url": "http://final-crm-server:8000/...",
        "data": {
            "apikey": "...",
            "data": [order_with_combined_details]
        }
    }
]
```

**Processing:**
- Generates `task_id` by hashing the entire request
- Creates `HttpRequest(url=..., data=..., task_id=...)`
- Calls `execute_request` for each item

---

## Step 3: Restate Sends to Final Server (main-sqlite.py `execute_request`)

**What Restate sends to `TARGET_URL`:**

```python
# This is request.data
{
    "apikey": "...",
    "data": [
        {
            "master": {...},
            "detail": [
                {
                    "maHang": "MP030_28_PURPLE-SHINY",
                    "tenHang": "VALY NHUA 8 BANH HIEU HOLDALL",
                    "imei": "DDLHQL21752503000416",
                    "soLuong": 1,
                    "doanhThu": 2026000
                },
                {
                    "maHang": "BL118_PURPLE",
                    "tenHang": "BALO SANTABARBARA",
                    "imei": "DBLSBP21392511000013",
                    "soLuong": 1,
                    "doanhThu": 0
                }
            ]
        }
    ]
}
```

**Code:**
```python
response = await client.post(request.url, json=request.data, timeout=30.0)
# Sends request.data to request.url
```

---

## Step 4: Final CRM Server Receives

**POST body received by final server:**

```json
{
  "apikey": "...",
  "data": [
    {
      "master": {
        "ngayCT": "2026-03-30",
        "maCT": "BL",
        "soCT": "0002",
        "maBoPhan": "LUG_VHM",
        "maDonHang": "BL/LUG_VHM/20260330/0002",
        "tenKhachHang": "CHI THU",
        "soDienThoai": "0903059876",
        "tinhThanh": "",
        "quanHuyen": "",
        "phuongXa": "",
        "diaChi": "Q10"
      },
      "detail": [
        {
          "maHang": "MP030_28_PURPLE-SHINY",
          "tenHang": "VALY NHUA 8 BANH HIEU HOLDALL",
          "imei": "DDLHQL21752503000416",
          "soLuong": 1,
          "doanhThu": 2026000
        },
        {
          "maHang": "BL118_PURPLE",
          "tenHang": "BALO SANTABARBARA",
          "imei": "DBLSBP21392511000013",
          "soLuong": 1,
          "doanhThu": 0
        }
      ]
    }
  ]
}
```

---

## Step 5: Restate Database Operations (main-sqlite.py)

When processing the response, Restate extracts details:

```python
# In insert_order_thread / update_order_thread
details_list = request.data["data"][0].get("detail", [])
# Returns: [detail_item_1, detail_item_2]

for details in details_list:
    # Each detail item is inserted/updated separately
    conn.execute(
        "INSERT INTO orders ...",
        (
            ...,
            details.get("maHang"),      # Product code
            details.get("tenHang"),     # Product name
            details.get("imei"),        # IMEI
            details.get("soLuong"),     # Quantity
            details.get("doanhThu"),    # Revenue
            ...
        )
    )
```

**Result in Database:**
- 2 separate rows in `orders` table
- Same `order_id` (task_id)
- Different `product_code`, `imei`, `quantity`, `revenue`

---

## Summary: Detail Structure Consistency

| Stage | Location | Detail Structure |
|-------|----------|------------------|
| **1. transform_data()** | lug-back/src/crm/flow.py | ✅ Array with 2 items |
| **2. submit_batch()** | lug-back sends to Restate | ✅ Array with 2 items |
| **3. execute_request()** | Restate sends to final server | ✅ Array with 2 items |
| **4. Final CRM Server** | Receives POST request | ✅ Array with 2 items |
| **5. Database Insert** | Restate processes response | ✅ Loops through 2 items |

**All stages maintain the exact same detail structure!** ✅

---

## Before vs After

### BEFORE (Bug)
```python
# 2 separate requests sent to Restate
[
    {"url": "...", "data": {"apikey": "...", "data": [{"master": {...}, "detail": [item1]}]}},
    {"url": "...", "data": {"apikey": "...", "data": [{"master": {...}, "detail": [item2]}]}}
]
```

### AFTER (Fixed)
```python
# 1 combined request sent to Restate
[
    {"url": "...", "data": {"apikey": "...", "data": [{"master": {...}, "detail": [item1, item2]}]}}
]
```

The final server receives **one order** with **two detail items**, exactly as required! ✅
