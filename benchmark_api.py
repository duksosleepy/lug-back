import httpx
import time
import json

# API endpoint
url = "https://testlug.baohanhsuachua.com/v1/importdonhang/import"

# Request payload
payload = {
    "master": {
        "ngayCT": "2025-10-09",
        "maCT": "HDO",
        "soCT": "0030",
        "maBoPhan": "TMDT_SHOPEE_LUG",
        "maDonHang": "251009KQK5B3H9",
        "tenKhachHang": "NGÔ THÀNH TÍN",
        "soDienThoai": "0328409066",
        "tinhThanh": "TP. HỒ CHÍ MINH",
        "quanHuyen": "Quận 1",
        "phuongXa": "Phường 1",
        "diaChi": "KL"
    },
    "detail": [
        {
            "maHang": "MHL010_20_BLACK",
            "tenHang": "VALY NHỰA 4 BÁNH HIỆU ABER",
            "imei": "DDLABE30082508000070",
            "soLuong": 1,
            "doanhThu": 489319.0
        }
    ]
}

# Number of requests to test
num_requests = 10

print(f"Benchmarking API with {num_requests} requests...")
print(f"URL: {url}")
print("-" * 50)

# Track response times
response_times = []

start_total = time.time()

with httpx.Client() as client:
    for i in range(num_requests):
        try:
            start = time.time()
            response = client.post(
                url,
                json=payload,
                headers={"content-type": "application/json"},
                timeout=30.0
            )
            end = time.time()

            elapsed = end - start
            response_times.append(elapsed)

            print(f"Request {i+1}: {elapsed:.3f}s - Status: {response.status_code}")

        except Exception as e:
            print(f"Request {i+1}: Error - {str(e)}")

end_total = time.time()

print("-" * 50)
print(f"Total time: {end_total - start_total:.3f}s")

if response_times:
    print(f"\nResponse Time Statistics:")
    print(f"  Min: {min(response_times):.3f}s")
    print(f"  Max: {max(response_times):.3f}s")
    print(f"  Avg: {sum(response_times) / len(response_times):.3f}s")
    print(f"  Total requests: {len(response_times)}")
