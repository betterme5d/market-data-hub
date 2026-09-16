import requests
import json
import os

def test_api():
    url = "http://localhost:8082/quote/BRND.L"
    print(f"Testing API endpoint: {url}")
    try:
        response = requests.get(url)
        print(f"Status Code: {response.status_code}")
        if response.status_code == 200:
            print("Response JSON:")
            print(json.dumps(response.json(), indent=2))
        else:
            print("Error Response:")
            print(response.text)
    except Exception as e:
        print(f"Request failed: {e}")

    url_cal = "http://localhost:8082/trading-days?exchange=CN&start=2026-01-01&end=2026-01-10"
    print(f"\nTesting Calendar API: {url_cal}")
    try:
        response = requests.get(url_cal)
        print(f"Status Code: {response.status_code}")
        if response.status_code == 200:
            print("Response JSON:")
            print(json.dumps(response.json(), indent=2))
        else:
            print("Error Response:")
            print(response.text)
    except Exception as e:
        print(f"Request failed: {e}")

if __name__ == "__main__":
    test_api()

