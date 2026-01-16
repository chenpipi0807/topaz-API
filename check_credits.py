import requests
from pathlib import Path
import sys

API_CREDITS_URL = "https://api.topazlabs.com/account/v1/credits/balance"
KEY_FILE = "key.txt"

def load_api_key() -> str:
    key_path = Path(KEY_FILE)
    if not key_path.exists():
        print(f"❌ API key file not found: {KEY_FILE}")
        sys.exit(1)
    return key_path.read_text().strip()

def check_credits():
    try:
        api_key = load_api_key()
        
        headers = {
            'X-API-Key': api_key
        }
        
        print("🔍 Checking credit balance...")
        print("-" * 50)
        
        response = requests.get(API_CREDITS_URL, headers=headers)
        
        if response.status_code != 200:
            print(f"❌ API Error: {response.status_code}")
            print(f"Response: {response.text}")
            sys.exit(1)
        
        data = response.json()
        
        available = data.get('available_credits', 0)
        reserved = data.get('reserved_credits', 0)
        total = data.get('total_credits', 0)
        
        print(f"💰 Available Credits:  {available:,}")
        print(f"🔒 Reserved Credits:   {reserved:,}")
        print(f"📊 Total Credits:      {total:,}")
        print("-" * 50)
        
        if available < 100:
            print("⚠️  Warning: Low credit balance!")
            print("💳 Add more credits at: https://topazlabs.com/my-account/subscriptions/")
        
    except FileNotFoundError as e:
        print(f"❌ Error: {e}")
        sys.exit(1)
    except requests.exceptions.RequestException as e:
        print(f"❌ Network Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    check_credits()
