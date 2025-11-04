import requests
import json

def test_auth():
    base_url = "http://localhost:8000"
    
    print("🔍 Тестування авторизації...")
    
    # 1. Логін
    login_url = f"{base_url}/api/v1/auth/login"
    login_data = {
        "username": "Admin",
        "password": "admin"
    }
    
    try:
        print("📝 Спроба авторизації...")
        response = requests.post(login_url, json=login_data)
        print(f"Status: {response.status_code}")
        
        if response.status_code == 200:
            auth_data = response.json()
            token = auth_data["access_token"]
            print(f"✅ Отримано токен: {token}")
            
            # 2. Тестування захищеного endpoint
            users_url = f"{base_url}/api/v1/users/"
            headers = {"Authorization": f"Bearer {token}"}
            
            print("📋 Запит до захищеного endpoint...")
            users_response = requests.get(users_url, headers=headers)
            print(f"Users endpoint status: {users_response.status_code}")
            
            if users_response.status_code == 200:
                users_data = users_response.json()
                print(f"✅ Отримано дані користувачів: {len(users_data.get('users', []))} користувачів")
            else:
                print(f"❌ Помилка: {users_response.text}")
        else:
            print(f"❌ Помилка авторизації: {response.text}")
    
    except Exception as e:
        print(f"❌ Помилка з'єднання: {e}")

if __name__ == "__main__":
    test_auth()