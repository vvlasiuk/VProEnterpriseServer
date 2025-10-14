import requests

# Отримати список користувачів
response = requests.get("http://127.0.0.1:8000/api/v1/users/")
print(response.json())
