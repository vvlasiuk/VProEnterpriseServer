import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from app.core.config import settings
import pyodbc

# Використовуємо дані з settings (які можуть братися з .env)
server = settings.DB_SERVER
port = settings.DB_PORT
user = settings.DB_USERNAME
password = settings.DB_PASSWORD
base = settings.DB_DATABASE

driver = settings.DB_DRIVER

conn_str = (
    f"DRIVER={{{driver}}};"
    f"SERVER={server},{port};"
    f"DATABASE={base};"
    f"UID={user};"
    f"PWD={password};"
    f"TrustServerCertificate=yes;"
    f"Encrypt=yes;"
)

try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT 1 AS test_result")
    row = cursor.fetchone()
    print(f"Connection successful, test result: {row.test_result}")
    conn.close()
except Exception as e:
    print(f"Connection failed: {e}")
