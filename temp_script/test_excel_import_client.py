#!/usr/bin/env python3
"""
Client test for Excel import via API
Simulates real client application behavior
"""

import requests
import pandas as pd
import json
import time
import os
from io import BytesIO
from typing import Dict, List, Any, Optional
from datetime import datetime
import shutil

class ExcelImportClient:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.session = requests.Session()
        self.token = None
        
        # tables configuration
        self.test_tables = {
            'products_brands_import': {
                # 'test_data': [
                #     {'name': 'Samsung', 'external_id': '00001', 'mark_deleted': 0},
                #     {'name': 'Apple', 'external_id': '00005', 'mark_deleted': 1}
                # ],
                'excel_columns': ['Name', 'External ID', 'Mark deleted'],
                'mapping': {'Name': 'name', 'External ID': 'external_id', 'Mark deleted': 'mark_deleted'}
            }
        }

    def authenticate(self, username: str = "test", password: str = "test123"):
        """Authenticate user"""
        print("🔐 Authentication...")
        
        auth_data = {
            "username": username,
            "password": password
        }
        
        try:
            response = self.session.post(
                f"{self.base_url}/api/v1/auth/login",
                json=auth_data
            )
            
            if response.status_code == 200:
                result = response.json()
                self.token = result.get('access_token')
                self.session.headers.update({'Authorization': f'Bearer {self.token}'})
                print(f"✅ Authentication successful. Token: {self.token[:20]}...")
                return True
            else:
                print(f"❌ Authentication error: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            print(f"❌ Connection error: {e}")
            return False
        
    def import_excel(self, filepath: str, import_type: str):
        """Upload Excel file"""
        print(f"\n📤 Uploading file for import type '{import_type}'...")
        
        try:
            with open(filepath, 'rb') as f:
                files = {'file': (os.path.basename(filepath), f, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')}
                data = {
                    'import_type': import_type,
                    'source_id': 1,
                    'batch_size': 100
                }
                
                response = self.session.post(
                    f"{self.base_url}/api/v1/import/excel",
                    files=files,
                    data=data
                )
            
            if response.status_code == 200:
                result = response.json()
                print(f"✅ Import started. Task ID: {result.get('task_id')}")
                return result
            else:
                print(f"❌ Import error: {response.status_code} - {response.text}")
                return {}
                
        except Exception as e:
            print(f"❌ Error: {e}")
            return {}
        
    def test_table_import(self, import_type: str, filepath: str):
        """Full import test for table"""
        print(f"\n{'='*60}")
        print(f"🧪 TESTING TABLE IMPORT: {import_type} path: {filepath}")
        print(f"{'='*60}")
        
        # 4. Use mapping from configuration
        column_mapping = self.test_tables[import_type]['mapping']
        print(f"\n🔗 Using mapping:")
        for excel_col, table_col in column_mapping.items():
            print(f"   {excel_col} → {table_col}")
        
        # 5. Import file
        result = self.import_excel(filepath, import_type)
        
        # 6. Cleanup test file
        try:
            self.move_test_file(filepath, bool(result))
        except:
            pass
        
        return bool(result)
    
    def run_full_test(self, tables: Optional[List[List[str]]] = None):
        """Run full test suite"""
        print("🚀 RUNNING FULL EXCEL IMPORT TEST")
        print("="*60)
        
        # 1. Authentication
        if not self.authenticate(username="Admin", password="admin"):
            print("❌ Test terminated due to authentication error")
            return
        
        for import_type, filepath in tables:
            self.test_table_import(import_type, filepath)
    
    def move_test_file(self, filepath: str, success: bool):
        now_str = datetime.now().strftime("%Y-%m-%d %H_%M_%S")
        folder = "OK" if success else "ERR"
        file_dir = os.path.dirname(filepath)
        target_dir = os.path.join(file_dir, folder)
        os.makedirs(target_dir, exist_ok=True)
        new_name = f"{folder}_{now_str}_{os.path.basename(filepath)}"
        target_path = os.path.join(target_dir, new_name)
        shutil.move(filepath, target_path)
        print(f"📁 Test file moved to: {target_path}")

def main():
    """Main function"""
    print("🎯 EXCEL IMPORT CLIENT TEST")
    print("Make sure server is running at http://localhost:8000")
    
    # Create client
    client = ExcelImportClient(base_url="http://localhost:8000")
    
    tables_to_test = [['products_brands_import', 'C:\\Python\\temp data\\Sales\\Brands.xlsx']] 

    client.run_full_test(tables_to_test)
    
    input("Press Enter to continue...")

if __name__ == "__main__":
    main()

# ВЫБРАТЬ
# 	ВЫБОР КОГДА ПСТ_Бренды.ПометкаУдаления ТОГДА "'1" ИНАЧЕ "'0" КОНЕЦ Mark_deleted,
# 	"'" + ПСТ_Бренды.Код КАК External_ID ,
# 	ПСТ_Бренды.Наименование КАК Name 
# ИЗ
# 	Справочник.ПСТ_Бренды КАК ПСТ_Бренды
