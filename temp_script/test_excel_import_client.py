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
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.session = requests.Session()
        self.token = None
        
        # tables configuration
        self.test_tables = {
            'products_brands_import': {
                'test_data': [
                    {'name': 'Samsung', 'external_id': '00001', 'mark_deleted': 0},
                    {'name': 'Apple', 'external_id': '00005', 'mark_deleted': 1}
                ],
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
    
    def get_available_tables(self) -> List[str]:
        """Get list of available tables for import"""
        print("\n📋 Getting tables list...")
        
        try:
            response = self.session.get(f"{self.base_url}/api/v1/import/tables")
            
            if response.status_code == 200:
                tables = response.json()
                print(f"✅ Available tables: {', '.join(tables)}")
                return tables
            else:
                print(f"❌ Error getting tables: {response.status_code}")
                return []
                
        except Exception as e:
            print(f"❌ Error: {e}")
            return []
    
    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """Get table schema"""
        print(f"\n🔍 Getting table schema '{table_name}'...")
        
        try:
            response = self.session.get(f"{self.base_url}/api/v1/import/tables/{table_name}/schema")
            
            if response.status_code == 200:
                schema = response.json()
                print(f"✅ Schema received. Columns: {schema.get('total_columns', 0)}")
                print(f"   Required: {', '.join(schema.get('required_columns', []))}")
                return schema
            else:
                print(f"❌ Error getting schema: {response.status_code}")
                return {}
                
        except Exception as e:
            print(f"❌ Error: {e}")
            return {}
    
    def create_test_excel(self, table_name: str, filename: str = None) -> str:
        """Create test Excel file"""
        if table_name not in self.test_tables:
            raise ValueError(f"Test data for table '{table_name}' not found")
        
        table_config = self.test_tables[table_name]
        
        # Create DataFrame with test data
        df_data = []
        for row in table_config['test_data']:
            excel_row = {}
            for excel_col, db_col in table_config['mapping'].items():
                excel_row[excel_col] = row.get(db_col, '')
            df_data.append(excel_row)
        
        df = pd.DataFrame(df_data)
        
        # Save to Excel
        if not filename:
            filename = f"test_{table_name}.xlsx"
        
        filepath = os.path.join("temp_script", filename)
        df.to_excel(filepath, index=False, engine='openpyxl')
        
        print(f"📄 Created test Excel: {filepath}")
        print(f"   Rows: {len(df)}, Columns: {len(df.columns)}")
        print(f"   Columns: {', '.join(df.columns)}")
        
        return filepath
    
    def preview_excel(self, filepath: str, table_name: str = None) -> Dict[str, Any]:
        """Preview Excel file"""
        print(f"\n👀 File preview '{filepath}'...")
        
        try:
            with open(filepath, 'rb') as f:
                files = {'file': (os.path.basename(filepath), f, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')}
                data = {}
                
                if table_name:
                    data['table_name'] = table_name
                
                response = self.session.post(
                    f"{self.base_url}/api/v1/import/preview",
                    files=files,
                    data=data
                )
            
            if response.status_code == 200:
                result = response.json()
                print(f"✅ Preview successful")
                print(f"   Rows: {result.get('total_rows', 0)}")
                print(f"   Columns: {', '.join(result.get('columns', []))}")
                print("💡 Suggested mapping:")
                
                if 'suggested_mapping' in result:
                    for excel_col, table_col in result['suggested_mapping'].items():
                        print(f"   {excel_col} → {table_col}")
                
                return result
            else:
                print(f"❌ Preview error: {response.status_code} - {response.text}")
                return {}
                
        except Exception as e:
            print(f"❌ Error: {e}")
            return {}
    
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
    
    def get_import_statistics(self) -> Dict[str, Any]:
        """Get import statistics"""
        print("\n📊 Getting statistics...")
        
        try:
            response = self.session.get(f"{self.base_url}/api/v1/import/statistics")
            
            if response.status_code == 200:
                stats = response.json()
                print(f"✅ Statistics received")
                print(f"   Total mappings: {stats.get('total_mappings', 0)}")
                print(f"   Active types: {stats.get('active_types', 0)}")
                return stats
            else:
                print(f"❌ Error getting statistics: {response.status_code}")
                return {}
                
        except Exception as e:
            print(f"❌ Error: {e}")
            return {}
    
    def test_table_import(self, import_type: str, filepath: str):
        """Full import test for table"""
        print(f"\n{'='*60}")
        print(f"🧪 TESTING TABLE IMPORT: {import_type} path: {filepath}")
        print(f"{'='*60}")
        
        # # 1. Get table schema
        # schema = self.get_table_schema(table_name)
        # if not schema:
        #     print(f"❌ Cannot continue test for {table_name}")
        #     return False
        
        # # 2. Create test Excel
        # try:
        #     filepath = self.create_test_excel(table_name)
        # except Exception as e:
        #     print(f"❌ Excel creation error: {e}")
        #     return False

        # filepath = "C:\\Python\\temp data\\Sales\\Brands.xlsx"

        # 3. Preview file
        # preview = self.preview_excel(filepath, table_name)
        # if not preview:
        #     print(f"❌ Cannot continue test for {table_name}")
        #     return False
        
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

        # # 2. Get available tables
        # available_tables = self.get_available_tables()
        # if not available_tables:
        #     print("❌ Test terminated - no available tables")
        #     return
        
        # 3. Test each table
        # success_count = 0
        # total_count = 0
        
        # for table_name in self.test_tables.keys():
        #     if table_name in available_tables:
        #         total_count += 1
        #         if self.test_table_import(table_name):
        #             success_count += 1
        #         time.sleep(2)  # Pause between tests
        #     else:
        #         print(f"⚠️ Table {table_name} not available for import")
        
        # 4. Statistics
        # self.get_import_statistics()
        
        # 5. Summary
        # print(f"\n{'='*60}")
        # print(f"📋 TEST SUMMARY")
        # print(f"{'='*60}")
        # print(f"✅ Successful: {success_count}/{total_count}")
        # print(f"❌ Errors: {total_count - success_count}")
        
        # if success_count == total_count:
        #     print("🎉 ALL TESTS PASSED SUCCESSFULLY!")
        # else:
        #     print("⚠️ Some tests completed with errors")
    
    def add_new_table_test(self, table_name: str, test_data: List[Dict], excel_columns: List[str], mapping: Dict[str, str]):
        """Add new table for testing"""
        self.test_tables[table_name] = {
            'test_data': test_data,
            'excel_columns': excel_columns,
            'mapping': mapping
        }
        print(f"✅ Added configuration for testing table: {table_name}")

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
    # input("Press Enter to continue...")
    
    # Create client
    client = ExcelImportClient()
    
    # Example of adding new table for testing
    # client.add_new_table_test(
    #     table_name='cat_new_table',
    #     test_data=[
    #         {'name': 'Test Item 1', 'created_by': 1},
    #         {'name': 'Test Item 2', 'created_by': 1}
    #     ],
    #     excel_columns=['Name', 'Created By'],
    #     mapping={'Name': 'name', 'Created By': 'created_by'}
    # )
    
    # Run tests
    tables_to_test = [['products_brands_import', 'C:\\Python\\temp data\\Sales\\Brands.xlsx']] 
    client.run_full_test(tables_to_test)
    input("Press Enter to continue...")

# ВЫБРАТЬ
# 	ВЫБОР КОГДА ПСТ_Бренды.ПометкаУдаления ТОГДА "'1" ИНАЧЕ "'0" КОНЕЦ Mark_deleted,
# 	"'" + ПСТ_Бренды.Код КАК External_ID ,
# 	ПСТ_Бренды.Наименование КАК Name 
# ИЗ
# 	Справочник.ПСТ_Бренды КАК ПСТ_Бренды

if __name__ == "__main__":
    main()