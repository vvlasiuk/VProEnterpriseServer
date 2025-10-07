from __future__ import annotations  # Для підтримки нових типів
import logging
from typing import Dict, Any, List, Optional
from app.services.database_service import DatabaseService
import hashlib

# app/services/seed_data_service.py
class SeedDataService:
    """Сервіс для заповнення початковими даними"""
    
    async def seed_all_data(self) -> Dict[str, Any]:
        """Заповнити всі таблиці початковими даними"""
        results = {
            "seeded_tables": [],
            "skipped_tables": [],
            "errors": []
        }
        
        try:
            # Заповнити користувачів
            await self.seed_users()
            results["seeded_tables"].append("users")
            
            # # Заповнити ролі
            # await self.seed_roles()
            # results["seeded_tables"].append("roles")
            
            # # Заповнити налаштування
            # await self.seed_settings()
            # results["seeded_tables"].append("settings")
            
            # Заповнити типи продукції
            await self.seed_product_types()
            results["seeded_tables"].append("product_types")
            
        except Exception as e:
            results["errors"].append(str(e))
        
        return results
    
    async def seed_users(self):
        """Створити адміністратора якщо немає користувачів"""
        # Перевірити чи є користувачі
        count_query = "SELECT COUNT(*) FROM cat_users"
        user_count = await DatabaseService.execute_scalar(count_query)
        
        if user_count == 0:
            # Створити адміністратора
            admin_data = {
                "name": "Administrator",
                "full_name": "System Administrator",
                "email": "",
                "password_hash": self._hash_password("admin"),
                "is_active": True,
                "is_admin": True,
                "created_at": "GETDATE()",
                "created_by": 1  # Self-reference
            }
            
            await self._insert_user(admin_data)
            # logger.info("Created default administrator")
    
    def _hash_password(self, password: str) -> str:
        """Захешувати пароль використовуючи bcrypt"""
        try:
            import bcrypt
            salt = bcrypt.gensalt()
            hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
            return hashed.decode('utf-8')
        except ImportError:
            # Fallback на простий хеш якщо bcrypt не встановлений
            import hashlib
            return hashlib.sha256(password.encode()).hexdigest()
    
    async def _insert_user(self, user_data: dict):
        """Вставити користувача в БД"""
        query = """
        INSERT INTO cat_users (name, full_name, email, password_hash, is_active, is_admin, created_at)
        VALUES (?, ?, ?, ?, ?, ?, GETDATE())
        """
        
        await DatabaseService.execute_non_query(query, (
            user_data["name"],
            # user_data["code"], 
            user_data["full_name"],
            user_data["email"],
            user_data["password_hash"],
            user_data["is_active"],
            user_data["is_admin"]
        ))
    
    async def seed_product_types(self):
        """Заповнити типи продукції базовими значеннями"""
        # Перевірити чи є типи
        count_query = "SELECT COUNT(*) FROM cat_products_type"
        types_count = await DatabaseService.execute_scalar(count_query)
        
        if types_count == 0:
            # Базові типи продукції
            product_types = [
                {
                    "name": "Товари",
                    "type_code": "GOODS", 
                    "created_by": 1
                },
                {
                    "name": "Матеріали", 
                    "type_code": "MATERIALS",
                    "created_by": 1
                },
                {
                    "name": "Послуги",
                    "type_code": "SERVICES", 
                    "created_by": 1
                },
                {
                    "name": "Сировина",
                    "type_code": "RAW",
                    "created_by": 1
                },
                {
                    "name": "Тара",
                    "type_code": "PACKAGING",
                    "created_by": 1
                }
            ]
            
            for product_type in product_types:
                await self._insert_product_type(product_type)
    
    async def _insert_product_type(self, product_type_data: dict):
        """Вставити тип продукції в БД"""
        query = """
        INSERT INTO cat_products_type (name, type_code, created_by, created_at)
        VALUES (?, ?, ?, GETDATE())
        """
        
        await DatabaseService.execute_non_query(query, (
            product_type_data["name"],
            product_type_data["type_code"], 
            product_type_data["created_by"]
        ))