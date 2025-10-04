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
            
            # Заповнити ролі
            await self.seed_roles()
            results["seeded_tables"].append("roles")
            
            # # Заповнити налаштування
            # await self.seed_settings()
            # results["seeded_tables"].append("settings")
            
        except Exception as e:
            results["errors"].append(str(e))
        
        return results
    
    async def seed_users(self):
        """Створити адміністратора якщо немає користувачів"""
        # Перевірити чи є користувачі
        count_query = "SELECT COUNT(*) FROM users"
        user_count = await DatabaseService.execute_scalar(count_query)
        
        if user_count == 0:
            # Створити адміністратора
            admin_data = {
                "name": "Administrator",
                "code": "admin",
                "full_name": "System Administrator",
                "email": "",
                "password_hash": self._hash_password("admin"),
                "is_active": True,
                "is_admin": True,
                "created_at": "GETDATE()",
                "created_by": 1  # Self-reference
            }
            
            await self._insert_user(admin_data)
            logger.info("Created default administrator")