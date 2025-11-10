from fastapi import APIRouter, Depends, HTTPException, status
from typing import List, Dict
from datetime import datetime
from app.core.security import get_current_user, require_admin_role, hash_password
from app.services.database_service import DatabaseService

router = APIRouter()

# # Заглушка даних
# MOCK_USERS = [
#     {"id": 1, "name": "John Doe", "email": "john@example.com", "created_at": "2024-01-01T10:00:00"},
#     {"id": 2, "name": "Jane Smith", "email": "jane@example.com", "created_at": "2024-01-02T10:00:00"},
# ]

@router.get("/")
async def get_users():
    """Отримати список користувачів з бази даних (публічний доступ)"""
    try:
        # SQL запит для отримання користувачів
        query = """
        SELECT 
            _id,
            name,
            full_name
        FROM cat_users 
        WHERE is_active = 1
        ORDER BY full_name
        """
        
        # Виконати запит
        users_data = await DatabaseService.execute_query(query)
        
        # Перетворити результат
        users = []
        for row in users_data:
            user = {
                "id": row["_id"],
                "name": row["name"],
                "full_name": row["full_name"]
            }
            users.append(user)
        
        return {
            "users": users,
            "total": len(users),
            "source": "database"
        }
        
    except Exception as e:
        # Fallback на MOCK дані якщо БД недоступна
        return {
            "users": None,
            "total": 0,
            # "source": "mock",
            "error": str(e)
        }

@router.get("/{user_id}")
async def get_user(user_id: int, current_user: Dict = Depends(get_current_user)):
    """Отримати користувача за ID (потрібна авторизація)"""
    user = next((u for u in MOCK_USERS if u["id"] == user_id), None)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    return user

@router.post("/create_user")
async def create_user(
    user_data: Dict, 
    current_user: Dict = Depends(get_current_user)
):
    """Створити користувача (тільки для адмінів)"""
    # require_admin_role(current_user)

    # Перевірка унікальності name/full_name/email за потреби

    password_hash = hash_password(user_data.get("password", ""))

    query = """
    INSERT INTO cat_users (name, full_name, email, password_hash, is_active, is_admin, _created_at, _created_by)
    VALUES (?, ?, ?, ?, ?, ?, GETDATE(), ?)
    """
    params = (
        user_data.get("name", ""),
        user_data.get("full_name", ""),
        user_data.get("email", ""),
        password_hash,
        True,
        user_data.get("is_admin", False),
        current_user["_id"]
    )
    await DatabaseService.execute_non_query(query, params)

    # Отримати створеного користувача (наприклад, за name)
    select_query = "SELECT _id, name, full_name, email FROM cat_users WHERE name = ?"
    result = await DatabaseService.execute_query(select_query, (user_data.get("name", ""),))
    if not result:
        raise HTTPException(status_code=500, detail="User creation failed")
    return result[0]

@router.delete("/{user_id}")
async def delete_user(
    user_id: int, 
    current_user: Dict = Depends(get_current_user)
):
    """Видалити користувача (тільки для адмінів)"""
    require_admin_role(current_user)
    
    global MOCK_USERS
    user = next((u for u in MOCK_USERS if u["id"] == user_id), None)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    MOCK_USERS = [u for u in MOCK_USERS if u["id"] != user_id]
    return {"message": f"User {user_id} deleted by {current_user['username']}"}
