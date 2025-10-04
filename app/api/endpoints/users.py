from fastapi import APIRouter, Depends, HTTPException, status
from typing import List, Dict
from datetime import datetime
from app.core.security import get_current_user, require_admin_role
from app.services.database_service import DatabaseService

router = APIRouter()

# Заглушка даних
MOCK_USERS = [
    {"id": 1, "name": "John Doe", "email": "john@example.com", "created_at": "2024-01-01T10:00:00"},
    {"id": 2, "name": "Jane Smith", "email": "jane@example.com", "created_at": "2024-01-02T10:00:00"},
]

@router.get("/")
async def get_users():
    """Отримати список користувачів з бази даних (публічний доступ)"""
    try:
        # SQL запит для отримання користувачів
        query = """
        SELECT 
            id,
            name,
            full_name
        FROM users 
        WHERE is_active = 1
        ORDER BY full_name
        """
        
        # Виконати запит
        users_data = await DatabaseService.execute_query(query)
        
        # Перетворити результат
        users = []
        for row in users_data:
            user = {
                "id": row["id"],
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
            "users": MOCK_USERS,
            "total": len(MOCK_USERS),
            "source": "mock",
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

@router.post("/")
async def create_user(
    user_data: Dict, 
    current_user: Dict = Depends(get_current_user)
):
    """Створити користувача (тільки для адмінів)"""
    require_admin_role(current_user)
    
    new_user = {
        "id": len(MOCK_USERS) + 1,
        "name": user_data.get("name", "Unknown"),
        "email": user_data.get("email", "unknown@example.com"),
        "created_at": datetime.utcnow().isoformat(),
        "created_by": current_user["username"]
    }
    MOCK_USERS.append(new_user)
    return new_user

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
