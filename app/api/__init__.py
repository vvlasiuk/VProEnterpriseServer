from fastapi import APIRouter

# Створюємо основний роутер
api_router = APIRouter()

# Імпорт endpoints
from .endpoints import health, users, auth, language

# Підключення роутерів
api_router.include_router(health.router, prefix="/health", tags=["health"])
api_router.include_router(auth.router, prefix="/auth", tags=["authentication"])
api_router.include_router(users.router, prefix="/users", tags=["users"])
api_router.include_router(language.router, prefix="/language", tags=["localization"])