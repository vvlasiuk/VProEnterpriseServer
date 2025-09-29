from fastapi import APIRouter

# Створюємо основний API роутер
api_router = APIRouter()

# Імпортуємо endpoints
from .endpoints import health

# Підключаємо роутери
api_router.include_router(health.router, prefix="/health", tags=["health"])