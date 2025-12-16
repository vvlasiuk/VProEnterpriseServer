from fastapi import APIRouter

# Створюємо основний роутер
api_router = APIRouter()

# Імпорт endpoints
from .endpoints import health, users, auth, data_import, catalogs, configurator

# Підключення роутерів
api_router.include_router(health.router, prefix="/health", tags=["health"])
api_router.include_router(auth.router, prefix="/auth", tags=["authentication"])
api_router.include_router(users.router, prefix="/users", tags=["users"])
api_router.include_router(data_import.router, prefix="/import", tags=["import"])
api_router.include_router(catalogs.router, prefix="", tags=["data"])
api_router.include_router(configurator.router, prefix="/configurator", tags=["configurator"])