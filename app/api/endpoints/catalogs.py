import importlib
from fastapi import APIRouter, Depends, Query
from app.services.database_service import DatabaseService
from app.core.security import get_current_user

router = APIRouter()

@router.get("/catalog/{model_name}")
async def get_catalog_table(
    model_name: str,
    current_user: dict = Depends(get_current_user)
):
    if not model_name.startswith("cat_"):
        return {"error": "Дозволено моделі з префіксом 'cat'"}
    try:
        # Формуємо шлях до модуля, наприклад: app.models.models_catalog.cat_products_brands
        module_path = f"app.models.models_catalog.{model_name}"
        module = importlib.import_module(module_path)
        # Ім'я класу має співпадати з model_name
        model_class = getattr(module, model_name)
        # instance = model_class()  # Якщо потрібен екземпляр
        table_name = model_class._db_head.get("table_name")
        if not table_name:
            return {"error": "У моделі не вказано table_name"}
        query = f"SELECT * FROM {table_name}"
        data = await DatabaseService.execute_query(query)
        return {table_name: data}
    except Exception as e:
        return {"error": str(e)}