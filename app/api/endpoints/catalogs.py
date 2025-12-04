import importlib
from fastapi import APIRouter, Depends, Query
from app.services.database_service import DatabaseService
from app.core.security import get_current_user

router = APIRouter()

# @router.get("/catalog/{model_name}")
@router.get("/catalog")
async def get_catalog_table(
    model_name: str = Query(...),
    skip: int = Query(0, ge=0),
    limit: int = Query(10, ge=1, le=100),
    current_user: dict = Depends(get_current_user)
):
    if not model_name.startswith("Cat_"):
        return {"error": "Дозволено моделі з префіксом 'Cat'"}
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
        
        # Загальна кількість записів
        count_query = f"SELECT COUNT(*) as total FROM {table_name}"
        count_result = await DatabaseService.execute_query(count_query)
        total = count_result[0]["total"] if count_result else 0


        # query = f"SELECT * FROM {table_name}"
        query = f"SELECT _id, name, mark_deleted FROM {table_name} ORDER BY name OFFSET {skip} ROWS FETCH NEXT {limit} ROWS ONLY"
        data = await DatabaseService.execute_query(query)

        # cat_data = []
        # for row in data:
        #     cat = {
        #         "id": row["_id"],
        #         "name": row["name"]
        #     }
        #     cat_data.append(cat)

        return {
            "data": data,
            "total": total,
            "skip": skip,
            "limit": limit,
            "table_name": table_name,
            "model_name": model_name
         }
    except Exception as e:
        return {"error": str(e)}