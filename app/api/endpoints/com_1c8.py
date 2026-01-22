from fastapi import APIRouter, Depends, HTTPException, Body
# from typing import Dict, Any, List
from app.core.security import get_current_user 
from pydantic import BaseModel
from typing import Any, List

from app.integrations.connector_1с8 import connector_1c8
import json

class FilterItem(BaseModel):
    field: str
    value: Any
    operator: str = "equals"

class PosRequest(BaseModel):
    skip: int = 0
    limit: int = 100
    filters: List[FilterItem] = []
# router = APIRouter(prefix="/com_1c8", tags=["com_1c8"])
router = APIRouter(prefix="", tags=["schemas"])


# @router.post("/com_1c8/test")
@router.get("/counterparty")
async def get_counterparty():
    try:
        if not connector_1c8.ensure_connected():
            raise ConnectionError("Не вдалося підключитися до 1С")
        
        result = connector_1c8._connection.ПСТ_ВідправкаПовідомлень.TestPython()

        return {"success": True, "result": result.Текст}
    except ConnectionError as e:
        raise HTTPException(status_code=503, detail=f"1C unavailable: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/tool-entry-for-sharpening")
async def get_tool_entry_for_sharpening(
    request: PosRequest = Body(...),
    current_user = Depends(get_current_user),
    # skip: int = 0,  # offset для пагінації
    # limit: int = 100,  # кількість записів на сторінці
    # filters = []
):
    try:
        if not connector_1c8.ensure_connected(): 
            raise ConnectionError("Не вдалося підключитися до 1С")
        
        # Отримати всі документи
        result = connector_1c8._connection.VPE.ОтриматиСписок_Документ()
        
        cleaned_text = result.text.replace("\r\n", "").replace("\n", "").replace("\r", "")
        all_data = json.loads(cleaned_text)
        
        # Застосувати фільтри
        filtered_data = all_data
        if request.filters:
            for filter_item in request.filters:
                field = filter_item.field
                value = filter_item.value
                operator = filter_item.operator
                
                if operator == "equals":
                    filtered_data = [item for item in filtered_data if item.get(field) == value]
                elif operator == "contains":
                    filtered_data = [item for item in filtered_data if value.lower() in str(item.get(field, "")).lower()]

        # # Застосувати пагінацію
        # paginated_data = all_data[request.skip:request.skip + request.limit]
        paginated_data = filtered_data[request.skip:request.skip + request.limit]
    
        return {
            "data": paginated_data,        # замість "documents"
            "total": len(filtered_data)          # загальна кількість
        }
    except ConnectionError as e:
        raise HTTPException(status_code=503, detail=f"1C unavailable: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))