from fastapi import APIRouter, Depends, HTTPException
# from typing import Dict, Any, List
from app.core.security import get_current_user 

from app.integrations.connector_1с8 import connector_1c8
import json

# router = APIRouter(prefix="/com_1c8", tags=["com_1c8"])
router = APIRouter(prefix="", tags=["schemas"])


# @router.post("/com_1c8/test")
@router.get("/counterparty")
async def com_1c8():
    try:
        if not connector_1c8.ensure_connected():
            raise ConnectionError("Не вдалося підключитися до 1С")
        
        result = connector_1c8._connection.ПСТ_ВідправкаПовідомлень.TestPython()

        return {"success": True, "result": result.Текст}
    except ConnectionError as e:
        raise HTTPException(status_code=503, detail=f"1C unavailable: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/tool-entry-for-sharpening")
async def com_1c8(current_user = Depends(get_current_user)):
    try:
        if not connector_1c8.ensure_connected(): 
            raise ConnectionError("Не вдалося підключитися до 1С")
        
        result = connector_1c8._connection.VPE.ОтриматиСписок_Документ()
        
        cleaned_text = result.text.replace("\r\n", "").replace("\n", "").replace("\r", "")
        data = json.loads(cleaned_text)
        return {"success": True, "documents": data}
    except ConnectionError as e:
        raise HTTPException(status_code=503, detail=f"1C unavailable: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))