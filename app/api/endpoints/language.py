from fastapi import APIRouter, Request
from app.core.localization import i18n

router = APIRouter()

@router.post("/{lang}")
async def set_language(lang: str, request: Request):
    """Зміна мови інтерфейсу"""
    current_lang = i18n.get_from_request(request)
    
    if lang in i18n.get_available_languages():
        i18n.set_language(lang)
        return {
            "message": i18n.get("language_changed", "common", lang),
            "previous_language": current_lang,
            "current_language": lang,
            "available_languages": i18n.get_available_languages()
        }
    else:
        return {
            "error": f"Language {lang} not supported",
            "current_language": current_lang,
            "available_languages": i18n.get_available_languages()
        }

@router.get("/")
async def get_language_info():
    """Отримати інформацію про поточну мову"""
    current_lang = i18n.get_current_language()
    return {
        "current_language": current_lang,
        "default_language": i18n.default_lang,
        "available_languages": i18n.get_available_languages()
    }