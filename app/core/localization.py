from app.core.config import settings
import json
import os

class Localizer:
    def __init__(self):
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

        lang_code = settings.CURRENT_LANGUAGE

        lang_path = os.path.join(project_root, "app", "lang", f"{lang_code}.json")

        with open(lang_path, "r", encoding="utf-8") as f:
            self.translations = json.load(f)
        
        # Ініціалізація кешу
        self._translation_cache = {}

    def t(self, key: str) -> str:
        """Максимально швидка версія"""
        # Швидка перевірка кешу
        cached = self._translation_cache.get(key)
        if cached is not None:
            return cached
        
        # Розділяємо ключ
        parts = key.split(".")
        value = self.translations
        
        # Швидкий прохід без додаткових перевірок
        try:
            for part in parts:
                value = value[part]
            
            # Кешуємо результат
            result = str(value)
            self._translation_cache[key] = result
            return result
            
        except (KeyError, TypeError):
            # Кешуємо помилку теж
            error_result = f"[{key}]"
            self._translation_cache[key] = error_result
            return error_result
        
    def get_available_languages(self):
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        lang_dir = os.path.join(project_root, "app", "lang")
        
        if not os.path.exists(lang_dir):
            return [settings.CURRENT_LANGUAGE]
        
        available_langs = []
        for file_name in os.listdir(lang_dir):
            if file_name.endswith('.json'):
                lang_code = file_name.replace('.json', '')
                available_langs.append(lang_code)
        
        return available_langs if available_langs else [settings.CURRENT_LANGUAGE]
    
    def get_current_language(self):
        return settings.CURRENT_LANGUAGE