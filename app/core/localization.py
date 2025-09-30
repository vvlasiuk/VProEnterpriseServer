import json
import os
from typing import Dict, Optional
from fastapi import Request

class Localization:
    def __init__(self):
        self.translations: Dict[str, Dict] = {}
        self.default_lang = "uk"
        self.current_lang = "uk"  # За замовчуванням українська
        self.load_translations()
    
    def load_translations(self):
        """Завантаження файлів локалізації"""
        # Шлях до папки lang відносно поточного файлу
        current_dir = os.path.dirname(os.path.abspath(__file__))
        lang_dir = os.path.join(current_dir, "..", "lang")
        
        if not os.path.exists(lang_dir):
            print(f"⚠️ Папка {lang_dir} не знайдена")
            return
        
        for file_name in os.listdir(lang_dir):
            if file_name.endswith('.json'):
                lang_code = file_name.replace('.json', '')
                file_path = os.path.join(lang_dir, file_name)
                
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        self.translations[lang_code] = json.load(f)
                        print(f"✅ Завантажено мову: {lang_code}")
                except Exception as e:
                    print(f"⚠️ Помилка завантаження {file_path}: {e}")
    
    def get(self, key: str, category: str = "common", lang: Optional[str] = None) -> str:
        """Отримання перекладу з категорією"""
        lang = lang or self.current_lang
        
        # Спробувати поточну мову
        if (lang in self.translations and 
            category in self.translations[lang] and 
            key in self.translations[lang][category]):
            return self.translations[lang][category][key]
        
        # Якщо не знайдено - спробувати мову за замовчуванням
        if (self.default_lang in self.translations and 
            category in self.translations[self.default_lang] and 
            key in self.translations[self.default_lang][category]):
            return self.translations[self.default_lang][category][key]
        
        # Якщо нічого не знайдено - повернути ключ
        return f"{category}.{key}"
    
    def set_language(self, lang: str):
        """Встановлення поточної мови"""
        if lang in self.translations:
            self.current_lang = lang
            print(f"🌍 Мова змінена на: {lang}")
        else:
            print(f"⚠️ Мова {lang} не підтримується")
    
    def get_from_request(self, request: Request) -> str:
        lang = request.path_params.get("lang", self.default_lang)
        
        # Простий парсинг Accept-Language
        if "uk" in lang.lower():
            return "uk"
        else:
            return "en"
    
    def get_available_languages(self) -> list:
        """Список доступних мов"""
        return list(self.translations.keys())
    
    def get_current_language(self) -> str:
        """Отримання поточної мови"""
        return self.current_lang
    
# Глобальний об'єкт локалізації
i18n = Localization()