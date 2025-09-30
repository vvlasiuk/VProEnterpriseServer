# Це єдине місце створення об'єктів
from app.core.config import settings
from app.core.localization import Localizer

# Створюємо об'єкти ОДИН РАЗ при імпорті модуля
localizer = Localizer()

def get_localizer():
    """Отримання єдиного екземпляра локалізатора"""
    return localizer

def get_settings():
    """Отримання налаштувань"""
    return settings