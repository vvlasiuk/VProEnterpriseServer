import pythoncom
import win32com.client
import logging
from typing import Optional, Any, Dict
from dotenv import load_dotenv
import os
from threading import Lock

logger = logging.getLogger(__name__)

class C1Connector:
    """Singleton коннектор з lazy initialization для роботи з 1С8 через COM"""
    
    def __init__(self):
        self._connection: Optional[Any] = None
        self._is_connected: bool = False
        self._lock = Lock()
        self._config: Dict[str, str] = {}
        self._config_loaded: bool = False
    
    def _load_config(self):
        """Завантаження конфігурації з .env файлу"""
        if self._config_loaded:
            return
            
        env_path = os.path.join(os.path.dirname(__file__), '..', '..', 'connect_1c8', '.env')
        if os.path.exists(env_path):
            load_dotenv(env_path)
            self._config = {
                'Srvr': os.getenv('C1_SRVR', ''),
                'Ref': os.getenv('C1_REF', ''),
                'Usr': os.getenv('C1_USR', ''),
                'Pwd': os.getenv('C1_PWD', '')
            }
            self._config_loaded = True
            logger.info("1C configuration loaded from .env")
        else:
            logger.warning(f"1C .env file not found at {env_path}")
            self._config_loaded = True
    
    def ensure_connected(self) -> bool:
        """Автоматичне підключення при необхідності"""
        if self._is_connected:
            return True
        
        with self._lock:
            # Double-check після отримання lock
            if self._is_connected:
                return True
            
            self._load_config()
            
            try:
                pythoncom.CoInitialize()
                
                connection_string = f"Srvr={self._config['Srvr']};Ref={self._config['Ref']};Usr={self._config['Usr']};Pwd={self._config['Pwd']};"
                self._connection = win32com.client.Dispatch("V83.COMConnector").Connect(connection_string)
                self._is_connected = True
                
                logger.info(f"Connected to 1C: {self._config['Srvr']}/{self._config['Ref']}")
                return True
            except Exception as e:
                logger.error(f"Failed to connect to 1C: {e}")
                self._is_connected = False
                # raise ConnectionError(f"Cannot connect to 1C: {e}")
                return False
    
    def disconnect(self):
        """Закриття з'єднання з 1С"""
        with self._lock:
            if not self._is_connected:
                return
            
            try:
                self._connection = None
                pythoncom.CoUninitialize()
                self._is_connected = False
                logger.info("Disconnected from 1C")
            except Exception as e:
                logger.error(f"Error during 1C disconnect: {e}")
    
    def is_connected(self) -> bool:
        """Перевірка стану з'єднання"""
        return self._is_connected
    
    # def execute_procedure(self, procedure_path: str, *args) -> Any:
    #     """
    #     Виконання процедури в 1С (автоматично підключається при необхідності)
        
    #     Args:
    #         procedure_path: Шлях до процедури, наприклад "ПСТ_ВідправкаПовідомлень.TestPython"
    #         *args: Аргументи для процедури
            
    #     Returns:
    #         Результат виконання процедури
    #     """
    #     self._ensure_connected()  # Автоматичне підключення
        
    #     try:
    #         # Розбираємо шлях до процедури
    #         parts = procedure_path.split('.')
            
    #         # Отримуємо об'єкт за шляхом
    #         obj = self._connection
    #         for part in parts[:-1]:
    #             obj = getattr(obj, part)
            
    #         # Викликаємо метод
    #         method = getattr(obj, parts[-1])
    #         result = method(*args)
            
    #         logger.info(f"Executed 1C procedure: {procedure_path}")
    #         return result
            
    #     except Exception as e:
    #         logger.error(f"Error executing 1C procedure {procedure_path}: {e}")
    #         # При помилці скидаємо з'єднання для повторної спроби
    #         self._is_connected = False
    #         raise
    
    # def get_object(self, object_path: str) -> Any:
    #     """
    #     Отримання об'єкта з 1С за шляхом (автоматично підключається)
        
    #     Args:
    #         object_path: Шлях до об'єкта, наприклад "Справочники.Номенклатура"
    #     """
    #     self._ensure_connected()  # Автоматичне підключення
        
    #     try:
    #         obj = self._connection
    #         for part in object_path.split('.'):
    #             obj = getattr(obj, part)
    #         return obj
    #     except Exception as e:
    #         logger.error(f"Error getting 1C object {object_path}: {e}")
    #         self._is_connected = False
    #         raise


# Створення singleton екземпляра
connector_1c8 = C1Connector()