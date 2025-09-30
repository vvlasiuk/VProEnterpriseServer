import uvicorn
from typing import Dict, Any
from app.core.config import settings
from app.core.localization import i18n

def get_ssl_config() -> Dict[str, Any]:
    """Отримання SSL конфігурації"""
    ssl_params = {}
    
    if hasattr(settings, 'USE_SSL') and settings.USE_SSL:
        # Перевірка наявності файлів сертифікатів
        if not hasattr(settings, 'SSL_KEYFILE') or not settings.SSL_KEYFILE:
            print("⚠️ SSL увімкнено, але SSL_KEYFILE не вказано")
            return {}
        
        if not hasattr(settings, 'SSL_CERTFILE') or not settings.SSL_CERTFILE:
            print("⚠️ SSL увімкнено, але SSL_CERTFILE не вказано")
            return {}
        
        ssl_params = {
            "ssl_keyfile": settings.SSL_KEYFILE,
            "ssl_certfile": settings.SSL_CERTFILE
        }
        
        # Додати пароль якщо є
        if hasattr(settings, 'SSL_KEYFILE_PASSWORD') and settings.SSL_KEYFILE_PASSWORD:
            ssl_params["ssl_keyfile_password"] = settings.SSL_KEYFILE_PASSWORD
    
    return ssl_params

def print_server_info():
    """Виведення інформації про сервер"""
    host = str(settings.HOST)
    port = int(settings.PORT)
    reload = bool(settings.DEBUG)
    ssl_params = get_ssl_config()
    
    print(f"🚀 Запуск сервера:")
    print(f"  📍 Host: {host}")
    print(f"  🔌 Port: {port}")
    print(f"  🌍 Environment: {'Development' if settings.DEBUG else 'Production'}")
    print(f"  🔄 Reload: {reload}")
    print(f"  🗣️ Available languages: {i18n.get_available_languages()}")
    
    if ssl_params:
        print(f"  🔒 SSL: Enabled")
        print(f"  📜 Certificate: {settings.SSL_CERTFILE}")
        print(f"  🔑 Key file: {settings.SSL_KEYFILE}")
    else:
        print(f"  🔓 SSL: Disabled")
    
    return host, port, reload, ssl_params

def start_server(app_module: str = "main:app"):
    """Запуск сервера з обробкою помилок"""
    host, port, reload, ssl_params = print_server_info()
    
    try:
        uvicorn.run(
            app_module,
            host=host,
            port=port,
            reload=reload,
            **ssl_params
        )
    except Exception as e:
        print(f"❌ Помилка запуску: {e}")
        print_fallback_commands(ssl_params)

def print_fallback_commands(ssl_params: Dict[str, Any]):
    """Виведення альтернативних команд запуску"""
    print("\n💡 Альтернативні способи запуску:")
    
    if ssl_params:
        print("📋 З SSL через командний рядок:")
        cmd = f"uvicorn main:app --ssl-keyfile {ssl_params.get('ssl_keyfile')} --ssl-certfile {ssl_params.get('ssl_certfile')}"
        if 'ssl_keyfile_password' in ssl_params:
            cmd += f" --ssl-keyfile-password {ssl_params['ssl_keyfile_password']}"
        if settings.DEBUG:
            cmd += " --reload"
        print(f"  {cmd}")
    
    print("📋 Без SSL через командний рядок:")
    cmd = "uvicorn main:app"
    if settings.HOST != "0.0.0.0":
        cmd += f" --host {settings.HOST}"
    if settings.PORT != 8000:
        cmd += f" --port {settings.PORT}"
    if settings.DEBUG:
        cmd += " --reload"
    print(f"  {cmd}")
    
    print("📋 Через Python:")
    print(f"  python main.py")