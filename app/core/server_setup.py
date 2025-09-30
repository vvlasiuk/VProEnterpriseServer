import uvicorn
from typing import Dict, Any
from app.core.config import settings
from app.core.app_globals import get_localizer

def get_ssl_config() -> Dict[str, Any]:
    """Отримання SSL конфігурації"""
    ssl_params = {}
    
    if hasattr(settings, 'USE_SSL') and settings.USE_SSL:
        # Перевірка наявності файлів сертифікатів
        if not hasattr(settings, 'SSL_KEYFILE') or not settings.SSL_KEYFILE:
            print("WARNING: SSL enabled but SSL_KEYFILE not specified")
            return {}
        
        if not hasattr(settings, 'SSL_CERTFILE') or not settings.SSL_CERTFILE:
            print("WARNING: SSL enabled but SSL_CERTFILE not specified")
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
    host = str(settings.HOST)
    port = int(settings.PORT)
    reload = bool(settings.DEBUG)
    ssl_params = get_ssl_config()
    
    localizer = get_localizer()
    
    print("Starting server:")
    print(f"  Host: {host}")
    print(f"  Port: {port}")
    print(f"  Environment: {'Development' if settings.DEBUG else 'Production'}")
    print(f"  Reload: {reload}")
    print(f"  Current language: {localizer.get_current_language()}")
    print(f"  Available languages: {localizer.get_available_languages()}")
    
    if ssl_params:
        print("  SSL: Enabled")
        print(f"  Certificate: {settings.SSL_CERTFILE}")
        print(f"  Key file: {settings.SSL_KEYFILE}")
    else:
        print("  SSL: Disabled")
    
    return host, port, reload, ssl_params

def start_server(app_module: str = "main:app"):    
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
        print(f"ERROR: Server startup failed: {e}")
        print_fallback_commands(ssl_params)

def print_fallback_commands(ssl_params: Dict[str, Any]):
    """Виведення альтернативних команд запуску"""
    print("\nAlternative startup methods:")
    
    if ssl_params:
        print("With SSL via command line:")
        cmd = f"uvicorn main:app --ssl-keyfile {ssl_params.get('ssl_keyfile')} --ssl-certfile {ssl_params.get('ssl_certfile')}"
        if 'ssl_keyfile_password' in ssl_params:
            cmd += f" --ssl-keyfile-password {ssl_params['ssl_keyfile_password']}"
        if settings.DEBUG:
            cmd += " --reload"
        print(f"  {cmd}")
    
    print("Without SSL via command line:")
    cmd = "uvicorn main:app"
    if settings.HOST != "0.0.0.0":
        cmd += f" --host {settings.HOST}"
    if settings.PORT != 8000:
        cmd += f" --port {settings.PORT}"
    if settings.DEBUG:
        cmd += " --reload"
    print(f"  {cmd}")
    
    print("Via Python:")
    print("  python main.py")
