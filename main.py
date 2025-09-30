from fastapi import FastAPI, Request
from fastapi.responses import FileResponse
from app.api import api_router
from app.core.config import settings
from app.core.localization import i18n
import os

app = FastAPI(
    title=settings.APP_NAME,
    description="VPro: Enterprise Server",
    version=settings.VERSION,
    debug=settings.DEBUG
)

# Підключення всіх API роутів
app.include_router(api_router, prefix="/api/v1")

@app.get("/")
async def root(request: Request):
    """Root endpoint - API status and information"""
    # Визначення мови з заголовків запиту
    lang = i18n.get_current_language()
    
    # Базова інформація (завжди)
    response = {
        "service": settings.APP_NAME,
        "version": settings.VERSION,
        "status": i18n.get("service_running", "common", lang),
        "language": lang
    }
    
    # Додаткова інформація для development
    if settings.DEBUG:
        response.update({
            "environment": i18n.get("development", "common", lang),
            "database": f"MS SQL Server ({settings.DB_SERVER}:{settings.DB_PORT})",
            "protocol": "HTTPS" if getattr(settings, 'USE_SSL', False) else "HTTP",
            "docs": "/docs",
            "redoc": "/redoc",
            "endpoints": {
                "health": "/api/v1/health",
                "auth": "/api/v1/auth/login", 
                "users": "/api/v1/users",
                "language": "/api/v1/language"
            },
            "debug_mode": True,
            "available_languages": i18n.get_available_languages()
        })
    else:
        # Мінімальна інформація для production
        response.update({
            "environment": i18n.get("production", "common", lang),
            "contact": "vlasrpo.com.ua@gmail.com"
        })
    
    return response

@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    """Favicon endpoint"""
    favicon_path = "app/static/favicon.ico"
    if os.path.exists(favicon_path):
        return FileResponse(favicon_path, media_type="image/x-icon")
    else:
        # Заглушка якщо файл не знайдено
        from fastapi import Response
        return Response(status_code=204)

if __name__ == "__main__":
    from app.core.server_setup import start_server
    start_server()
    # import uvicorn
    
    # # Додаткова перевірка та конвертація типів
    # host = str(settings.HOST)
    # port = int(settings.PORT) 
    # reload = bool(settings.DEBUG)
    
    # print(f"Start server:")
    # print(f"  Host: {host}")
    # print(f"  Port: {port}")
    # print(f"  Environment: {'Development' if settings.DEBUG else 'Production'}")
    # print(f"  Reload: {reload}")
    # print(f"  Available languages: {i18n.get_available_languages()}")
    
    # # SSL параметри (якщо доступні)
    # ssl_params = {}
    # if hasattr(settings, 'USE_SSL') and settings.USE_SSL:
    #     ssl_params = {
    #         "ssl_keyfile": settings.SSL_KEYFILE,
    #         "ssl_certfile": settings.SSL_CERTFILE
    #     }
    #     if hasattr(settings, 'SSL_KEYFILE_PASSWORD') and settings.SSL_KEYFILE_PASSWORD:
    #         ssl_params["ssl_keyfile_password"] = settings.SSL_KEYFILE_PASSWORD
        
    #     print(f"  SSL: Enabled")
    #     print(f"  Certificate: {settings.SSL_CERTFILE}")
    # else:
    #     print(f"  SSL: Disabled")
    
    # try:
    #     uvicorn.run(
    #         "main:app",
    #         host=host, 
    #         port=port, 
    #         reload=reload,
    #         **ssl_params
    #     )
    # except Exception as e:
    #     print(f"Помилка запуску: {e}")
    #     if ssl_params:
    #         print("Спробуйте запустити через командний рядок з SSL:")
    #         print(f"uvicorn main:app --ssl-keyfile {ssl_params.get('ssl_keyfile')} --ssl-certfile {ssl_params.get('ssl_certfile')} --reload")
    #     else:
    #         print("Спробуйте запустити через командний рядок:")
    #         print("uvicorn main:app --reload")