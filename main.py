from fastapi import FastAPI
from fastapi.responses import FileResponse
from app.api import api_router
from app.core.config import settings
import os

app = FastAPI(
    title=settings.APP_NAME,
    description="Enterprise Server API with MS SQL",
    version=settings.VERSION,
    debug=settings.DEBUG
)

# Підключення всіх API роутів
app.include_router(api_router, prefix="/api/v1")

@app.get("/")
async def root():
    return {
        "message": f"{settings.APP_NAME} is running",
        "version": settings.VERSION,
        "database": f"MS SQL Server ({settings.DB_SERVER}:{settings.DB_PORT})",
        "environment": "development" if settings.DEBUG else "production",
        "protocol": "HTTPS" if getattr(settings, 'USE_SSL', False) else "HTTP",
        "docs": "/docs"
    }

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
    import uvicorn
    
    # Додаткова перевірка та конвертація типів
    host = str(settings.HOST)
    port = int(settings.PORT) 
    reload = bool(settings.DEBUG)
    
    print(f"Запуск сервера:")
    print(f"  Host: {host}")
    print(f"  Port: {port}")
    print(f"  Environment: {'Development' if settings.DEBUG else 'Production'}")
    print(f"  Reload: {reload}")
    
    # SSL параметри (якщо доступні)
    ssl_params = {}
    if hasattr(settings, 'USE_SSL') and settings.USE_SSL:
        ssl_params = {
            "ssl_keyfile": settings.SSL_KEYFILE,
            "ssl_certfile": settings.SSL_CERTFILE
        }
        if hasattr(settings, 'SSL_KEYFILE_PASSWORD') and settings.SSL_KEYFILE_PASSWORD:
            ssl_params["ssl_keyfile_password"] = settings.SSL_KEYFILE_PASSWORD
        
        print(f"  SSL: Enabled")
        print(f"  Certificate: {settings.SSL_CERTFILE}")
    else:
        print(f"  SSL: Disabled")
    
    try:
        uvicorn.run(
            "main:app",
            host=host, 
            port=port, 
            reload=reload,
            **ssl_params
        )
    except Exception as e:
        print(f"Помилка запуску: {e}")
        if ssl_params:
            print("Спробуйте запустити через командний рядок з SSL:")
            print(f"uvicorn main:app --ssl-keyfile {ssl_params.get('ssl_keyfile')} --ssl-certfile {ssl_params.get('ssl_certfile')} --reload")
        else:
            print("Спробуйте запустити через командний рядок:")
            print("uvicorn main:app --reload")