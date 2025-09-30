import sys
import os
import logging
from contextlib import asynccontextmanager

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/app.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Виправлення шляхів для PyInstaller
if getattr(sys, 'frozen', False):
    # Якщо запущено як executable
    application_path = os.path.dirname(sys.executable)
    # Для onefile версії
    if hasattr(sys, '_MEIPASS'):
        bundle_path = sys._MEIPASS
    else:
        bundle_path = application_path
else:
    # Якщо запущено як скрипт Python
    application_path = os.path.dirname(os.path.abspath(__file__))
    bundle_path = application_path

# Встановлюємо робочу директорію
os.chdir(application_path)

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.middleware.gzip import GZipMiddleware
import time

from app.api import api_router
from app.core.app_globals import get_localizer, get_settings

# Отримуємо налаштування
settings = get_settings()
localizer = get_localizer()

# Lifecycle events
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info(f"Starting {settings.APP_NAME} v{settings.VERSION}")
    logger.info(f"Language: {settings.CURRENT_LANGUAGE}")
    logger.info(f"Debug mode: {settings.DEBUG}")
    yield
    # Shutdown
    logger.info("Shutting down server")

# FastAPI app
app = FastAPI(
    title=settings.APP_NAME,
    description="VPro: Enterprise Server",
    version=settings.VERSION,
    debug=settings.DEBUG,
    lifespan=lifespan,
    docs_url="/docs" if settings.DEBUG else None,  # Disable docs in production
    redoc_url="/redoc" if settings.DEBUG else None
)

# Middleware
app.add_middleware(GZipMiddleware, minimum_size=1000)

# CORS (налаштуйте для ваших доменів)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "https://yourdomain.com"] if not settings.DEBUG else ["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

# Security
if not settings.DEBUG:
    app.add_middleware(
        TrustedHostMiddleware, 
        allowed_hosts=["yourdomain.com", "*.yourdomain.com"]
    )

# Request logging middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    
    logger.info(
        f"{request.client.host} - {request.method} {request.url.path} - "
        f"{response.status_code} - {process_time:.4f}s"
    )
    return response

# Підключення API роутів
app.include_router(api_router, prefix="/api/v1")

@app.get("/")
async def root():
    """Root endpoint - API status and information"""
    response = {
        "service": settings.APP_NAME,
        "version": settings.VERSION,
        "status": localizer.t("common.service_running"),
        "language": settings.CURRENT_LANGUAGE
    }
    
    if settings.DEBUG:
        response.update({
            "environment": localizer.t("common.development"),
            "database": f"MS SQL Server ({settings.DB_SERVER}:{settings.DB_PORT})",
            "protocol": "HTTPS" if settings.USE_SSL else "HTTP",
            "docs": "/docs",
            "redoc": "/redoc",
            "available_languages": localizer.get_available_languages(),
            "debug_mode": True
        })
    else:
        response.update({
            "environment": localizer.t("common.production"),
            "contact": "vlaspro.com.ua@gmail.com"
        })
    
    return response

@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    """Favicon endpoint"""
    favicon_path = os.path.join(bundle_path, "app", "static", "favicon.ico")
    if os.path.exists(favicon_path):
        return FileResponse(favicon_path, media_type="image/x-icon")
    else:
        from fastapi import Response
        return Response(status_code=204)

if __name__ == "__main__":
    from app.core.server_setup import start_server
    start_server()