from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):
    # MS SQL Database
    DB_SERVER: str = "localhost"
    DB_DATABASE: str = "VProEnterpriseDB"
    DB_USERNAME: str = "sa"
    DB_PASSWORD: str = "YourPassword123"
    DB_PORT: int = 1433
    DB_DRIVER: str = "ODBC Driver 17 for SQL Server"
    
    # Connection string for MS SQL
    @property
    def DATABASE_URL(self) -> str:
        return (
            f"mssql+aioodbc://{self.DB_USERNAME}:{self.DB_PASSWORD}"
            f"@{self.DB_SERVER}:{self.DB_PORT}/{self.DB_DATABASE}"
            f"?driver={self.DB_DRIVER.replace(' ', '+')}"
        )
    
    # Server Configuration
    HOST: str = "127.0.0.1"
    PORT: int = 8000
    DEBUG: bool = True
    
    # SSL Configuration - ДОДАЙТЕ ЦІ ПОЛЯ
    USE_SSL: bool = False
    SSL_KEYFILE: Optional[str] = None
    SSL_CERTFILE: Optional[str] = None
    SSL_KEYFILE_PASSWORD: Optional[str] = None
    
    CURRENT_LANGUAGE: str = "en"

    # App Configuration
    APP_NAME: str = "VProEnterpriseServer"
    VERSION: str = "1.0.0"
    
    # JWT Configuration
    JWT_SECRET_KEY: str = "your-super-secret-jwt-key-change-in-production"
    JWT_ALGORITHM: str = "HS256"
    JWT_ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    JWT_REFRESH_TOKEN_EXPIRE_DAYS: int = 7

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()
