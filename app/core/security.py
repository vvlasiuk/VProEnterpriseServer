from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from fastapi import HTTPException, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
from app.core.config import settings

# Використовуйте налаштування з config
SECRET_KEY = settings.JWT_SECRET_KEY
ALGORITHM = settings.JWT_ALGORITHM
ACCESS_TOKEN_EXPIRE_MINUTES = settings.JWT_ACCESS_TOKEN_EXPIRE_MINUTES

# Заглушка користувачів
FAKE_USERS_DB = {
    "admin": {
        "username": "admin",
        "password": "admin123",  # В продакшн - хешовані паролі!
        "role": "admin",
        "active": True
    },
    "user": {
        "username": "user",
        "password": "user123",
        "role": "user", 
        "active": True
    }
}

security = HTTPBearer()

def create_access_token(data: Dict[str, Any], expires_delta: Optional[timedelta] = None):
    """Створення JWT токену (заглушка)"""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def verify_token(token: str) -> Dict[str, Any]:
    """Перевірка JWT токену (заглушка)"""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid authentication credentials",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return payload
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

def authenticate_user(username: str, password: str) -> Optional[Dict[str, Any]]:
    """Перевірка користувача (заглушка)"""
    user = FAKE_USERS_DB.get(username)
    if user and user["password"] == password and user["active"]:
        return user
    return None

# ВИПРАВЛЕНА ФУНКЦІЯ
async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Отримання поточного користувача з токену"""
    token = credentials.credentials  # Правильний доступ до токену
    payload = verify_token(token)
    username = payload.get("sub")
    user = FAKE_USERS_DB.get(username)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found"
        )
    return user

def require_admin_role(current_user: Dict = None):
    """Перевірка ролі адміністратора"""
    if not current_user or current_user.get("role") != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required"
        )
    return True