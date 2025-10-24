from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from fastapi import HTTPException, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
from app.core.config import settings
from app.services.database_service import DatabaseService

# Використовуйте налаштування з config
SECRET_KEY = settings.JWT_SECRET_KEY
ALGORITHM = settings.JWT_ALGORITHM
ACCESS_TOKEN_EXPIRE_MINUTES = settings.JWT_ACCESS_TOKEN_EXPIRE_MINUTES

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

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Перевірити пароль"""
    try:
        import bcrypt
        return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password.encode('utf-8'))
    except ImportError:
        # Fallback на простий хеш якщо bcrypt не встановлений
        import hashlib
        return hashlib.sha256(plain_password.encode()).hexdigest() == hashed_password

def hash_password(password: str) -> str:
    """Захешувати пароль"""
    try:
        import bcrypt
        salt = bcrypt.gensalt()
        hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
        return hashed.decode('utf-8')
    except ImportError:
        # Fallback на простий хеш якщо bcrypt не встановлений
        import hashlib
        return hashlib.sha256(password.encode()).hexdigest()

# Замінити authenticate_user на БД
async def authenticate_user(username: str, password: str) -> Optional[Dict[str, Any]]:
    """Check user in database"""
    query = """
    SELECT _id, name, full_name,email, password_hash, is_admin, is_active
    FROM cat_users
    WHERE (name = ? OR email = ?) AND is_active = 1
    """
    users = await DatabaseService.execute_query(query, (username, username))
    if not users:
        return None
    user = users[0]
    if verify_password(password, user["password_hash"]):
        return {
            "_id": user["_id"],
            "name": user["name"],
            "full_name": user["full_name"],
            "email": user["email"],
            "is_admin": user["is_admin"],
            "role": "admin" if user["is_admin"] else "user"
        }
    return None

# ВИПРАВЛЕНА ФУНКЦІЯ
async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Get current user from DB"""
    token = credentials.credentials
    payload = verify_token(token)
    user_id = payload.get("user_id")
    query = "SELECT * FROM cat_users WHERE _id = ? AND is_active = 1"
    users = await DatabaseService.execute_query(query, (user_id,))
    if not users:
        raise HTTPException(status_code=401, detail="User not found")
    return users[0]

def require_admin_role(current_user: Dict = None):
    """Перевірка ролі адміністратора"""
    if not current_user or current_user.get("role") != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required"
        )
    return True