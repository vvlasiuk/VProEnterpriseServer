from fastapi import APIRouter, HTTPException, status, Depends
from fastapi.security import HTTPAuthorizationCredentials
from datetime import timedelta
from typing import Dict
from app.core.security import (
    authenticate_user, 
    create_access_token, 
    get_current_user,
    ACCESS_TOKEN_EXPIRE_MINUTES
)

router = APIRouter()

@router.post("/login")
async def login(credentials: Dict[str, str]):
    """Авторизація користувача (заглушка)"""
    username = credentials.get("username")
    password = credentials.get("password")
    
    if not username or not password:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Username and password required"
        )
    
    user = await authenticate_user(username, password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password"
        )
    
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={
            "sub": user["name"],
            "user_id": user["_id"],
            "email": user["email"],
            "is_admin": user["is_admin"],
            "role": user["role"]
        }, 
        expires_delta=access_token_expires
    )
    
    return {
        "access_token": access_token,
        "token_type": "bearer", 
        "expires_in": ACCESS_TOKEN_EXPIRE_MINUTES * 60,
        "user": {
            "_id": user["_id"],
            "name": user["name"],
            "full_name": user["full_name"],
            "email": user["email"],
            "is_admin": user["is_admin"],
            "role": user["role"]
        }
    }

@router.get("/me")
async def get_me(current_user: Dict = Depends(get_current_user)):
    """Отримати інформацію про поточного користувача"""
    return {
        "_id": current_user["_id"],
        "name": current_user["name"],
        "full_name": current_user["full_name"],
        "is_admin": current_user["is_admin"],
        "is_active": current_user["is_active"]
    }

@router.post("/logout")
async def logout(current_user: Dict = Depends(get_current_user)):
    """Вихід з системи (заглушка)"""
    return {"message": f"User {current_user['name']} logged out successfully"}