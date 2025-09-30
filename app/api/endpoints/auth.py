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
    
    user = authenticate_user(username, password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password"
        )
    
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user["username"], "role": user["role"]}, 
        expires_delta=access_token_expires
    )
    
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "expires_in": ACCESS_TOKEN_EXPIRE_MINUTES * 60,
        "user": {
            "username": user["username"],
            "role": user["role"]
        }
    }

@router.get("/me")
async def get_me(current_user: Dict = Depends(get_current_user)):
    """Отримати інформацію про поточного користувача"""
    return {
        "username": current_user["username"],
        "role": current_user["role"],
        "active": current_user["active"]
    }

@router.post("/logout")
async def logout(current_user: Dict = Depends(get_current_user)):
    """Вихід з системи (заглушка)"""
    return {"message": f"User {current_user['username']} logged out successfully"}