# deps.py
from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import jwt
import os

from schemas import UserClaims

SECRET_KEY = os.getenv("JWT_SECRET", "change_me_for_dev_only")
ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")

bearer = HTTPBearer()



def get_current_user(token: HTTPAuthorizationCredentials = Depends(bearer)) -> UserClaims:
    try:
        payload = jwt.decode(token.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        return UserClaims(**payload)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid or expired token")