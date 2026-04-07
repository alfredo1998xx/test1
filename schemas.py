from pydantic import BaseModel


from typing import Optional

class UserCreate(BaseModel):
    username: str
    email: Optional[str] = None   # ✅ FIXED
    password: str
    hotel_name: str = None
    role: str = "manager"

class UserLogin(BaseModel):
    username: str
    password: str

class UserResponse(BaseModel):
    id: int
    username: str
    hotel_name: str
    role: str

    class Config:
        orm_mode = True