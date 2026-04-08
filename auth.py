from fastapi import APIRouter, Depends, HTTPException, status, Request, Query
from sqlalchemy.orm import Session
import bcrypt as _bcrypt
import os
from jose import jwt, JWTError
from datetime import datetime, timedelta
from database import SessionLocal
from db import User, Employee, UserAccessControl
from schemas import UserCreate, UserLogin, UserResponse
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel
import secrets
from datetime import datetime, timedelta
import smtplib
from email.message import EmailMessage
from email_sender import send_email

# ────────────────
#  JWT CONFIG
# ────────────────
SECRET_KEY = "your-secret-key"  # Replace with secure key
ALGORITHM = "HS256"
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

router = APIRouter()

# ────────────────
#  EMAIL RESET CONFIG
# ────────────────


SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USERNAME = "your_email@gmail.com"
SMTP_PASSWORD = "your_app_password"

# Build the reset URL base from the Replit public domain (or localhost fallback)
_replit_domain = os.environ.get("REPLIT_DEV_DOMAIN", "")
if _replit_domain:
    RESET_URL_BASE = f"https://{_replit_domain}/"
else:
    RESET_URL_BASE = "http://localhost:5000/"

# ────────────────
#  DB SESSION
# ────────────────
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ────────────────
#  HOTEL SCOPING HELPER
# ────────────────
def filter_by_hotel(query, user):
    # Allow Super Users to see all hotels
    if user.role == "Super User":
        return query  # No filtering
    # Managers/Admins restricted to their hotel
    return query.filter_by(hotel_name=user.hotel_name)


def has_role(user, allowed: list[str]) -> bool:
    r = (user.role or "").strip().lower()
    return r in [a.strip().lower() for a in allowed]
# ────────────────
#  SIGNUP
# ────────────────
@router.post("/signup", response_model=UserResponse)
async def signup(user: UserCreate, request: Request, db: Session = Depends(get_db)):
    existing = db.query(User).filter(User.username == user.username).first()
    if existing:
        raise HTTPException(status_code=400, detail="Username already exists")

    if user.email:
        email_exists = db.query(User).filter(User.email == user.email).first()
        if email_exists:
            raise HTTPException(status_code=400, detail="Email already exists")

    hashed = _bcrypt.hashpw(user.password.encode('utf-8'), _bcrypt.gensalt()).decode('utf-8')

    role = user.role or "manager"
    hotel_name = user.hotel_name if role != "Super User" else ""

    new_user = User(
        username=user.username,
        email=user.email,
        hashed_password=hashed,
        hotel_name=hotel_name,
        role=role
    )
    db.add(new_user)
    db.commit()
    db.refresh(new_user)

    # 👇 Read raw JSON to grab access_control (we didn't change your schema)
    try:
        body = await request.json()
    except Exception:
        body = {}

    access_list = body.get("access_control") or []

    # Insert rows if provided (works with your existing UserAccessControl model)
    for ac in access_list:
        record = UserAccessControl(
            user_id=new_user.id,
            department=ac.get("department"),
            position=ac.get("position"),
            can_view_hourly_rate=bool(ac.get("can_view_hourly_rate", False))
        )
        db.add(record)

    if access_list:
        db.commit()

    return new_user

# ────────────────
#  LOGIN
# ────────────────
from sqlalchemy import or_

@router.post("/login")
def login(user: UserLogin, db: Session = Depends(get_db)):
    db_user = db.query(User).filter(
        or_(User.username == user.username, User.email == user.username)
    ).first()

    if not db_user or not _bcrypt.checkpw(user.password.encode('utf-8'), db_user.hashed_password.encode('utf-8')):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    
    token = jwt.encode(
        {
            "sub": db_user.username,
            "exp": datetime.utcnow() + timedelta(hours=12)
        },
        SECRET_KEY,
        algorithm=ALGORITHM
    )
    return {"access_token": token, "token_type": "bearer"}

# ────────────────
#  GET CURRENT USER FROM JWT
# ────────────────
def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise HTTPException(status_code=401, detail="Invalid token")
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

    user = db.query(User).filter(User.username == username).first()
    if user is None:
        raise HTTPException(status_code=401, detail="User not found")

    return user

# ────────────────
#  ME ENDPOINT
# ────────────────
@router.get("/me", response_model=UserResponse)
def read_current_user(current_user: User = Depends(get_current_user)):
    return current_user

# ────────────────
#  EMPLOYEES (GET)
# ────────────────
@router.get("/employees")
def get_employees(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    employees = filter_by_hotel(db.query(Employee), current_user).all()
    return employees

# ────────────────
#  EMPLOYEE CREATE (POST)
# ────────────────
class EmployeeCreate(BaseModel):
    name: str
    role: str
    department: str
    hourly_rate: float
    emp_type: str = "import"

@router.post("/employees")
def create_employee(
    emp: EmployeeCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    new_emp = Employee(
        name=emp.name,
        role=emp.role,
        department=emp.department,
        hourly_rate=emp.hourly_rate,
        emp_type=emp.emp_type,
        hotel_name=current_user.hotel_name  # ✅ Hotel-specific scope
    )
    db.add(new_emp)
    db.commit()
    db.refresh(new_emp)
    return new_emp
    return employees

# ────────────────
#  LIST ALL LOGIN USERS (Super User only)
# ────────────────
@router.get("/login-users")
def list_login_users(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    # Only Super Users can see all login users
    if current_user.role != "Super User":
        raise HTTPException(status_code=403, detail="Not authorized")

    # Query all users who have login credentials (User table)
    all_users = db.query(User).all()

    return [
        {
            "id": user.id,
            "username": user.username,
            "role": user.role,
            "hotel_name": "All Hotels" if user.role == "Super User" else user.hotel_name
        }
        for user in all_users
    ]

# ────────────────
#  DELETE ADMIN (TEMPORARY)
# ────────────────
@router.delete("/delete-admin")
def delete_admin(db: Session = Depends(get_db)):
    admin_user = db.query(User).filter_by(username="admin").first()
    if admin_user:
        db.delete(admin_user)
        db.commit()
        return {"message": "Admin deleted."}
    return {"message": "Admin not found."}

# ────────────────
#  DEV ONLY: LIST USERS WITHOUT LOGIN (INSECURE)
# ────────────────
@router.get("/dev-users")
def list_users_without_login(db: Session = Depends(get_db)):
    users = db.query(User).all()
    return [
        {
            "id": user.id,
            "username": user.username,
            "role": user.role,
            "hotel_name": user.hotel_name
        }
        for user in users
    ]

@router.get("/users", tags=["Dev"])
def list_users_unauthenticated(db: Session = Depends(get_db)):
    users = db.query(User).all()
    return [
        {
            "id": u.id,
            "username": u.username,
            "email": u.email,
            "role": u.role,
            "hotel_name": u.hotel_name
        }
        for u in users
    ]

@router.get("/departments", tags=["admin"])
def get_departments(db: Session = Depends(get_db), user=Depends(get_current_user)):
    hotel_name = user.hotel_name
    departments = (
        db.query(Employee.department)
        .filter(Employee.hotel_name == hotel_name)
        .distinct()
        .all()
    )
    return [d[0] for d in departments]

@router.get("/positions", tags=["admin"])
def get_positions(department: str, db: Session = Depends(get_db), user=Depends(get_current_user)):
    hotel_name = user.hotel_name
    positions = (
        db.query(Employee.role)
        .filter(Employee.department == department, Employee.hotel_name == hotel_name)
        .distinct()
        .all()
    )
    return [p[0] for p in positions]

@router.get("/users/{username}/access", tags=["admin"])
def read_user_access(username: str,
                     current_user: User = Depends(get_current_user),
                     db: Session = Depends(get_db)):

    # Allow Admin or Super User to inspect access
    if not has_role(current_user, ["Admin", "Super User"]):
        raise HTTPException(status_code=403, detail="Access denied")

    u = db.query(User).filter(User.username == username).first()
    if not u:
        raise HTTPException(status_code=404, detail="User not found")

    rows = db.query(UserAccessControl).filter(UserAccessControl.user_id == u.id).all()
    return [
        {
            "username": u.username,
            "hotel_name": u.hotel_name,
            "department": r.department,
            "position": r.position,
            "can_view_hourly_rate": r.can_view_hourly_rate,
        }
        for r in rows
    ]

@router.get("/my/access")
def my_access(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    rows = db.query(UserAccessControl).filter(UserAccessControl.user_id == current_user.id).all()
    return [
        {
            "department": r.department,
            "position": r.position,
            "can_view_hourly_rate": r.can_view_hourly_rate,
        }
        for r in rows
    ]

@router.delete("/users/{username}")
def delete_user(username: str,
                current_user: User = Depends(get_current_user),
                db: Session = Depends(get_db)):

    # Only Admins and Super Users can delete users
    if not has_role(current_user, ["Admin", "Super User"]):
        raise HTTPException(status_code=403, detail="Not authorized")

    user = db.query(User).filter(User.username == username).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    # Prevent deleting self
    if user.username == current_user.username:
        raise HTTPException(status_code=400, detail="Cannot delete yourself")

    db.delete(user)
    db.commit()
    return {"detail": f"User '{username}' deleted successfully"}

@router.get("/debug/users-email")
def debug_users_email(db: Session = Depends(get_db)):
    users = db.query(User).all()
    return [
        {
            "id": u.id,
            "username": u.username,
            "email": u.email
        }
        for u in users
    ]

@router.post("/forgot-password")
def forgot_password(email: str = Query(...), db: Session = Depends(get_db)):

    user = db.query(User).filter(User.email == email).first()
    if not user:
        raise HTTPException(status_code=404, detail="No account found with that email")

    reset_token = secrets.token_urlsafe(32)
    expires_at = datetime.utcnow() + timedelta(minutes=30)

    user.reset_token = reset_token
    user.reset_token_expires = expires_at.isoformat()
    db.commit()

    reset_link = f"{RESET_URL_BASE}?token={reset_token}"

    email_body = f"""
Hello {user.username},

You requested a password reset.

Click the link below to reset your password (valid for 30 minutes):

{reset_link}

If you did not request this, please ignore this email.

Thanks,
Labor Pilot
"""

    send_email(
        recipients=[email],
        subject="Password Reset | Labor Pilot",
        body=email_body,
        attachments=None
    )

    return {"detail": "Password reset email sent"}

@router.post("/reset-password")
def reset_password(token: str = Query(...), new_password: str = Query(...), db: Session = Depends(get_db)):

    user = db.query(User).filter(User.reset_token == token).first()
    if not user:
        raise HTTPException(status_code=400, detail="Invalid or expired reset token")

    if not user.reset_token_expires:
        raise HTTPException(status_code=400, detail="Invalid or expired reset token")

    expires_at = datetime.fromisoformat(user.reset_token_expires)
    if datetime.utcnow() > expires_at:
        raise HTTPException(status_code=400, detail="Reset token has expired")

    user.hashed_password = _bcrypt.hashpw(new_password.encode('utf-8'), _bcrypt.gensalt()).decode('utf-8')

    user.reset_token = None
    user.reset_token_expires = None

    db.commit()

    return {"detail": "Password reset successful"}

@router.get("/reset-user")
def get_user_from_token(token: str, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.reset_token == token).first()
    if not user:
        raise HTTPException(status_code=404, detail="Invalid token")
    return {"username": user.username}
@router.get("/my-scope")
def get_my_scope(user=Depends(get_current_user), db: Session = Depends(get_db)):
    # Return scope for managers and employees (both are department-scoped)
    if user.role.strip().lower() not in ("manager", "employee"):
        return []

    rows = db.query(UserAccessControl).filter(
        UserAccessControl.user_id == user.id
    ).all()

    return [
        {"department": r.department, "position": r.position}
        for r in rows
    ]