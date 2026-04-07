import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from passlib.hash import bcrypt

# import your models + Base
import db
from db import Base, User

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise Exception("DATABASE_URL env var not set. Paste your Neon DATABASE_URL in PowerShell first.")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def main():
    db_session = SessionLocal()

    # make sure tables exist
    Base.metadata.create_all(bind=engine)

    username = "Admin1"
    password = "Admin1"  # change this after first login

    existing = db_session.query(User).filter(User.username == username).first()
    if existing:
        print("✅ Admin1 already exists in Neon. No changes made.")
        return

    new_user = User(
        username=username,
        password=bcrypt.hash(password),
        role="Super User",
        hotel_name="DEFAULT"
    )

    db_session.add(new_user)
    db_session.commit()
    print("✅ Admin1 created in Neon with password 'Admin1' (change it after login).")

if __name__ == "__main__":
    main()