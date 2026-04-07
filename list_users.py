from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import db  # this should match your db.py file

# Connect to the same database your app uses
engine = create_engine("sqlite:///hotel_labor.db")
Session = sessionmaker(bind=engine)
session = Session()

# Fetch all users
users = session.query(db.User).all()

if not users:
    print("No users found in the database.")
else:
    for user in users:
        print(f"ID: {user.id}, Username: {user.username}, Role: {user.role}, Hotel: {user.hotel_name}")