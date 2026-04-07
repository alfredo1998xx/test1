import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def _get_database_url():
    url = os.environ.get("DATABASE_URL", "sqlite:///hotel_labor.db")
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return url

DATABASE_URL = _get_database_url()
_engine_kwargs = {} if DATABASE_URL.startswith("postgresql") else {"connect_args": {"check_same_thread": False}}
engine = create_engine(DATABASE_URL, **_engine_kwargs)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
