"""
seed_production.py
Runs at startup. Creates all known users in the production database
if the users table is empty. Safe to run repeatedly — skips if users exist.
"""
import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

def _get_url():
    url = os.environ.get("DATABASE_URL", "")
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return url

USERS = [
    # (username, hashed_password, role, hotel_name, email)
    ("Admin11",  "$2b$12$KiBD6Vb/9KA6bKoCNoOg1OVLHcdfJrYjNvRiPfz2PKgZ/2E0GUjRq", "Admin",         "E11EVEN CLUB HOTEL & RESIDENCES", "jperez@11clubhotelmiami.com"),
    ("Revenue",  "$2b$12$ze7V5XjSNLk5ZgTfQZD/v.t6AFpBBaB2pb/928Ep1a1BPjQOQ4V1a", "Night Audit",   "E11EVEN CLUB HOTEL & RESIDENCES", "Test@test.com"),
    ("GSA",      "$2b$12$NdHXdv432R7zaJdGWCSffehDCbciXgGj92kt7yHEohPWbqLWvYLnC", "Employee",      "E11EVEN CLUB HOTEL & RESIDENCES", "GSA@GSA.com"),
    ("AssetMgr", "$2b$12$IcOGXJGw1CEAIdBDK5mezuqElPRcrKYgK8CMv7rOLAiTNwk4WrY22", "Asset Manager", "E11EVEN CLUB HOTEL & RESIDENCES", "AssetMgr@AssetMgr.com"),
    ("super",    "$2b$12$qfQiVaEWlzHv/WgyWW1oVuOvKUtbf7/z3OoThcC///rKs9dI6mUlG", "Super User",    "ALL",                             None),
    ("SuperUser","$2b$12$Zbi7eRj9zPbOzUIT.SPPKOitXcbGRo/vCnV/mH0WHwuFEJ9z./Clu", "Super User",    "ALL",                             None),
    ("Admin1",   "$2b$12$JXpFSHC1vfXlWXMttyVCr.EIU9bJ/hFY7nijgqRAbU0.yObYmCdo.", "Admin",         "Elser",                           None),
    ("Jesus",    "$2b$12$OVEafeIF3RWiG4KVYF9d/OIsriPFYJawnzBdJtPUs0462A9QTRp.y", "Manager",       "Elser",                           None),
]

def seed():
    url = _get_url()
    if not url:
        print("[Seed] No DATABASE_URL — skipping.")
        return

    engine = create_engine(url, pool_pre_ping=True)

    # Make sure users table exists first (db.py init_db() runs before this
    # via import, but be safe)
    try:
        with engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM users")).scalar()
    except Exception:
        print("[Seed] users table not ready yet — skipping seed.")
        return

    if count and count > 0:
        print(f"[Seed] {count} user(s) already exist — skipping seed.")
        return

    Session = sessionmaker(bind=engine)
    db = Session()
    try:
        for username, hashed_password, role, hotel_name, email in USERS:
            db.execute(
                text("""
                    INSERT INTO users (username, hashed_password, role, hotel_name, email)
                    VALUES (:username, :hashed_password, :role, :hotel_name, :email)
                    ON CONFLICT (username) DO NOTHING
                """),
                {
                    "username": username,
                    "hashed_password": hashed_password,
                    "role": role,
                    "hotel_name": hotel_name,
                    "email": email,
                }
            )
        db.commit()
        print(f"[Seed] ✅ Seeded {len(USERS)} users into production database.")
    except Exception as e:
        db.rollback()
        print(f"[Seed] ❌ Error seeding users: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    seed()
