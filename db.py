"""
db.py  ──────────────────────────────────────────────────────────
Central database + ORM layer for the Hotel-Labor-Tool project.
SQLite for development; swap the connection-string in ENGINE
for Postgres/MySQL when you’re ready for production.
"""

# Global dict to hold hotel context
current_hotel_context = {"hotel_name": None}
from datetime import datetime
from sqlalchemy import (
    create_engine, Column, Integer, Float, String,
    Date, ForeignKey, UniqueConstraint, func, text, Time, Boolean
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import (
    sessionmaker, relationship, scoped_session,
    Query, Mapper
)
from sqlalchemy import event, inspect

# ────────────── ENGINE / BASE / QUERY ──────────────
ENGINE = create_engine("sqlite:///hotel_labor.db", echo=False)

# Custom query class to auto-filter by hotel_name
class HotelScopedQuery(Query):
    def _with_current_hotel(self):
        hotel_name = current_hotel_context.get("hotel_name")
        if not hotel_name:
            return self

        for desc in self.column_descriptions:
            model = desc.get("entity", None)
            if model and hasattr(model, "hotel_name"):
                self = self.enable_assertions(False).filter(model.hotel_name == hotel_name)
        return self

    def __iter__(self):
        return super(HotelScopedQuery, self._with_current_hotel()).__iter__()

    def from_self(self, *ent):
        return super(HotelScopedQuery, self._with_current_hotel()).from_self(*ent)

    def count(self):
        return super(HotelScopedQuery, self._with_current_hotel()).count()

    def first(self):
        return super(HotelScopedQuery, self._with_current_hotel()).first()

    def all(self):
        return super(HotelScopedQuery, self._with_current_hotel()).all()

    def one(self):
        return super(HotelScopedQuery, self._with_current_hotel()).one()

Session = scoped_session(sessionmaker(bind=ENGINE, query_cls=HotelScopedQuery))
Base = declarative_base()

# ────────────── HOTEL SCOPING ──────────────
class HotelScoped:
    hotel_name = Column(String, nullable=False)

# Automatically inject hotel_name before insert
def set_hotel_name(mapper: Mapper, connection, target):
    if isinstance(target, HotelScoped) and not target.hotel_name:
        target.hotel_name = current_hotel_context.get("hotel_name")

# Inject hotel_name before flush (for bulk)
@event.listens_for(Session, "before_flush")
def inject_hotel_name(session, flush_context, instances):
    hotel_name = current_hotel_context.get("hotel_name")
    if not hotel_name:
        return
    for obj in session.new:
        if hasattr(obj, "hotel_name") and getattr(obj, "hotel_name") is None:
            setattr(obj, "hotel_name", hotel_name)

# ────────────── MODELS ──────────────
class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    username = Column(String, unique=True, nullable=False)
    email = Column(String, unique=True, nullable=True)
    hashed_password = Column(String, nullable=False)
    hotel_name = Column(String, nullable=False)
    role = Column(String, default="manager")

    reset_token = Column(String, nullable=True)
    reset_token_expires = Column(String, nullable=True)


class Department(Base, HotelScoped):
    __tablename__ = "departments"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)

class Position(Base, HotelScoped):
    __tablename__ = "positions"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    department_id = Column(Integer, ForeignKey("departments.id"))
    __table_args__ = (UniqueConstraint("name", "department_id", name="uix_pos_dept"),)

class Employee(Base, HotelScoped):
    __tablename__ = "employee"
    __table_args__ = {'extend_existing': True}
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    role = Column(String, nullable=False)
    department = Column(String, nullable=False)
    hourly_rate = Column(Float, nullable=False)
    emp_type = Column(String(15), nullable=False, default="import")

class Schedule(Base, HotelScoped):
    __tablename__ = "schedule"
    id = Column(Integer, primary_key=True)
    emp_id = Column(Integer, ForeignKey("employee.id"))
    day = Column(Date, nullable=False)
    shift_type = Column(String(20), nullable=False)
    __table_args__ = (UniqueConstraint("emp_id", "day", name="uix_emp_day"),)

class PositionShift(Base, HotelScoped):
    __tablename__ = "position_shift"
    id = Column(Integer, primary_key=True)
    department = Column(String)
    position = Column(String)
    shift_period = Column(String)
    shift_time = Column(String)

class ShiftTime(Base, HotelScoped):
    __tablename__ = "shift_times"
    id = Column(Integer, primary_key=True)
    position_id = Column(Integer, ForeignKey("positions.id"))
    period = Column(String)
    start = Column(Time)
    end = Column(Time)

class RoomActual(Base, HotelScoped):
    __tablename__ = "room_actual"
    id = Column(Integer, primary_key=True)
    kpi = Column(String)
    date = Column(Date)
    value = Column(Integer)

class RoomForecast(Base, HotelScoped):
    __tablename__ = "room_forecast"
    id = Column(Integer, primary_key=True)
    kpi = Column(String)
    date = Column(Date)
    value = Column(Integer)

class ScheduleAvailability(Base, HotelScoped):
    __tablename__ = "schedule_availability"
    id = Column(Integer, primary_key=True)
    emp_id = Column(Integer, ForeignKey("employee.id"))
    weekday = Column(String, nullable=False)
    availability = Column(String, nullable=False)
    employee = relationship("Employee", backref="availabilities")

class LaborStandard(Base, HotelScoped):
    __tablename__ = "labor_standards"
    id = Column(Integer, primary_key=True)
    position_id = Column(Integer, ForeignKey("positions.id"))
    metric = Column(String)
    standard = Column(Float)
    unit = Column(String, default="per FTE")

class RoomOTBPickup(Base, HotelScoped):
    __tablename__ = "room_otb_pickup"
    id = Column(Integer, primary_key=True)
    date = Column(Date)
    kpi = Column(String)
    value = Column(Integer, default=0)

class OTBShift(Base, HotelScoped):
    __tablename__ = "otb_shift"
    id = Column(Integer, primary_key=True)
    position_id = Column(Integer)
    date = Column(Date)
    hours = Column(Float)

class PlanningSummary(Base, HotelScoped):
    __tablename__ = "planning_summary"
    id = Column(Integer, primary_key=True)
    position = Column(String)
    date = Column(Date)
    scheduled_hours = Column(Float)
    fte = Column(Float)

class ProjectedHours(Base, HotelScoped):
    __tablename__ = "projected_hours"
    id = Column(Integer, primary_key=True)
    position = Column(String)
    date = Column(Date)
    otb_hours = Column(Float)
    fte = Column(Float)

class OTBHours(Base, HotelScoped):
    __tablename__ = "otb_hours"
    id = Column(Integer, primary_key=True)
    position = Column(String)
    date = Column(Date)
    otb_hours = Column(Float)
    fte = Column(Float)

class Actual(Base, HotelScoped):
    __tablename__ = "actual"
    id = Column(Integer, primary_key=True)
    emp_id = Column(Integer, ForeignKey("employee.id"))
    position_id = Column(Integer, ForeignKey("positions.id"))
    date = Column(Date, nullable=False)
    hours = Column(Float, default=0.0)
    ot_hours = Column(Float, default=0.0)
    reg_pay = Column(Float, default=0.0)
    ot_pay = Column(Float, default=0.0)
    source = Column(String, default="manual")

class Rooms(Base, HotelScoped):
    __tablename__ = "rooms"
    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False, unique=True)
    occupied = Column(Integer, nullable=False)

class UserAccessControl(Base):
    __tablename__ = "user_access_control"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    department = Column(String)
    position = Column(String)
    can_view_hourly_rate = Column(Boolean, default=False)

    user = relationship("User", back_populates="access_controls")


User.access_controls = relationship("UserAccessControl", back_populates="user", cascade="all, delete-orphan")
# ---------- RoomKPI model ----------
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text, UniqueConstraint, func

class RoomKPI(HotelScoped, Base):  # HotelScoped already adds hotel_name
    __tablename__ = "room_kpis"

    id         = Column(Integer, primary_key=True)
    name       = Column(String(120), nullable=False)     # display name
    has_rule   = Column(Boolean, default=False, nullable=False)
    rule_expr  = Column(Text, nullable=True)             # e.g. "[A] - [B]"
    is_active  = Column(Boolean, default=True, nullable=False)
    sort_order = Column(Integer, default=100, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)

    __table_args__ = (
        # prevent duplicates per hotel
        UniqueConstraint("hotel_name", "name", name="uq_roomkpi_hotel_name"),
    )

    def __repr__(self):
        return f"<RoomKPI {self.hotel_name or '-'} | {self.name}>"

class ScheduledTask(HotelScoped, Base):
    __tablename__ = "scheduled_tasks"

    id = Column(Integer, primary_key=True)
    task_type = Column(String)         # e.g., "OT Risk"
    date_mode = Column(String)         # Yesterday / Current Week / Last Week / MTD
    frequency = Column(String)         # Daily / Weekly / Bi-Weekly
    run_time = Column(String)          # Stored as "06:00"
    emails = Column(String)            # comma-separated
    department = Column(String)
    position = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)

# ────────────── INIT ──────────────
def init_db():
    Base.metadata.create_all(ENGINE)

init_db()

if __name__ == "__main__":
    init_db()
    print("✅ hotel_labor.db initialized")