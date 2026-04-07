# Hotel Labor Management Tool

A Streamlit-based web application for managing hotel labor, tracking employee hours, schedules, and generating OT risk / labor variance reports.

## Architecture

- **Frontend/App**: Streamlit (`app.py`) — all UI, data visualization, and report generation
- **Database**: PostgreSQL (Replit cloud database) via SQLAlchemy ORM — falls back to SQLite locally if `DATABASE_URL` is not set
- **Auth**: JWT-based authentication (`auth.py`) backed by FastAPI on port 8000
- **Reporting**: Business logic in `report_logic.py`, exports in `scheduler_report_exports.py`
- **Background Jobs**: `scheduler.py` for automated report emails

## Key Files

| File | Purpose |
|------|---------|
| `app.py` | Main Streamlit application (UI + logic) |
| `db.py` | SQLAlchemy models and database engine (uses `DATABASE_URL`) |
| `database.py` | FastAPI session factory (also uses `DATABASE_URL`) |
| `auth.py` | JWT authentication via FastAPI router |
| `report_logic.py` | OT risk, forecast, productivity reports |
| `scheduler.py` | Background job scheduler |
| `email_sender.py` | SMTP email integration |

## Running

Two workflows run simultaneously:

1. **Start application** — Streamlit frontend on port 5000: `streamlit run app.py`
2. **Backend API** — FastAPI auth service on port 8000: `uvicorn main:app --host 0.0.0.0 --port 8000`

Streamlit config is in `.streamlit/config.toml` — port 5000, `0.0.0.0` host, CORS disabled for Replit proxy.

## Database

- Uses Replit's built-in PostgreSQL (via `DATABASE_URL` secret)
- `db.py` auto-detects PostgreSQL vs SQLite based on `DATABASE_URL` env var
- `postgres://` URLs are automatically rewritten to `postgresql://` for SQLAlchemy compatibility
- All 21 tables with 21,489 rows migrated from SQLite on initial setup

## Dependencies

Managed via `requirements.txt`. Key packages:
- `streamlit==1.38.0`, `streamlit-aggrid` — UI framework (1.38.0 pinned for aggrid compatibility)
- `sqlalchemy`, `psycopg2-binary` — ORM and PostgreSQL driver
- `pandas`, `numpy`, `plotly` — data analysis and charts
- `reportlab`, `openpyxl`, `xlsxwriter` — report exports
- `fastapi`, `uvicorn`, `python-jose[cryptography]`, `bcrypt` — API and auth
