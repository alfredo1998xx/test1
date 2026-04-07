# Hotel Labor Management Tool

A Streamlit-based web application for managing hotel labor, tracking employee hours, schedules, and generating OT risk / labor variance reports.

## Architecture

- **Frontend/App**: Streamlit (`app.py`) — all UI, data visualization, and report generation
- **Database**: SQLite (`hotel_labor.db`) via SQLAlchemy ORM
- **Auth**: JWT-based authentication (`auth.py`)
- **Reporting**: Business logic in `report_logic.py`, exports in `scheduler_report_exports.py`
- **Background Jobs**: `scheduler.py` for automated report emails

## Key Files

| File | Purpose |
|------|---------|
| `app.py` | Main Streamlit application (UI + logic) |
| `db.py` | SQLAlchemy models and database engine |
| `auth.py` | JWT authentication |
| `report_logic.py` | OT risk, forecast, productivity reports |
| `scheduler.py` | Background job scheduler |
| `email_sender.py` | SMTP email integration |
| `hotel_labor.db` | SQLite database |

## Running

The app runs via Streamlit on port 5000:

```
streamlit run app.py
```

Streamlit config is in `.streamlit/config.toml` — configured for port 5000, `0.0.0.0` host, CORS disabled for Replit proxy compatibility.

## Dependencies

Managed via `requirements.txt`. Key packages:
- `streamlit`, `streamlit-aggrid` — UI framework
- `sqlalchemy` — ORM and database access
- `pandas`, `numpy`, `plotly` — data analysis and charts
- `reportlab`, `openpyxl`, `xlsxwriter` — report exports
- `fastapi`, `pyjwt`, `passlib` — API and auth utilities
