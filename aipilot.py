"""
aipilot.py — AI-powered labor intelligence for LaborPilot
Full database awareness: employees, schedules, positions, costs, OT, mockups.
"""

import os
import re
import io
import base64
import calendar
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date, timedelta, datetime
from groq import Groq
from sqlalchemy import text
from db import ENGINE
from email_sender import send_email


# ── Logo helper ───────────────────────────────────────────────────────────────
def _get_logo_b64() -> str:
    logo_path = os.path.join(os.path.dirname(__file__),
                             "attached_assets", "laborpilot_logo_nobg.png")
    if os.path.exists(logo_path):
        with open(logo_path, "rb") as f:
            return base64.b64encode(f.read()).decode()
    return ""


# ── Date parser ───────────────────────────────────────────────────────────────
def parse_dates_from_question(question: str):
    q = question.lower()
    today = date.today()

    if "today" in q:
        return today, today
    if "yesterday" in q:
        d = today - timedelta(days=1)
        return d, d
    if "next week" in q:
        days_until_mon = (7 - today.weekday()) % 7 or 7
        start = today + timedelta(days=days_until_mon)
        return start, start + timedelta(days=6)
    if "this week" in q:
        start = today - timedelta(days=today.weekday())
        return start, today
    if "last week" in q:
        start = today - timedelta(days=today.weekday() + 7)
        return start, start + timedelta(days=6)
    if "last 7 days" in q or "past 7 days" in q:
        return today - timedelta(days=6), today
    if "last 14 days" in q or "past 14 days" in q or "two weeks" in q:
        return today - timedelta(days=13), today
    if "next month" in q:
        m = today.month % 12 + 1
        y = today.year + (1 if today.month == 12 else 0)
        last_day = calendar.monthrange(y, m)[1]
        return date(y, m, 1), date(y, m, last_day)
    if "this month" in q:
        return today.replace(day=1), today
    if "last month" in q:
        first_this = today.replace(day=1)
        last_prev  = first_this - timedelta(days=1)
        return last_prev.replace(day=1), last_prev
    if "last 30 days" in q or "past 30 days" in q:
        return today - timedelta(days=29), today
    if "last 60 days" in q or "past 60 days" in q:
        return today - timedelta(days=59), today
    if "last 90 days" in q or "quarter" in q:
        return today - timedelta(days=89), today
    if "this year" in q or "ytd" in q or "year to date" in q:
        return today.replace(month=1, day=1), today

    months = {
        "january": 1, "february": 2, "march": 3, "april": 4,
        "may": 5, "june": 6, "july": 7, "august": 8,
        "september": 9, "october": 10, "november": 11, "december": 12
    }
    for month_name, month_num in months.items():
        if month_name in q:
            year = today.year
            if month_num > today.month:
                year -= 1
            last_day = calendar.monthrange(year, month_num)[1]
            return date(year, month_num, 1), date(year, month_num, last_day)

    return today - timedelta(days=29), today


# ── Intent detection ──────────────────────────────────────────────────────────
def detect_intent(question: str) -> list:
    q = question.lower()
    intents = []
    if any(w in q for w in ["overtime", "ot ", "ot,", "overti"]):
        intents.append("overtime")
    if any(w in q for w in ["cost", "pay", "spend", "budget", "wage", "dollar", "expensive", "labor cost"]):
        intents.append("cost")
    if any(w in q for w in ["department", "dept", "housekeeping", "front desk", "food", "f&b", "engineering", "guest service"]):
        intents.append("department")
    if any(w in q for w in ["employee", "staff", "worker", "who", "top", "highest", "most", "team", "agent"]):
        intents.append("employee")
    if any(w in q for w in ["trend", "daily", "day by day", "over time", "pattern"]):
        intents.append("trend")
    if any(w in q for w in ["schedule", "scheduled", "shift", "roster", "mockup", "mock",
                              "next week", "plan", "coverage", "assign", "rotation",
                              "generate", "create a", "make a"]):
        intents.append("schedule_mockup")
    if any(w in q for w in ["room", "occupancy", "occ", "occupied"]):
        intents.append("rooms")
    if any(w in q for w in ["position", "role", "title", "job"]):
        intents.append("positions")
    if not intents:
        intents = ["overtime", "cost", "department", "employee", "trend"]
    return intents


# ── DB helpers ────────────────────────────────────────────────────────────────
def _query(sql: str, params: dict) -> pd.DataFrame:
    with ENGINE.connect() as conn:
        return pd.read_sql_query(text(sql), conn, params=params)


def fetch_employees(hotel: str) -> pd.DataFrame:
    return _query("""
        SELECT id, name, department, role, hourly_rate, emp_type
        FROM   employee
        WHERE  hotel_name = :hotel
        ORDER  BY department, name
    """, {"hotel": hotel})


def fetch_positions(hotel: str) -> pd.DataFrame:
    return _query("""
        SELECT p.id, p.name AS position, d.name AS department
        FROM   positions p
        JOIN   departments d ON d.id = p.department_id
        WHERE  p.hotel_name = :hotel
        ORDER  BY d.name, p.name
    """, {"hotel": hotel})


def fetch_existing_schedule(hotel: str, start: date, end: date) -> pd.DataFrame:
    return _query("""
        SELECT e.name, e.department, e.role, s.day, s.shift_type
        FROM   schedule s
        JOIN   employee e ON e.id = s.emp_id AND e.hotel_name = :hotel
        WHERE  s.hotel_name = :hotel
          AND  s.day BETWEEN :start AND :end
        ORDER  BY s.day, e.department, e.name
    """, {"hotel": hotel, "start": str(start), "end": str(end)})


def fetch_labor_data(hotel: str, start: date, end: date, intents: list) -> dict:
    p = {"hotel": hotel, "start": str(start), "end": str(end)}
    result = {}

    result["totals"] = _query("""
        SELECT COALESCE(SUM(hours), 0)    AS total_hours,
               COALESCE(SUM(ot_hours), 0) AS total_ot_hours,
               COALESCE(SUM(reg_pay), 0)  AS total_reg_pay,
               COALESCE(SUM(ot_pay), 0)   AS total_ot_pay,
               COUNT(DISTINCT emp_id)      AS unique_employees,
               COUNT(DISTINCT date)        AS active_days
        FROM   actual
        WHERE  hotel_name = :hotel AND date BETWEEN :start AND :end
    """, p)

    result["by_dept"] = _query("""
        SELECT e.department,
               COALESCE(SUM(a.hours), 0)    AS total_hours,
               COALESCE(SUM(a.ot_hours), 0) AS ot_hours,
               COALESCE(SUM(a.reg_pay), 0)  AS reg_pay,
               COALESCE(SUM(a.ot_pay), 0)   AS ot_pay,
               COUNT(DISTINCT a.emp_id)      AS employees
        FROM   actual a
        JOIN   employee e ON e.id = a.emp_id AND e.hotel_name = :hotel
        WHERE  a.hotel_name = :hotel AND a.date BETWEEN :start AND :end
        GROUP  BY e.department ORDER BY total_hours DESC
    """, p)

    result["top_employees"] = _query("""
        SELECT e.name, e.department, e.role,
               COALESCE(SUM(a.hours), 0)              AS total_hours,
               COALESCE(SUM(a.ot_hours), 0)            AS ot_hours,
               COALESCE(SUM(a.reg_pay + a.ot_pay), 0)  AS total_pay
        FROM   actual a
        JOIN   employee e ON e.id = a.emp_id AND e.hotel_name = :hotel
        WHERE  a.hotel_name = :hotel AND a.date BETWEEN :start AND :end
        GROUP  BY e.name, e.department, e.role
        ORDER  BY ot_hours DESC, total_hours DESC LIMIT 20
    """, p)

    result["daily"] = _query("""
        SELECT date,
               COALESCE(SUM(hours), 0)              AS total_hours,
               COALESCE(SUM(ot_hours), 0)            AS ot_hours,
               COALESCE(SUM(reg_pay + ot_pay), 0)    AS total_cost
        FROM   actual
        WHERE  hotel_name = :hotel AND date BETWEEN :start AND :end
        GROUP  BY date ORDER BY date
    """, p)

    return result


# ── Build AI prompt ───────────────────────────────────────────────────────────
def build_prompt(hotel: str, start: date, end: date,
                 question: str, data: dict, intents: list,
                 employees_df: pd.DataFrame, positions_df: pd.DataFrame,
                 existing_sched: pd.DataFrame) -> str:

    today = date.today()
    days  = (end - start).days + 1
    day_names = []
    for i in range(days):
        d = start + timedelta(days=i)
        day_names.append(d.strftime("%a %b %d"))

    period_label = f"{start.strftime('%b %d')} – {end.strftime('%b %d, %Y')}"

    sections = []
    if not employees_df.empty:
        sections.append("FULL EMPLOYEE ROSTER:\n" + employees_df.to_string(index=False))
    if not positions_df.empty:
        sections.append("AVAILABLE POSITIONS BY DEPARTMENT:\n" + positions_df.to_string(index=False))
    if not existing_sched.empty:
        sections.append(f"EXISTING SCHEDULE ({period_label}):\n" + existing_sched.to_string(index=False))
    else:
        sections.append(f"EXISTING SCHEDULE ({period_label}): No schedule data found — you are creating a new mockup.")

    totals = data.get("totals", pd.DataFrame())
    if not totals.empty and float(totals.iloc[0].get("total_hours", 0)) > 0:
        r = totals.iloc[0]
        th  = float(r.get("total_hours", 0))
        tot = float(r.get("total_ot_hours", 0))
        rp  = float(r.get("total_reg_pay", 0))
        op  = float(r.get("total_ot_pay", 0))
        sections.append(f"LABOR ACTUALS ({period_label}):\n"
                        f"- Total Hours: {th:,.1f}  OT Hours: {tot:,.1f} ({tot/th*100:.1f}% of total)\n"
                        f"- Regular Pay: ${rp:,.2f}  OT Pay: ${op:,.2f}  Total Cost: ${rp+op:,.2f}")

    dept_df = data.get("by_dept", pd.DataFrame())
    if not dept_df.empty:
        sections.append("DEPARTMENT BREAKDOWN:\n" + dept_df.to_string(index=False))

    emp_df = data.get("top_employees", pd.DataFrame())
    if not emp_df.empty:
        sections.append("EMPLOYEE HOURS/OT:\n" + emp_df.to_string(index=False))

    daily_df = data.get("daily", pd.DataFrame())
    if not daily_df.empty and len(daily_df) <= 14:
        sections.append("DAILY BREAKDOWN:\n" + daily_df.to_string(index=False))

    data_block = "\n\n".join(sections) if sections else "No data found — roster may be empty."

    is_schedule = "schedule_mockup" in intents

    # Detect department filter from question
    dept_filter = ""
    q_low = question.lower()
    for kw in ["housekeeping", "front desk", "food & beverage", "f&b", "engineering",
                "guest service", "maintenance", "security", "spa", "finance"]:
        if kw in q_low:
            dept_filter = kw.title()
            break

    if is_schedule:
        col_header = " | ".join(day_names)
        col_csv    = ",".join(day_names)
        dept_note  = f" Focus ONLY on the {dept_filter} department." if dept_filter else " Include all departments or the ones relevant to the question."
        schedule_instruction = f"""
CRITICAL — THE USER WANTS AN ACTUAL SCHEDULE TABLE, NOT A SUMMARY.{dept_note}

YOUR RESPONSE MUST FOLLOW THIS EXACT FORMAT:

1. One opening sentence (plain text, no headers).

<<<TABLE_START>>>
Employee Name,Department,Role,{col_csv}
John Smith,Housekeeping,Room Attendant,AM,OFF,PM,AM,RDO,OFF,AM
... (one row per employee — use real names from the FULL EMPLOYEE ROSTER)
<<<TABLE_END>>>

2. A 2–3 sentence staffing note for leadership after the table.

SHIFT CODES: AM=Morning | PM=Afternoon | MID=Mid-shift | OFF=Day Off | RDO=Regular Day Off | NT=Night
RULES:
- Every employee must appear in the table.
- Minimum 2 days off per employee per 7-day week.
- Do NOT write explanations inside the table — only shift codes.
- The table must be valid CSV (no extra commas, consistent columns).
- If filtered by department, only include employees from that department.
"""
    else:
        schedule_instruction = ""

    return f"""You are the most capable hotel labor analytics AI available.
You have FULL access to the hotel's live database. Answer EXACTLY what was asked using real names and real numbers.

HOTEL: {hotel}
PERIOD: {period_label}
USER'S QUESTION: "{question}"
{schedule_instruction}

=== LIVE HOTEL DATA ===
{data_block}

=== RULES (non-schedule questions) ===
- Lead with the most important number in the first sentence.
- Give 2-3 numbered action points with real numbers from the data.
- End with one "Bottom Line" sentence — the single most important thing to act on.
- Under 250 words. No markdown headers."""


# ── Parse schedule table from AI response ────────────────────────────────────
def extract_schedule_table(text: str):
    """Extract CSV table between <<<TABLE_START>>> and <<<TABLE_END>>>"""
    pattern = r"<<<TABLE_START>>>(.*?)<<<TABLE_END>>>"
    match = re.search(pattern, text, re.DOTALL)
    if not match:
        return None, text

    csv_block = match.group(1).strip()
    summary   = text[:match.start()].strip() + "\n\n" + text[match.end():].strip()
    summary   = summary.strip()

    try:
        df = pd.read_csv(io.StringIO(csv_block))
        df.columns = [c.strip() for c in df.columns]
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].str.strip()
        return df, summary
    except Exception:
        return None, text


# ── Groq call ─────────────────────────────────────────────────────────────────
def call_groq(prompt: str) -> str:
    api_key = os.environ.get("GROQ_API_KEY", "")
    if not api_key:
        return "GROQ_API_KEY not configured. Please add it in Secrets."
    client = Groq(api_key=api_key)
    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.4,
        max_tokens=1200,
    )
    return response.choices[0].message.content.strip()


# ── Charts ────────────────────────────────────────────────────────────────────
def render_charts(data: dict, intents: list):
    dept_df  = data.get("by_dept", pd.DataFrame())
    daily_df = data.get("daily", pd.DataFrame())
    emp_df   = data.get("top_employees", pd.DataFrame())
    cols = st.columns(2)
    c = 0
    B = "#3D52A0"; BL = "#2196F3"; OT = "#FF5722"
    base = dict(template="plotly_white",
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                margin=dict(t=40, b=30, l=10, r=10),
                title_font=dict(size=13, color="#3D52A0"), font=dict(color="#444", size=11))

    if not dept_df.empty:
        with cols[c % 2]:
            fig = go.Figure()
            fig.add_trace(go.Bar(x=dept_df["department"], y=dept_df["total_hours"],
                                 name="Reg Hours", marker_color=B, opacity=0.85))
            fig.add_trace(go.Bar(x=dept_df["department"], y=dept_df["ot_hours"],
                                 name="OT Hours", marker_color=OT, opacity=0.9))
            fig.update_layout(**base, title="Hours by Department", barmode="stack",
                              legend=dict(orientation="h", y=1.12), xaxis_tickangle=-25)
            st.plotly_chart(fig, use_container_width=True)
        c += 1

        if "cost" in intents:
            with cols[c % 2]:
                fig2 = go.Figure()
                fig2.add_trace(go.Bar(x=dept_df["department"], y=dept_df["reg_pay"],
                                      name="Regular Pay", marker_color=BL, opacity=0.85))
                fig2.add_trace(go.Bar(x=dept_df["department"], y=dept_df["ot_pay"],
                                      name="OT Pay", marker_color=OT, opacity=0.9))
                fig2.update_layout(**base, title="Labor Cost by Department", barmode="stack",
                                   legend=dict(orientation="h", y=1.12), xaxis_tickangle=-25)
                st.plotly_chart(fig2, use_container_width=True)
            c += 1

    if not daily_df.empty:
        with cols[c % 2]:
            fig3 = go.Figure()
            fig3.add_trace(go.Scatter(x=daily_df["date"], y=daily_df["total_hours"],
                                      mode="lines+markers", name="Total Hours",
                                      line=dict(color=B, width=2.5),
                                      fill="tozeroy", fillcolor="rgba(61,82,160,0.07)"))
            fig3.add_trace(go.Scatter(x=daily_df["date"], y=daily_df["ot_hours"],
                                      mode="lines+markers", name="OT Hours",
                                      line=dict(color=OT, width=2, dash="dot")))
            fig3.update_layout(**base, title="Daily Hours Trend",
                               legend=dict(orientation="h", y=1.12))
            st.plotly_chart(fig3, use_container_width=True)
        c += 1

    ot_emp = emp_df[emp_df["ot_hours"] > 0].head(8) if not emp_df.empty else pd.DataFrame()
    if not ot_emp.empty:
        with cols[c % 2]:
            fig4 = go.Figure(go.Bar(
                x=ot_emp["ot_hours"], y=ot_emp["name"], orientation="h",
                marker=dict(color=ot_emp["ot_hours"],
                            colorscale=[[0, B], [1, OT]], showscale=False),
                text=ot_emp["ot_hours"].apply(lambda x: f"{x:.1f}h"), textposition="inside",
            ))
            fig4.update_layout(**base, title="Top OT Employees",
                               yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig4, use_container_width=True)
        c += 1


# ── Email helper ──────────────────────────────────────────────────────────────
def send_ai_report(recipients: list, hotel: str, question: str,
                   summary: str, period_label: str, data: dict,
                   schedule_df: pd.DataFrame | None):
    dept_df = data.get("by_dept", pd.DataFrame())
    emp_df  = data.get("top_employees", pd.DataFrame())
    totals  = data.get("totals", pd.DataFrame())
    t = totals.iloc[0] if not totals.empty else {}
    total_hours = float(t.get("total_hours", 0))
    total_ot    = float(t.get("total_ot_hours", 0))
    reg_pay     = float(t.get("total_reg_pay", 0))
    ot_pay      = float(t.get("total_ot_pay", 0))

    lines = [
        f"AIPilot Labor Report — {hotel}",
        f"Period: {period_label}",
        "=" * 60, "",
        f"QUESTION: {question}", "",
        "AI SUMMARY:", summary, "",
    ]

    if schedule_df is not None:
        lines += ["SCHEDULE MOCKUP:", schedule_df.to_string(index=False), ""]

    if total_hours > 0:
        lines += [
            "KEY METRICS:",
            f"  Total Hours : {total_hours:,.1f}",
            f"  OT Hours    : {total_ot:,.1f}",
            f"  Total Cost  : ${reg_pay + ot_pay:,.2f}",
            f"  OT Pay      : ${ot_pay:,.2f}", ""
        ]

    if not dept_df.empty:
        lines += ["DEPARTMENT BREAKDOWN:", dept_df.to_string(index=False), ""]

    if not emp_df.empty:
        lines += ["TOP EMPLOYEES (OT):", emp_df.head(10).to_string(index=False), ""]

    lines.append(f"— Sent from LaborPilot AIPilot · {date.today().strftime('%B %d, %Y')}")
    body    = "\n".join(lines)
    subject = f"AIPilot Report — {hotel} | {period_label}"
    send_email(recipients, subject, body)


# ── Main render ───────────────────────────────────────────────────────────────
def render_aipilot(hotel: str):

    # ── Session state ──
    if "ai_pending" not in st.session_state:
        st.session_state.ai_pending = ""
    if "ai_result" not in st.session_state:
        st.session_state.ai_result = None

    LOGO_B64 = _get_logo_b64()

    # ── CSS ──
    st.markdown("""
    <style>
    .ai-hero {
        background: linear-gradient(135deg, #3D52A0 0%, #5C6FBF 60%, #8697C4 100%);
        border-radius: 14px; padding: 26px 32px 22px; margin-bottom: 20px;
        box-shadow: 0 4px 20px rgba(61,82,160,0.22);
    }
    .ai-hero-top { display:flex; justify-content:space-between; align-items:flex-start; }
    .ai-logo-row { display:flex; align-items:center; gap:12px; margin-bottom:6px; }
    .ai-logo-row img { height:38px; width:auto; filter: brightness(0) invert(1); }
    .ai-title { font-size:26px; font-weight:900; color:#ffffff; letter-spacing:-0.5px; line-height:1.1; }
    .ai-subtitle { color:rgba(255,255,255,0.78); font-size:13px; margin-top:2px; }
    .ai-live-badge {
        display:inline-flex; align-items:center; gap:5px;
        background:rgba(255,255,255,0.15); border:1px solid rgba(255,255,255,0.3);
        border-radius:20px; padding:3px 10px; font-size:11px; color:#ffffff; font-weight:600;
    }
    .ai-live-dot {
        width:6px; height:6px; border-radius:50%; background:#4CAF50;
        animation:ai-pulse 1.6s ease-in-out infinite; display:inline-block;
    }
    @keyframes ai-pulse { 0%,100%{opacity:1;transform:scale(1)} 50%{opacity:0.3;transform:scale(0.7)} }
    .ai-hero-desc { margin-top:10px; color:rgba(255,255,255,0.62); font-size:12.5px; line-height:1.6; }
    .ai-response-card {
        background:linear-gradient(135deg, #f8f9ff 0%, #ffffff 100%);
        border:1px solid #e0e4f0; border-left:4px solid #3D52A0;
        border-radius:10px; padding:20px 24px; margin:12px 0 14px 0;
        font-size:14.5px; line-height:1.8; color:#1a1a2e; white-space:pre-wrap;
        box-shadow:0 2px 12px rgba(61,82,160,0.08);
    }
    .ai-model-tag {
        display:inline-flex; align-items:center; gap:6px;
        background:linear-gradient(135deg, #3D52A0, #8697C4);
        border-radius:20px; padding:3px 12px; font-size:11px; color:#fff; font-weight:600; margin-bottom:8px;
    }
    .ai-metrics-row { display:flex; flex-wrap:wrap; gap:10px; margin:14px 0 4px 0; }
    .ai-metric { background:#f0f4ff; border:1px solid #d0d8f5; border-radius:10px; padding:10px 16px; min-width:110px; text-align:center; }
    .ai-metric .val { font-size:20px; font-weight:800; color:#3D52A0; line-height:1.2; }
    .ai-metric .val.red { color:#FF5722; }
    .ai-metric .val.teal { color:#2196F3; }
    .ai-metric .lbl { font-size:10px; color:#888; margin-top:2px; }
    .ai-period-badge {
        display:inline-flex; align-items:center; gap:5px;
        background:#eef2ff; border:1px solid #c7d2fe; border-radius:8px; padding:4px 12px;
        font-size:11px; color:#3D52A0; margin-bottom:10px;
    }
    .ai-charts-header { color:#3D52A0; font-size:12px; font-weight:700; letter-spacing:0.6px; text-transform:uppercase; margin:20px 0 6px 0; }
    .ai-charts-divider { height:1px; background:linear-gradient(90deg,#3D52A0,transparent); margin-bottom:12px; opacity:0.2; }
    .ai-table-header { color:#3D52A0; font-size:13px; font-weight:700; margin:18px 0 6px 0; }
    .ai-empty { background:#f8f9ff; border:1px dashed #c7d2fe; border-radius:12px; padding:48px 24px; text-align:center; margin-top:8px; }
    .ai-empty-icon { font-size:40px; margin-bottom:12px; }
    .ai-empty-title { color:#3D52A0; font-size:15px; font-weight:700; margin-bottom:6px; }
    .ai-empty-sub { color:#888; font-size:13px; }
    .ai-suggestion-row { display:flex; flex-wrap:wrap; gap:8px; margin-top:14px; }
    .ai-email-box { background:#f0f4ff; border:1px solid #c7d2fe; border-radius:10px; padding:16px 20px; margin-top:20px; }
    div[data-testid="stTextInput"] input {
        border:1.5px solid #c7d2fe !important; border-radius:10px !important;
        font-size:14px !important; padding:12px 16px !important;
        box-shadow:0 2px 8px rgba(61,82,160,0.05) !important;
    }
    div[data-testid="stTextInput"] input:focus {
        border-color:#3D52A0 !important; box-shadow:0 0 0 3px rgba(61,82,160,0.1) !important;
    }
    </style>
    """, unsafe_allow_html=True)

    # ── Hero ──
    logo_html = (f'<img src="data:image/png;base64,{LOGO_B64}" alt="LaborPilot" />'
                 if LOGO_B64 else "")
    st.markdown(f"""
    <div class="ai-hero">
        <div class="ai-hero-top">
            <div>
                <div class="ai-logo-row">
                    {logo_html}
                    <div class="ai-title">AIPilot</div>
                </div>
                <div class="ai-subtitle">Labor Intelligence for {hotel}</div>
            </div>
            <div class="ai-live-badge"><span class="ai-live-dot"></span> LIVE DATA</div>
        </div>
        <div class="ai-hero-desc">
            Ask anything — schedules, OT risk, cost analysis, staff mockups, trends.<br>
            I read your entire database and give you real answers with real names.
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Suggestion chips (clickable) ──
    SUGGESTIONS = [
        "Create a mockup schedule for next week for all departments",
        "Who had the most OT this month?",
        "Which department has the highest labor cost this month?",
        "Which employees are at OT risk this week?",
        "Give me a full labor cost summary for last month",
    ]

    st.markdown('<div class="ai-suggestion-row">', unsafe_allow_html=True)
    sug_cols = st.columns(len(SUGGESTIONS))
    for i, sug in enumerate(SUGGESTIONS):
        with sug_cols[i]:
            short = sug[:28] + "…" if len(sug) > 28 else sug
            if st.button(short, key=f"sug_{i}",
                         help=sug,
                         use_container_width=True):
                st.session_state.ai_pending = sug
                st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)

    # ── Input form (Enter to submit) ──
    with st.form("ai_form", clear_on_submit=False):
        question_input = st.text_input(
            "",
            value=st.session_state.ai_pending,
            placeholder='e.g. "Create a mockup schedule for housekeeping next week" or "Who had the most OT last month?"',
            label_visibility="collapsed",
            key="ai_question_field",
        )
        run = st.form_submit_button("Ask AIPilot", type="primary")

    if run and question_input.strip():
        st.session_state.ai_pending = question_input

    question = st.session_state.ai_pending

    if not run or not question.strip():
        if not question.strip():
            st.markdown("""
            <div class="ai-empty">
                <div class="ai-empty-icon">🤖</div>
                <div class="ai-empty-title">What do you want to know?</div>
                <div class="ai-empty-sub">
                    Type a question above or click a suggestion chip.<br>
                    Press <strong>Enter</strong> or click <strong>Ask AIPilot</strong> to get your answer.
                </div>
            </div>
            """, unsafe_allow_html=True)
        return

    # ── Resolve dates + intents ──
    start_date, end_date = parse_dates_from_question(question)
    intents = detect_intent(question)
    days    = (end_date - start_date).days + 1
    today   = date.today()

    if start_date == end_date:
        period_label = start_date.strftime("%B %d, %Y")
    else:
        period_label = f"{start_date.strftime('%b %d')} – {end_date.strftime('%b %d, %Y')}"

    # ── Fetch all data ──
    with st.spinner("Reading your database..."):
        try:
            data           = fetch_labor_data(hotel, start_date, end_date, intents)
            employees_df   = fetch_employees(hotel)
            positions_df   = fetch_positions(hotel)
            existing_sched = fetch_existing_schedule(hotel, start_date, end_date)
        except Exception as e:
            st.error(f"Database error: {e}")
            return

    # ── Period badge ──
    st.markdown(f'<div class="ai-period-badge">◈ Period: {period_label} · {days} day{"s" if days != 1 else ""}</div>',
                unsafe_allow_html=True)

    # ── KPI pills ──
    totals    = data.get("totals", pd.DataFrame())
    has_actuals = not totals.empty and float(totals.iloc[0].get("total_hours", 0)) > 0
    if has_actuals:
        t = totals.iloc[0]
        th = float(t.get("total_hours", 0)); tot = float(t.get("total_ot_hours", 0))
        rp = float(t.get("total_reg_pay", 0)); op  = float(t.get("total_ot_pay", 0))
        ue = int(t.get("unique_employees", 0))
        ot_pct = (tot / th * 100) if th > 0 else 0
        st.markdown(f"""
        <div class="ai-metrics-row">
            <div class="ai-metric"><div class="val">{th:,.0f}</div><div class="lbl">Total Hours</div></div>
            <div class="ai-metric"><div class="val red">{tot:,.1f}</div><div class="lbl">OT Hours ({ot_pct:.1f}%)</div></div>
            <div class="ai-metric"><div class="val teal">${rp+op:,.0f}</div><div class="lbl">Labor Cost</div></div>
            <div class="ai-metric"><div class="val red">${op:,.0f}</div><div class="lbl">OT Pay</div></div>
            <div class="ai-metric"><div class="val">{ue}</div><div class="lbl">Employees</div></div>
        </div>
        """, unsafe_allow_html=True)

    # ── AI call ──
    with st.spinner("Generating your answer..."):
        try:
            prompt  = build_prompt(hotel, start_date, end_date, question, data, intents,
                                   employees_df, positions_df, existing_sched)
            raw     = call_groq(prompt)
        except Exception as e:
            st.error(f"AI error: {e}")
            return

    # ── Parse schedule table if present ──
    schedule_df, summary = extract_schedule_table(raw)

    # ── Response card ──
    st.markdown(f'<div class="ai-model-tag">✦ Llama 3.3 · 70B</div>', unsafe_allow_html=True)
    if summary.strip():
        st.markdown(f'<div class="ai-response-card">{summary}</div>', unsafe_allow_html=True)

    # ── Schedule table display ──
    if schedule_df is not None:
        st.markdown('<div class="ai-table-header">📅 Generated Schedule</div>', unsafe_allow_html=True)

        # Color-code shift cells
        shift_colors = {
            "AM": "background-color:#dbeafe;color:#1e40af;font-weight:600",
            "PM": "background-color:#dcfce7;color:#166534;font-weight:600",
            "MID": "background-color:#fef9c3;color:#854d0e;font-weight:600",
            "OFF": "background-color:#f3f4f6;color:#9ca3af",
            "RDO": "background-color:#fee2e2;color:#991b1b;font-weight:600",
            "NT": "background-color:#ede9fe;color:#5b21b6;font-weight:600",
        }

        def style_cell(val):
            v = str(val).strip().upper()
            return shift_colors.get(v, "")

        styled = schedule_df.style.applymap(
            style_cell,
            subset=[c for c in schedule_df.columns if c not in
                    ["Employee Name", "Department", "Role", "Name"]]
        )
        st.dataframe(styled, use_container_width=True, height=min(40 + 36 * len(schedule_df), 600))

        # Download button
        csv_out = schedule_df.to_csv(index=False)
        st.download_button(
            "Download Schedule (.csv)",
            data=csv_out,
            file_name=f"schedule_{start_date}_{end_date}.csv",
            mime="text/csv",
            key="dl_schedule",
        )

    # ── Roster & existing schedule expanders ──
    if "schedule_mockup" in intents:
        if not employees_df.empty:
            with st.expander("Employee Roster Used", expanded=False):
                st.dataframe(employees_df, use_container_width=True)
        if not existing_sched.empty:
            with st.expander("Existing Schedule for This Period", expanded=False):
                st.dataframe(existing_sched, use_container_width=True)

    # ── Charts (actuals only) ──
    if has_actuals and "schedule_mockup" not in intents:
        st.markdown('<div class="ai-charts-header">Supporting Charts</div>'
                    '<div class="ai-charts-divider"></div>', unsafe_allow_html=True)
        render_charts(data, intents)

    # ── Email section ──
    st.markdown('<div class="ai-email-box">', unsafe_allow_html=True)
    st.markdown("**📧 Email This Report**")
    email_input = st.text_input(
        "Recipient(s) — separate multiple with commas",
        placeholder="email@example.com, another@example.com",
        key="ai_email_recipients",
        label_visibility="visible",
    )
    if st.button("Send Report via Email", key="ai_send_email"):
        if not email_input.strip():
            st.warning("Please enter at least one email address.")
        else:
            recipients = [e.strip() for e in email_input.split(",") if e.strip()]
            with st.spinner("Sending..."):
                try:
                    send_ai_report(recipients, hotel, question, summary,
                                   period_label, data, schedule_df)
                    st.success(f"Report sent to: {', '.join(recipients)}")
                except Exception as e:
                    st.error(f"Failed to send: {e}")
    st.markdown('</div>', unsafe_allow_html=True)
