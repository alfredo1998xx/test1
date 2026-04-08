"""
aipilot.py — AI-powered labor intelligence for LaborPilot.
Features: intent detection, rich data fetching,
question-focused prompts, table-aware response rendering, email export.
"""

import os
import re
import io
import base64
import smtplib
import ssl
import textwrap
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from datetime import date, timedelta
from groq import Groq
from sqlalchemy import text
from db import ENGINE

EMAIL_SENDER   = "alfredo1998x@gmail.com"
EMAIL_PASSWORD = "iqry ajnp zvuo yeuq"


# ─────────────────────────────────────────────────────────────────────────────
# SUGGESTION CHIPS
# ─────────────────────────────────────────────────────────────────────────────
SUGGESTIONS = [
    "What was our overtime last week?",
    "Is anyone at OT risk this week?",
    "Which department had the highest labor cost this month?",
    "Create a mockup schedule for this week",
    "Show me YTD hours and cost by department",
    "Who are our top OT earners this month?",
    "Compare this week's labor cost to last week",
    "What is our labor cost per occupied room this month?",
    "Show me hours by position this month",
    "Which employees worked the most hours last month?",
]


# ─────────────────────────────────────────────────────────────────────────────
# INTENT DETECTION
# ─────────────────────────────────────────────────────────────────────────────
INTENT_MAP = {
    "ot":         ["overtime", "ot hours", "ot pay", "over time", "time and a half",
                   "time-and-a-half", "ot rate", "ot cost"],
    "risk":       ["risk", "at risk", "approaching 40", "close to 40", "near 40",
                   "threshold", "about to hit", "almost 40", "ot risk"],
    "schedule":   ["schedule", "scheduling", "mockup", "mock-up", "who is working",
                   "who's working", "shift", "shifts", "staffing", "coverage",
                   "create a schedule", "build a schedule", "suggest a schedule",
                   "next week schedule", "this week schedule"],
    "cost":       ["cost", "payroll", "spend", "spending", "budget", "wage", "wages",
                   "salary", "expense", "pay", "total cost", "labor cost", "labor spend"],
    "comparison": ["compare", "comparison", "vs ", "versus", "week over week",
                   "last week vs", "previous", "how does", "better or worse",
                   "more than", "less than", "higher", "lower", "trend",
                   "change", "increased", "decreased"],
    "position":   ["position", "role", "job title", "which position", "by position",
                   "by role", "job type"],
    "efficiency": ["per room", "per occupied", "efficiency", "labor ratio", "cpor",
                   "cost per room", "revenue per", "productivity"],
    "employee":   ["employee", "employees", "staff", "worker", "who worked",
                   "who has the most", "top earner", "earner", "individual",
                   "person", "who logged", "who put in"],
    "headcount":  ["headcount", "how many employees", "how many people",
                   "staffing level", "head count", "number of employees",
                   "team size", "how many staff"],
    "department": ["department", "dept", "housekeeping", "front desk", "food",
                   "maintenance", "engineering", "by department", "which dept",
                   "food and beverage", "f&b", "front office"],
}

def detect_intents(question: str) -> set:
    """Return ALL matching intents for the question (multi-intent support)."""
    q = question.lower()
    found = set()
    for intent, keywords in INTENT_MAP.items():
        if any(kw in q for kw in keywords):
            found.add(intent)
    return found if found else {"general"}

def detect_intent(question: str) -> str:
    """Return primary intent (kept for backwards compatibility)."""
    intents = detect_intents(question)
    priority = ["risk", "schedule", "comparison", "ot", "efficiency",
                "employee", "cost", "position", "department", "headcount"]
    for p in priority:
        if p in intents:
            return p
    return next(iter(intents))


# ─────────────────────────────────────────────────────────────────────────────
# DATE RANGE PARSING
# ─────────────────────────────────────────────────────────────────────────────
def parse_date_range(question: str):
    today = date.today()
    q = question.lower()

    if "yesterday" in q:
        s = today - timedelta(days=1); return s, s
    if "last week" in q or "previous week" in q:
        s = today - timedelta(days=today.weekday() + 7)
        return s, s + timedelta(days=6)
    if "this week" in q or "current week" in q:
        return today - timedelta(days=today.weekday()), today
    if "next week" in q:
        s = today + timedelta(days=7 - today.weekday())
        return s, s + timedelta(days=6)
    if "last month" in q or "previous month" in q:
        first = today.replace(day=1)
        e = first - timedelta(days=1)
        return e.replace(day=1), e
    if "this month" in q or "current month" in q or "mtd" in q or "month to date" in q:
        return today.replace(day=1), today
    if "last 7 days" in q or "past 7 days" in q:
        return today - timedelta(days=6), today
    if "last 14 days" in q or "past 14 days" in q:
        return today - timedelta(days=13), today
    if "last 30 days" in q or "past 30 days" in q:
        return today - timedelta(days=29), today
    if "ytd" in q or "year to date" in q or "this year" in q:
        return today.replace(month=1, day=1), today
    if "last quarter" in q:
        m = today.month
        q_start_month = ((m - 1) // 3) * 3 + 1 - 3
        if q_start_month <= 0: q_start_month += 12
        s = today.replace(month=q_start_month, day=1)
        e = (today.replace(month=((q_start_month - 1 + 3) % 12) + 1, day=1) - timedelta(days=1))
        return s, e
    return today - timedelta(days=29), today   # default: last 30 days


# ─────────────────────────────────────────────────────────────────────────────
# DATA FETCHING
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=180, show_spinner=False)
def _q(sql: str, params: dict) -> pd.DataFrame:
    with ENGINE.connect() as conn:
        return pd.read_sql_query(text(sql), conn, params=params)


def fetch_data(hotel: str, start: date, end: date, intent) -> dict:
    """
    intent can be a str or a set of strings (multi-intent).
    "general" triggers fetching of ALL data types.
    """
    if isinstance(intent, str):
        intents = {intent}
    else:
        intents = set(intent)

    # "general" means we know nothing — fetch everything
    if "general" in intents:
        intents = set(INTENT_MAP.keys()) | {"general"}

    p  = {"hotel": hotel, "start": str(start), "end": str(end)}
    today = date.today()
    data = {}

    # ── Always: totals ──
    data["totals"] = _q("""
        SELECT COALESCE(SUM(a.hours),0) total_hours,
               COALESCE(SUM(a.ot_hours),0) total_ot,
               COALESCE(SUM(a.reg_pay),0)  reg_pay,
               COALESCE(SUM(a.ot_pay),0)   ot_pay,
               COUNT(DISTINCT a.emp_id)     employees
        FROM actual a
        WHERE a.hotel_name=:hotel AND a.date BETWEEN :start AND :end
    """, p)

    # ── Always: department breakdown ──
    data["dept"] = _q("""
        SELECT d.name department,
               COALESCE(SUM(a.hours),0)   total_hours,
               COALESCE(SUM(a.ot_hours),0) ot_hours,
               COALESCE(SUM(a.reg_pay),0) reg_pay,
               COALESCE(SUM(a.ot_pay),0)  ot_pay,
               COUNT(DISTINCT a.emp_id)   employees
        FROM actual a
        JOIN positions pos ON pos.id=a.position_id
        JOIN departments d  ON d.id=pos.department_id
        WHERE a.hotel_name=:hotel AND a.date BETWEEN :start AND :end
        GROUP BY d.name ORDER BY total_hours DESC
    """, p)

    # ── OT / Employee detail ──
    if intents & {"ot", "employee", "general", "headcount", "comparison", "cost"}:
        data["emp_ot"] = _q("""
            SELECT e.name, e.department,
                   COALESCE(SUM(a.hours),0)    total_hours,
                   COALESCE(SUM(a.ot_hours),0) ot_hours,
                   COALESCE(SUM(a.ot_pay),0)   ot_pay,
                   COALESCE(SUM(a.reg_pay),0)  reg_pay
            FROM actual a
            JOIN employee e ON e.id=a.emp_id AND e.hotel_name=:hotel
            WHERE a.hotel_name=:hotel AND a.date BETWEEN :start AND :end
            GROUP BY e.name, e.department
            ORDER BY total_hours DESC
        """, p)

    # ── Position breakdown ──
    if intents & {"position", "general", "cost", "department", "employee"}:
        data["position"] = _q("""
            SELECT pos.name position, d.name department,
                   COALESCE(SUM(a.hours),0)    total_hours,
                   COALESCE(SUM(a.ot_hours),0) ot_hours,
                   COALESCE(SUM(a.reg_pay+a.ot_pay),0) total_cost,
                   COUNT(DISTINCT a.emp_id) employees
            FROM actual a
            JOIN positions pos ON pos.id=a.position_id
            JOIN departments d  ON d.id=pos.department_id
            WHERE a.hotel_name=:hotel AND a.date BETWEEN :start AND :end
            GROUP BY pos.name, d.name ORDER BY total_hours DESC LIMIT 20
        """, p)

    # ── Daily trend ──
    if intents & {"ot", "cost", "general", "comparison", "department", "employee"}:
        data["daily"] = _q("""
            SELECT a.date,
                   COALESCE(SUM(a.hours),0)    hours,
                   COALESCE(SUM(a.ot_hours),0) ot_hours,
                   COALESCE(SUM(a.reg_pay),0)  reg_pay,
                   COALESCE(SUM(a.ot_pay),0)   ot_pay
            FROM actual a
            WHERE a.hotel_name=:hotel AND a.date BETWEEN :start AND :end
            GROUP BY a.date ORDER BY a.date
        """, p)

    # ── Comparison: fetch previous period too ──
    if "comparison" in intents:
        period_days = (end - start).days + 1
        prev_end   = start - timedelta(days=1)
        prev_start = prev_end - timedelta(days=period_days - 1)
        pp = {"hotel": hotel, "start": str(prev_start), "end": str(prev_end)}
        data["prev_totals"] = _q("""
            SELECT COALESCE(SUM(a.hours),0)  total_hours,
                   COALESCE(SUM(a.ot_hours),0) total_ot,
                   COALESCE(SUM(a.reg_pay+a.ot_pay),0) total_cost
            FROM actual a
            WHERE a.hotel_name=:hotel AND a.date BETWEEN :start AND :end
        """, pp)
        data["prev_dept"] = _q("""
            SELECT d.name department,
                   COALESCE(SUM(a.hours),0)  total_hours,
                   COALESCE(SUM(a.reg_pay+a.ot_pay),0) total_cost
            FROM actual a
            JOIN positions pos ON pos.id=a.position_id
            JOIN departments d  ON d.id=pos.department_id
            WHERE a.hotel_name=:hotel AND a.date BETWEEN :start AND :end
            GROUP BY d.name
        """, pp)
        data["prev_period"] = (prev_start, prev_end)

    # ── OT Risk: this week's schedule + actual so far ──
    if "risk" in intents:
        week_start = today - timedelta(days=today.weekday())
        week_end   = week_start + timedelta(days=6)
        rp = {"hotel": hotel, "ws": str(week_start),
              "we": str(week_end), "today": str(today)}
        data["ot_risk"] = _q("""
            SELECT e.name, e.department,
                   COUNT(DISTINCT s.day)     scheduled_days,
                   COUNT(DISTINCT s.day)*8.0 projected_hours,
                   COALESCE(SUM(a.hours),0)  actual_so_far,
                   e.hourly_rate
            FROM employee e
            LEFT JOIN schedule s ON s.emp_id=e.id AND s.hotel_name=:hotel
                AND s.day BETWEEN :ws AND :we
                AND s.shift_type NOT IN ('OFF','off','Off')
            LEFT JOIN actual a ON a.emp_id=e.id AND a.hotel_name=:hotel
                AND a.date BETWEEN :ws AND :today
            WHERE e.hotel_name=:hotel
            GROUP BY e.name, e.department, e.hourly_rate
            HAVING COUNT(DISTINCT s.day)>0
            ORDER BY projected_hours DESC
        """, rp)
        data["risk_week"] = (week_start, week_end)

    # ── Schedule: shifts in range ──
    if intents & {"schedule", "risk", "headcount", "general"}:
        data["schedule"] = _q("""
            SELECT e.name employee, e.department, e.role position,
                   s.day, s.shift_type
            FROM schedule s
            JOIN employee e ON e.id=s.emp_id AND e.hotel_name=:hotel
            WHERE s.hotel_name=:hotel AND s.day BETWEEN :start AND :end
              AND s.shift_type NOT IN ('OFF','off','Off')
            ORDER BY s.day, e.department, e.name
        """, p)
        data["headcount_daily"] = _q("""
            SELECT s.day, COUNT(DISTINCT s.emp_id) headcount
            FROM schedule s
            WHERE s.hotel_name=:hotel AND s.day BETWEEN :start AND :end
              AND s.shift_type NOT IN ('OFF','off','Off')
            GROUP BY s.day ORDER BY s.day
        """, p)

    # ── Efficiency: labor cost per occupied room ──
    if "efficiency" in intents:
        data["rooms"] = _q("""
            SELECT ra.date, COALESCE(SUM(ra.value),0) occupied_rooms
            FROM room_actual ra
            WHERE ra.hotel_name=:hotel AND ra.date BETWEEN :start AND :end
              AND ra.kpi='Occupied Rooms'
            GROUP BY ra.date ORDER BY ra.date
        """, p)

    # ── Schedule mockup: full employee roster ──
    if "schedule" in intents or "general" in intents:
        data["roster"] = _q("""
            SELECT e.name, e.department, e.role position, e.emp_type
            FROM employee e WHERE e.hotel_name=:hotel
            ORDER BY e.department, e.name
        """, p)

    return data


# ─────────────────────────────────────────────────────────────────────────────
# PROMPT BUILDER — question-focused
# ─────────────────────────────────────────────────────────────────────────────
def build_prompt(hotel: str, start: date, end: date,
                 question: str, intent, data: dict) -> str:
    """
    Build the user-facing data context prompt.
    The system persona is in SYSTEM_PROMPT (sent as role=system in call_groq).
    intent can be a str or set of strings.
    """
    today  = date.today()
    period = f"{start.strftime('%B %d')} — {end.strftime('%B %d, %Y')}"
    days   = (end - start).days + 1

    if isinstance(intent, str):
        intents = {intent}
    else:
        intents = set(intent)

    # ── Core metrics ──
    t   = data["totals"].iloc[0] if not data["totals"].empty else {}
    th  = float(t.get("total_hours", 0))
    tot = float(t.get("total_ot", 0))
    rp  = float(t.get("reg_pay", 0))
    op  = float(t.get("ot_pay", 0))
    tc  = rp + op
    emp = int(t.get("employees", 0))
    ot_pct   = (tot / th * 100) if th else 0
    reg_pct  = 100 - ot_pct

    lines = [
        f"QUESTION: {question}",
        "",
        f"HOTEL: {hotel}",
        f"ANALYSIS PERIOD: {period} ({days} days)",
        f"TODAY: {today.strftime('%A, %B %d, %Y')}",
        "",
        "─── OVERALL LABOR METRICS ───",
        f"Total Hours Worked : {th:,.1f}h  (Regular: {th - tot:,.1f}h  |  OT: {tot:,.1f}h = {ot_pct:.1f}% of total)",
        f"Total Labor Cost   : ${tc:,.2f}  (Regular Pay: ${rp:,.2f}  |  OT Pay: ${op:,.2f})",
        f"Avg Cost/Hour      : ${(tc / th):,.2f}" if th else "Avg Cost/Hour: N/A",
        f"Active Employees   : {emp}",
        "",
    ]

    # ── Department breakdown — always included ──
    if not data["dept"].empty:
        dept = data["dept"].copy()
        # Add total_cost column if not present
        if "total_cost" not in dept.columns:
            dept["total_cost"] = dept.get("reg_pay", 0) + dept.get("ot_pay", 0)
        lines += ["─── DEPARTMENT BREAKDOWN ───", dept.to_string(index=False), ""]

    # ── Employee / OT detail ──
    if "emp_ot" in data and not data["emp_ot"].empty:
        emp_df = data["emp_ot"]
        if "ot" in intents or "comparison" in intents:
            top_ot = emp_df.nlargest(15, "ot_hours")
            lines += ["─── TOP EMPLOYEES BY OT HOURS ───", top_ot.to_string(index=False), ""]
        if "employee" in intents:
            lines += ["─── ALL EMPLOYEES BY HOURS ───", emp_df.to_string(index=False), ""]
        if "headcount" in intents or "general" in intents:
            lines += [
                f"─── EMPLOYEE COUNT SUMMARY ───",
                f"Employees with OT: {(emp_df['ot_hours'] > 0).sum()}",
                f"Employees at/over 40h: {(emp_df['total_hours'] >= 40).sum()}",
                "",
            ]

    # ── Position breakdown ──
    if "position" in data and not data["position"].empty:
        lines += ["─── BY POSITION/ROLE ───", data["position"].to_string(index=False), ""]

    # ── Daily trend ──
    if "daily" in data and not data["daily"].empty:
        daily = data["daily"].copy()
        if "reg_pay" in daily.columns and "ot_pay" in daily.columns:
            daily["total_cost"] = daily["reg_pay"] + daily["ot_pay"]
        lines += ["─── DAILY TREND ───", daily.to_string(index=False), ""]

    # ── OT Risk ──
    if "ot_risk" in data and not data["ot_risk"].empty:
        ws, we = data.get("risk_week", (today, today))
        risk_df = data["ot_risk"]
        at_risk = risk_df[risk_df["projected_hours"] >= 36]
        near_40 = risk_df[risk_df["projected_hours"] == 40]
        lines += [
            f"─── OT RISK ANALYSIS ({ws.strftime('%b %d')} – {we.strftime('%b %d')}) ───",
            "NOTE: projected_hours = scheduled_days × 8h  |  OT threshold = 40h/week (FLSA)",
            risk_df.to_string(index=False),
            "",
            f"AT/NEAR OT THRESHOLD (≥36h projected):",
            at_risk.to_string(index=False) if not at_risk.empty else "  None currently flagged.",
            "",
            f"WILL HIT EXACTLY 40h (full 5-day schedule):",
            near_40.to_string(index=False) if not near_40.empty else "  None.",
            "",
        ]

    # ── Schedule / Roster ──
    if "schedule" in data and not data["schedule"].empty:
        lines += ["─── EXISTING SCHEDULE DATA ───", data["schedule"].to_string(index=False), ""]
    if "headcount_daily" in data and not data["headcount_daily"].empty:
        lines += ["─── DAILY HEADCOUNT ───", data["headcount_daily"].to_string(index=False), ""]
    if "roster" in data and not data["roster"].empty:
        lines += [
            "─── FULL EMPLOYEE ROSTER (use these real names for any schedule) ───",
            data["roster"].to_string(index=False),
            "",
        ]

    # ── Comparison ──
    if "prev_totals" in data and not data["prev_totals"].empty and "prev_period" in data:
        pp   = data["prev_period"]
        prev = data["prev_totals"].iloc[0]
        ph   = float(prev.get("total_hours", 0))
        pc   = float(prev.get("total_cost", 0))
        pot  = float(prev.get("total_ot", 0))
        h_chg = ((th - ph) / ph * 100) if ph else 0
        c_chg = ((tc - pc) / pc * 100) if pc else 0
        lines += [
            f"─── COMPARISON: PREVIOUS PERIOD ({pp[0].strftime('%b %d')} – {pp[1].strftime('%b %d')}) ───",
            f"Previous Hours : {ph:,.1f}h  →  Current: {th:,.1f}h  ({h_chg:+.1f}%)",
            f"Previous Cost  : ${pc:,.2f}  →  Current: ${tc:,.2f}  ({c_chg:+.1f}%)",
            f"Previous OT    : {pot:,.1f}h  →  Current: {tot:,.1f}h",
            "",
        ]
        if "prev_dept" in data and not data["prev_dept"].empty:
            lines += ["Previous Period by Department:", data["prev_dept"].to_string(index=False), ""]

    # ── Efficiency / CPOR ──
    if "rooms" in data and not data["rooms"].empty:
        rooms_df    = data["rooms"]
        total_rooms = rooms_df["occupied_rooms"].sum()
        cpor        = tc / total_rooms if total_rooms else 0
        lines += [
            "─── ROOM OCCUPANCY & EFFICIENCY ───",
            f"Total Occupied Room-Nights : {total_rooms:,.0f}",
            f"Labor Cost Per Occupied Room (CPOR) : ${cpor:,.2f}",
            f"Industry benchmark for full-service hotels: $18–$35 CPOR",
            rooms_df.to_string(index=False),
            "",
        ]

    # ── Data availability note ──
    missing = []
    if data["totals"].empty:  missing.append("labor actuals")
    if data["dept"].empty:    missing.append("department data")
    if missing:
        lines += [f"NOTE: No {', '.join(missing)} found for the selected period/hotel.", ""]

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# GROQ API
# ─────────────────────────────────────────────────────────────────────────────
class GroqRateLimitError(Exception):
    """Raised when Groq returns a 429 / token-limit error."""


SYSTEM_PROMPT = """You are LaborPilot AI — a senior hotel labor analytics advisor with 20+ years of hospitality industry experience.

Your expertise covers:
- Hotel labor cost management, OT compliance, and scheduling optimization
- Wage & hour law (FLSA overtime at 40h/week threshold)
- Labor cost benchmarking across departments (Housekeeping, Front Desk, F&B, Maintenance, Engineering, Night Audit)
- Cost Per Occupied Room (CPOR) and labor-to-revenue ratios
- Schedule building, shift planning, and staffing coverage analysis
- Payroll analysis, budget variance, and trend identification
- Employee productivity and department efficiency metrics

You have access to REAL data from the hotel's labor management system. Your responses must:
1. Be grounded ENTIRELY in the data provided — never fabricate numbers
2. Lead with the direct answer and key metric in the first sentence
3. Use confident, executive-level language — no hedging, no jargon
4. Give specific employee names, department names, and dollar amounts from the data
5. Flag anomalies proactively (e.g., unusually high OT in one department, a spike in a day's cost)
6. For schedule requests — output a complete, clean markdown table with real employee names from the roster
7. For OT risk — list every at-risk employee by name with their exact projected hours
8. For comparisons — always include the % change and direction (↑ increase / ↓ decrease)
9. For cost questions — break down by department and identify the largest driver
10. If asked something outside labor/hotel operations — briefly answer it, then pivot back to what you can help with in LaborPilot

Formatting rules:
- Use **bold** for key numbers and employee/department names
- Use bullet lists for Key Takeaways (3 bullets, each with a real number)
- End every response with a single "Recommendation:" line — one decisive action
- Do NOT use markdown headers (##, ###)
- Keep responses under 500 words unless generating a schedule table
- For schedule tables use this format: | Employee | Department | Mon | Tue | Wed | Thu | Fri | Sat | Sun |"""


def call_groq(user_prompt: str, system_prompt: str = SYSTEM_PROMPT) -> str:
    api_key = os.environ.get("GROQ_API_KEY", "")
    if not api_key:
        raise ValueError("GROQ_API_KEY secret is not configured.")
    client = Groq(api_key=api_key)
    try:
        model = st.session_state.get("aipilot_model", "llama-3.3-70b-versatile")
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": user_prompt},
            ],
            temperature=0.25,
            max_tokens=1400,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        msg = str(e)
        if "429" in msg or "rate_limit" in msg.lower() or "rate limit" in msg.lower():
            wait = ""
            m = re.search(r'try again in ([^\.\'"]+)', msg, re.IGNORECASE)
            if m:
                wait = f" Please wait **{m.group(1).strip()}** before trying again."
            raise GroqRateLimitError(
                f"Daily AI token limit reached.{wait}"
            ) from e
        raise


# ─────────────────────────────────────────────────────────────────────────────
# RESPONSE RENDERER — collapses whitespace, parses inline tables
# ─────────────────────────────────────────────────────────────────────────────
def table_md_to_df(table_text: str) -> pd.DataFrame:
    """Convert a markdown table string to a DataFrame."""
    lines = [l.strip() for l in table_text.splitlines()
             if l.strip() and not re.match(r'^\|?\s*[-:]+[-| :]*\|?\s*$', l.strip())]
    if not lines:
        return pd.DataFrame()
    headers = [h.strip() for h in lines[0].strip("|").split("|") if h.strip()]
    rows = []
    for line in lines[1:]:
        cols = [c.strip() for c in line.strip("|").split("|")]
        if cols:
            rows.append(cols)
    max_cols = max(len(r) for r in rows) if rows else len(headers)
    headers  = (headers + [""] * max_cols)[:max_cols]
    df = pd.DataFrame(rows, columns=headers)
    return df


def _text_to_html(text: str) -> str:
    """
    Convert AI plain-text/markdown response to safe HTML.
    - Escapes $ to prevent Streamlit LaTeX rendering
    - Converts **bold**, bullet lists, blank lines to HTML
    """
    import html as _html

    lines   = text.splitlines()
    out     = []
    in_list = False

    for line in lines:
        # Detect bullet lines (-, *, •, numbered)
        is_bullet = bool(re.match(r'^\s*([-*•]|\d+\.)\s+', line))

        # Escape HTML special chars first (except we handle $ separately)
        safe = _html.escape(line, quote=False)

        # Restore < > for any we may want (none in AI output — keep escaped)
        # Escape $ → &#36; (prevents Streamlit LaTeX interpretation)
        safe = safe.replace("$", "&#36;")

        # Bold: **text**
        safe = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', safe)
        # Italic: *text*  (only if not a bullet marker)
        safe = re.sub(r'(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)', r'<em>\1</em>', safe)

        if is_bullet:
            # Strip the leading bullet/number marker
            safe = re.sub(r'^\s*([-*•]|\d+\.)\s+', '', safe)
            if not in_list:
                out.append('<ul style="margin:8px 0 8px 20px;padding:0;">')
                in_list = True
            out.append(
                f'<li style="margin:5px 0;font-size:14px;'
                f'line-height:1.7;color:#1f2937;">{safe}</li>'
            )
        else:
            if in_list:
                out.append('</ul>')
                in_list = False
            if not safe.strip():
                out.append('<div style="height:8px;"></div>')
            else:
                out.append(
                    f'<p style="margin:0 0 6px;font-size:14px;'
                    f'line-height:1.75;color:#1f2937;">{safe}</p>'
                )

    if in_list:
        out.append('</ul>')

    return "\n".join(out)


def render_ai_response(raw: str):
    """Split AI text into prose and markdown-table segments, render each properly."""
    # Collapse 3+ blank lines → 2
    cleaned = re.sub(r'\n{3,}', '\n\n', raw.strip())

    lines     = cleaned.splitlines()
    segments  = []
    txt_buf   = []
    tbl_buf   = []
    in_table  = False

    for line in lines:
        is_table_row = bool(re.match(r'^\s*\|', line))
        if is_table_row:
            if not in_table:
                if txt_buf:
                    segments.append(("text", "\n".join(txt_buf)))
                    txt_buf = []
                in_table = True
            tbl_buf.append(line)
        else:
            if in_table:
                segments.append(("table", "\n".join(tbl_buf)))
                tbl_buf = []
                in_table = False
            txt_buf.append(line)

    if tbl_buf:
        segments.append(("table", "\n".join(tbl_buf)))
    if txt_buf:
        segments.append(("text", "\n".join(txt_buf)))

    for kind, content in segments:
        if kind == "text" and content.strip():
            html_content = _text_to_html(content)
            st.markdown(
                f'<div class="ai-card">{html_content}</div>',
                unsafe_allow_html=True,
            )
        elif kind == "table":
            df = table_md_to_df(content)
            if not df.empty:
                st.dataframe(df, use_container_width=True, hide_index=True)


# ─────────────────────────────────────────────────────────────────────────────
# DYNAMIC CHARTS
# ─────────────────────────────────────────────────────────────────────────────
def render_charts(intent: str, data: dict):
    dept    = data.get("dept", pd.DataFrame())
    daily   = data.get("daily", pd.DataFrame())
    emp_ot  = data.get("emp_ot", pd.DataFrame())
    shown   = False

    # ── OT ──
    if intent == "ot":
        c1, c2 = st.columns(2)
        with c1:
            if not emp_ot.empty:
                top = emp_ot[emp_ot["ot_hours"] > 0].nlargest(10, "ot_hours")
                if not top.empty:
                    fig = px.bar(top, x="ot_hours", y="name", orientation="h",
                                 color="ot_hours", color_continuous_scale=["#FFF9C4","#FF5722"],
                                 title="OT Hours by Employee",
                                 labels={"ot_hours":"OT Hours","name":""},
                                 template="plotly_white")
                    fig.update_layout(yaxis=dict(autorange="reversed"),
                                      margin=dict(t=40,b=10), title_font_size=14)
                    st.plotly_chart(fig, use_container_width=True); shown=True
        with c2:
            if not daily.empty:
                fig2 = go.Figure()
                fig2.add_trace(go.Bar(x=daily["date"], y=daily["hours"],
                                       name="Total Hours", marker_color="#90CAF9"))
                fig2.add_trace(go.Bar(x=daily["date"], y=daily["ot_hours"],
                                       name="OT Hours", marker_color="#FF5722"))
                fig2.update_layout(barmode="overlay", title="Daily Hours vs OT",
                                   template="plotly_white",
                                   margin=dict(t=40,b=10), title_font_size=14,
                                   legend=dict(orientation="h",y=1.1))
                st.plotly_chart(fig2, use_container_width=True); shown=True

    # ── Risk ──
    elif intent == "risk":
        risk_df = data.get("ot_risk", pd.DataFrame())
        if not risk_df.empty:
            risk_df = risk_df.copy()
            risk_df["status"] = risk_df["projected_hours"].apply(
                lambda x: "🔴 At Risk ≥40h" if x >= 40 else
                          ("🟠 Approaching ≥36h" if x >= 36 else "🟢 Safe"))
            color_map = {"🔴 At Risk ≥40h":"#e53935",
                         "🟠 Approaching ≥36h":"#FF9800",
                         "🟢 Safe":"#43A047"}
            fig = px.bar(risk_df.sort_values("projected_hours"),
                         x="projected_hours", y="name", orientation="h",
                         color="status", color_discrete_map=color_map,
                         title="Projected Weekly Hours — OT Risk",
                         labels={"projected_hours":"Projected Hours","name":""},
                         template="plotly_white")
            fig.add_vline(x=40, line_dash="dash", line_color="red",
                          annotation_text="40h threshold")
            fig.add_vline(x=36, line_dash="dot", line_color="orange",
                          annotation_text="Warning")
            fig.update_layout(margin=dict(t=50,b=10), title_font_size=14,
                              legend=dict(title="",orientation="h",y=1.12))
            st.plotly_chart(fig, use_container_width=True); shown=True
        hc = data.get("headcount_daily", pd.DataFrame())
        if not hc.empty:
            fig2 = px.bar(hc, x="day", y="headcount",
                          title="Daily Headcount This Week",
                          color="headcount",
                          color_continuous_scale=["#E3F2FD","#1565C0"],
                          labels={"headcount":"Employees","day":""},
                          template="plotly_white")
            fig2.update_layout(margin=dict(t=40,b=10), title_font_size=14)
            st.plotly_chart(fig2, use_container_width=True); shown=True

    # ── Schedule / Headcount ──
    elif intent in ("schedule","headcount"):
        c1, c2 = st.columns(2)
        hc = data.get("headcount_daily", pd.DataFrame())
        with c1:
            if not hc.empty:
                fig = px.bar(hc, x="day", y="headcount",
                             title="Scheduled Headcount by Day",
                             color="headcount",
                             color_continuous_scale=["#E8F5E9","#2E7D32"],
                             labels={"headcount":"Employees","day":""},
                             template="plotly_white")
                fig.update_layout(margin=dict(t=40,b=10), title_font_size=14)
                st.plotly_chart(fig, use_container_width=True); shown=True
        with c2:
            sched = data.get("schedule", pd.DataFrame())
            if not sched.empty and "department" in sched.columns:
                dd = sched.groupby(["department","day"]).size().reset_index(name="count")
                fig2 = px.bar(dd, x="day", y="count", color="department",
                              barmode="stack",
                              title="Shifts by Department per Day",
                              labels={"count":"Shifts","day":""},
                              template="plotly_white")
                fig2.update_layout(margin=dict(t=40,b=10), title_font_size=14,
                                   legend=dict(title="",orientation="h",y=1.12))
                st.plotly_chart(fig2, use_container_width=True); shown=True

    # ── Cost ──
    elif intent == "cost":
        c1, c2 = st.columns(2)
        with c1:
            if not daily.empty:
                fig = go.Figure()
                fig.add_trace(go.Scatter(x=daily["date"], y=daily["reg_pay"],
                                          name="Regular Pay", fill="tozeroy",
                                          line=dict(color="#2196F3")))
                fig.add_trace(go.Scatter(x=daily["date"], y=daily["ot_pay"],
                                          name="OT Pay", fill="tozeroy",
                                          line=dict(color="#FF5722")))
                fig.update_layout(title="Daily Cost Trend", template="plotly_white",
                                  margin=dict(t=40,b=10), title_font_size=14,
                                  legend=dict(orientation="h",y=1.1))
                st.plotly_chart(fig, use_container_width=True); shown=True
        with c2:
            if not dept.empty:
                dept2 = dept.copy()
                dept2["total_cost"] = dept2["reg_pay"] + dept2["ot_pay"]
                fig2 = px.pie(dept2, names="department", values="total_cost",
                              title="Cost Share by Department",
                              template="plotly_white",
                              color_discrete_sequence=px.colors.qualitative.Set2)
                fig2.update_layout(margin=dict(t=40,b=10), title_font_size=14)
                st.plotly_chart(fig2, use_container_width=True); shown=True

    # ── Comparison ──
    elif intent == "comparison":
        curr = data["dept"].copy() if not data["dept"].empty else pd.DataFrame()
        prev = data.get("prev_dept", pd.DataFrame()).copy()
        if not curr.empty:
            curr["period"] = "Current"
            prev2 = prev.copy() if not prev.empty else pd.DataFrame(columns=curr.columns)
            if not prev2.empty:
                prev2["period"] = "Previous"
                combined = pd.concat([curr[["department","total_hours","period"]],
                                      prev2[["department","total_hours","period"]]])
                fig = px.bar(combined, x="department", y="total_hours",
                             color="period", barmode="group",
                             title="Hours: Current vs Previous Period",
                             color_discrete_map={"Current":"#2196F3","Previous":"#90CAF9"},
                             labels={"total_hours":"Hours","department":""},
                             template="plotly_white")
                fig.update_layout(margin=dict(t=40,b=10), title_font_size=14,
                                  legend=dict(title="",orientation="h",y=1.1))
                st.plotly_chart(fig, use_container_width=True); shown=True

    # ── Position ──
    elif intent == "position":
        pos_df = data.get("position", pd.DataFrame())
        if not pos_df.empty:
            c1, c2 = st.columns(2)
            with c1:
                fig = px.bar(pos_df.head(15), x="total_hours", y="position",
                             orientation="h", color="department",
                             title="Hours by Position (Top 15)",
                             labels={"total_hours":"Hours","position":""},
                             template="plotly_white")
                fig.update_layout(yaxis=dict(autorange="reversed"),
                                  margin=dict(t=40,b=10), title_font_size=14,
                                  legend=dict(title="",orientation="h",y=1.12))
                st.plotly_chart(fig, use_container_width=True); shown=True
            with c2:
                fig2 = px.bar(pos_df.head(15), x="total_cost", y="position",
                              orientation="h", color="department",
                              title="Cost by Position (Top 15)",
                              labels={"total_cost":"Cost ($)","position":""},
                              template="plotly_white")
                fig2.update_layout(yaxis=dict(autorange="reversed"),
                                   margin=dict(t=40,b=10), title_font_size=14,
                                   legend=dict(title="",orientation="h",y=1.12))
                st.plotly_chart(fig2, use_container_width=True); shown=True

    # ── Efficiency ──
    elif intent == "efficiency":
        rooms_df = data.get("rooms", pd.DataFrame())
        if not rooms_df.empty and not daily.empty:
            merged = pd.merge(daily, rooms_df, left_on="date", right_on="date", how="left")
            merged["total_cost"] = merged["reg_pay"] + merged["ot_pay"]
            merged["cpor"] = merged.apply(
                lambda r: r["total_cost"] / r["occupied_rooms"]
                if r["occupied_rooms"] > 0 else None, axis=1)
            c1, c2 = st.columns(2)
            with c1:
                fig = px.line(merged.dropna(subset=["cpor"]),
                              x="date", y="cpor",
                              title="Labor Cost Per Occupied Room (CPOR)",
                              labels={"cpor":"CPOR ($)","date":""},
                              template="plotly_white")
                fig.update_layout(margin=dict(t=40,b=10), title_font_size=14)
                st.plotly_chart(fig, use_container_width=True); shown=True
            with c2:
                fig2 = go.Figure()
                fig2.add_trace(go.Bar(x=merged["date"], y=merged["occupied_rooms"],
                                       name="Occ. Rooms", marker_color="#90CAF9"))
                fig2.add_trace(go.Scatter(x=merged["date"], y=merged["total_cost"],
                                           name="Labor Cost $", yaxis="y2",
                                           line=dict(color="#FF5722")))
                fig2.update_layout(
                    title="Occupancy vs Labor Cost",
                    yaxis=dict(title="Rooms"),
                    yaxis2=dict(title="Cost ($)", overlaying="y", side="right"),
                    template="plotly_white",
                    margin=dict(t=40,b=10), title_font_size=14,
                    legend=dict(orientation="h",y=1.1))
                st.plotly_chart(fig2, use_container_width=True); shown=True

    # ── General / Department / Employee ──
    else:
        c1, c2 = st.columns(2)
        with c1:
            if not dept.empty:
                fig = px.bar(dept, x="department", y="total_hours",
                             color="ot_hours",
                             color_continuous_scale=["#4CAF50","#FF5722"],
                             title="Hours by Department",
                             labels={"total_hours":"Hours","department":"",
                                     "ot_hours":"OT Hours"},
                             template="plotly_white")
                fig.update_layout(margin=dict(t=40,b=10), title_font_size=14,
                                  xaxis_tickangle=-20)
                st.plotly_chart(fig, use_container_width=True); shown=True
        with c2:
            if not dept.empty:
                d2 = dept.copy()
                d2["total_cost"] = d2["reg_pay"] + d2["ot_pay"]
                fig2 = px.bar(d2, x="department", y=["reg_pay","ot_pay"],
                              barmode="stack", title="Labor Cost by Department",
                              color_discrete_map={"reg_pay":"#2196F3","ot_pay":"#FF5722"},
                              labels={"value":"Cost ($)","department":""},
                              template="plotly_white")
                fig2.update_layout(margin=dict(t=40,b=10), title_font_size=14,
                                   xaxis_tickangle=-20,
                                   legend=dict(title="",orientation="h",y=1.1))
                fig2.for_each_trace(lambda t: t.update(
                    name="Regular Pay" if t.name=="reg_pay" else "OT Pay"))
                st.plotly_chart(fig2, use_container_width=True); shown=True

        if not emp_ot.empty:
            top = emp_ot[emp_ot["ot_hours"]>0].nlargest(8,"ot_hours")
            if not top.empty:
                fig3 = px.bar(top, x="ot_hours", y="name", orientation="h",
                              title="Top OT Employees",
                              color="ot_hours",
                              color_continuous_scale=["#FFF9C4","#FF5722"],
                              labels={"ot_hours":"OT Hours","name":""},
                              template="plotly_white")
                fig3.update_layout(yaxis=dict(autorange="reversed"),
                                   margin=dict(t=40,b=10), title_font_size=14)
                st.plotly_chart(fig3, use_container_width=True); shown=True

    if not shown:
        st.info("No chart data available for the selected period.")


# ─────────────────────────────────────────────────────────────────────────────
# EMAIL UTILITIES
# ─────────────────────────────────────────────────────────────────────────────

def _df_to_html(df: pd.DataFrame, title: str = "") -> str:
    """Convert a DataFrame to a styled HTML table string."""
    if df is None or df.empty:
        return ""
    rows_html = ""
    for i, (_, row) in enumerate(df.iterrows()):
        bg = "#f9f8ff" if i % 2 == 0 else "#ffffff"
        cells = "".join(
            f'<td style="padding:7px 12px;border-bottom:1px solid #ede9fe;'
            f'font-size:12px;color:#374151;">{v}</td>'
            for v in row.values
        )
        rows_html += f'<tr style="background:{bg};">{cells}</tr>'
    headers = "".join(
        f'<th style="padding:8px 12px;background:#6366f1;color:#fff;'
        f'font-size:12px;font-weight:700;text-align:left;">{c}</th>'
        for c in df.columns
    )
    title_html = (
        f'<p style="font-size:13px;font-weight:700;color:#4c1d95;'
        f'margin:18px 0 6px;">{title}</p>' if title else ""
    )
    return (
        title_html +
        f'<table style="width:100%;border-collapse:collapse;border-radius:8px;'
        f'overflow:hidden;box-shadow:0 1px 6px rgba(0,0,0,.07);">'
        f'<thead><tr>{headers}</tr></thead>'
        f'<tbody>{rows_html}</tbody></table>'
    )


def _make_chart_pngs(intent: str, data: dict) -> list:
    """
    Generate up to 2 matplotlib chart PNGs for the email.
    Returns list of (cid_name, png_bytes) tuples.
    """
    charts = []
    dept   = data.get("dept", pd.DataFrame())

    PURPLE  = "#6366f1"
    RED     = "#ef4444"
    BLUE    = "#3b82f6"
    GREY    = "#e2e8f0"

    plt.rcParams.update({
        "font.family": "sans-serif",
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.spines.left": False,
        "axes.grid": True,
        "grid.color": "#f0f0f0",
        "grid.linewidth": 0.7,
    })

    def _save(fig) -> bytes:
        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=130, bbox_inches="tight",
                    facecolor="#ffffff")
        plt.close(fig)
        return buf.getvalue()

    # Chart 1: hours by department (horizontal bar)
    if not dept.empty and "department" in dept.columns:
        top = dept.head(10).copy()
        top = top.sort_values("total_hours")
        fig, ax = plt.subplots(figsize=(6, max(2.5, len(top) * 0.4 + 0.5)))
        colors  = [RED if h > 0 else PURPLE for h in top["ot_hours"]]
        ax.barh(top["department"], top["total_hours"], color=PURPLE, height=0.55,
                label="Regular Hours")
        ax.barh(top["department"], top["ot_hours"], color=RED, height=0.55,
                label="OT Hours", alpha=0.85)
        ax.set_xlabel("Hours", fontsize=10)
        ax.set_title("Hours by Department", fontsize=12, fontweight="bold",
                     color="#1a1a2e", pad=10)
        p_patch = mpatches.Patch(color=PURPLE, label="Total Hours")
        r_patch = mpatches.Patch(color=RED, label="OT Hours")
        ax.legend(handles=[p_patch, r_patch], fontsize=9, loc="lower right")
        fig.tight_layout()
        charts.append(("chart_dept", _save(fig)))

    # Chart 2a: OT by employee (if available)
    emp_ot = data.get("emp_ot", pd.DataFrame())
    if not emp_ot.empty and "ot_hours" in emp_ot.columns:
        top_ot = emp_ot[emp_ot["ot_hours"] > 0].nlargest(8, "ot_hours")
        if not top_ot.empty:
            fig, ax = plt.subplots(figsize=(6, max(2.5, len(top_ot) * 0.4 + 0.5)))
            vals = top_ot["ot_hours"].values
            names = top_ot["name"].values
            clrs = [RED if v >= 8 else "#f97316" for v in vals]
            ax.barh(names, vals, color=clrs, height=0.55)
            ax.set_xlabel("OT Hours", fontsize=10)
            ax.set_title("OT Hours by Employee", fontsize=12, fontweight="bold",
                         color="#1a1a2e", pad=10)
            ax.axvline(x=8, color="#6b7280", linewidth=1, linestyle="--",
                       label="8h mark")
            ax.legend(fontsize=9)
            ax.invert_yaxis()
            fig.tight_layout()
            charts.append(("chart_emp_ot", _save(fig)))
            return charts

    # Chart 2b: daily cost trend (if available)
    daily = data.get("daily", pd.DataFrame())
    if not daily.empty and "reg_pay" in daily.columns:
        fig, ax = plt.subplots(figsize=(6, 3))
        ax.fill_between(range(len(daily)), daily["reg_pay"], color=BLUE,
                        alpha=0.3, label="Regular Pay")
        ax.plot(range(len(daily)), daily["reg_pay"], color=BLUE, linewidth=1.5)
        ax.fill_between(range(len(daily)), daily["ot_pay"], color=RED,
                        alpha=0.3, label="OT Pay")
        ax.plot(range(len(daily)), daily["ot_pay"], color=RED, linewidth=1.5)
        tick_step = max(1, len(daily) // 7)
        ax.set_xticks(range(0, len(daily), tick_step))
        ax.set_xticklabels(
            [str(d)[:10] for d in daily["date"].iloc[::tick_step]],
            rotation=30, ha="right", fontsize=8)
        ax.set_ylabel("Cost ($)", fontsize=10)
        ax.set_title("Daily Labor Cost Trend", fontsize=12, fontweight="bold",
                     color="#1a1a2e", pad=10)
        ax.legend(fontsize=9)
        fig.tight_layout()
        charts.append(("chart_daily", _save(fig)))

    return charts


def _md_table_to_email_html(table_lines: list) -> str:
    """Convert a list of markdown table lines to a styled HTML table for email."""
    # Filter out separator lines (--- rows)
    data_lines = [l for l in table_lines
                  if not re.match(r'^\s*\|[\s\-:|]+\|\s*$', l)]
    if not data_lines:
        return ""

    rows = []
    for line in data_lines:
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        rows.append(cells)

    if not rows:
        return ""

    # First row = header
    header_cells = rows[0]
    thead = "".join(
        f'<th style="padding:9px 14px;background:#6366f1;color:#ffffff;'
        f'font-size:12px;font-weight:700;text-align:left;white-space:nowrap;">'
        f'{c}</th>'
        for c in header_cells
    )

    tbody = ""
    for i, row in enumerate(rows[1:]):
        bg = "#f5f3ff" if i % 2 == 0 else "#ffffff"
        cells_html = "".join(
            f'<td style="padding:8px 14px;border-bottom:1px solid #ede9fe;'
            f'font-size:12px;color:#374151;white-space:nowrap;">{c}</td>'
            for c in row
        )
        tbody += f'<tr style="background:{bg};">{cells_html}</tr>'

    return (
        f'<div style="overflow-x:auto;margin:12px 0;">'
        f'<table style="border-collapse:collapse;min-width:100%;'
        f'border-radius:8px;overflow:hidden;'
        f'box-shadow:0 1px 8px rgba(99,102,241,.12);">'
        f'<thead><tr>{thead}</tr></thead>'
        f'<tbody>{tbody}</tbody>'
        f'</table></div>'
    )


def _md_table_to_df(table_lines: list) -> pd.DataFrame:
    """Convert markdown table lines to DataFrame (for Excel export)."""
    data_lines = [l for l in table_lines
                  if not re.match(r'^\s*\|[\s\-:|]+\|\s*$', l)]
    if len(data_lines) < 2:
        return pd.DataFrame()
    headers = [c.strip() for c in data_lines[0].strip().strip("|").split("|")]
    rows = []
    for line in data_lines[1:]:
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if cells:
            rows.append(cells)
    if not rows:
        return pd.DataFrame(columns=headers)
    max_c = max(len(r) for r in rows)
    headers = (headers + [""] * max_c)[:max_c]
    return pd.DataFrame(rows, columns=headers)


def _summary_to_email_html(summary: str) -> tuple:
    """
    Parse AI summary into email-safe HTML.
    Returns (html_string, list_of_table_dataframes).
    Converts markdown tables to proper styled HTML tables.
    Escapes $ signs. Renders bullets and bold properly.
    """
    import html as _html

    lines    = re.sub(r'\n{3,}', '\n\n', summary.strip()).splitlines()
    html_out = []
    dfs      = []  # extracted table DataFrames for Excel export
    in_table = False
    tbl_buf  = []
    in_list  = False

    def flush_table():
        nonlocal in_table, tbl_buf
        if tbl_buf:
            html_out.append(_md_table_to_email_html(tbl_buf))
            df = _md_table_to_df(tbl_buf)
            if not df.empty:
                dfs.append(df)
        tbl_buf  = []
        in_table = False

    def flush_list():
        nonlocal in_list
        if in_list:
            html_out.append('</ul>')
            in_list = False

    for line in lines:
        is_table_row = bool(re.match(r'^\s*\|', line))

        if is_table_row:
            flush_list()
            in_table = True
            tbl_buf.append(line)
            continue

        if in_table:
            flush_table()

        # Escape HTML, then fix $ → &#36;
        safe = _html.escape(line, quote=False).replace("$", "&#36;")
        # Bold
        safe = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', safe)

        is_bullet = bool(re.match(r'^\s*([-*•]|\d+\.)\s+', line))

        if is_bullet:
            safe = re.sub(r'^\s*([-*•]|\d+\.)\s+', '', safe)
            if not in_list:
                html_out.append(
                    '<ul style="margin:8px 0 10px 22px;padding:0;">'
                )
                in_list = True
            html_out.append(
                f'<li style="margin:5px 0;font-size:13px;line-height:1.75;'
                f'color:#1f2937;">{safe}</li>'
            )
        else:
            flush_list()
            if not safe.strip():
                html_out.append('<div style="height:6px;"></div>')
            else:
                html_out.append(
                    f'<p style="margin:0 0 7px;font-size:13px;line-height:1.8;'
                    f'color:#1f2937;">{safe}</p>'
                )

    flush_list()
    if in_table:
        flush_table()

    return "\n".join(html_out), dfs


def _make_excel_bytes(sheets: dict) -> bytes:
    """Generate in-memory Excel workbook bytes from {sheet_name: DataFrame}."""
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            safe_name = sheet_name[:31]  # Excel tab name max length
            df.to_excel(writer, sheet_name=safe_name, index=False)
            ws = writer.sheets[safe_name]
            # Auto-fit columns
            for col_cells in ws.columns:
                max_len = max(
                    (len(str(c.value)) for c in col_cells if c.value), default=8
                )
                ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 4, 40)
    return buf.getvalue()


def build_email_html(hotel: str, question: str,
                     start: date, end: date,
                     summary: str, data: dict,
                     th: float, tot: float, tc: float,
                     op: float, emp: int, ot_pct: float,
                     chart_cids: list) -> str:
    """Build full HTML email body."""
    period = f"{start.strftime('%B %d')} – {end.strftime('%B %d, %Y')}"
    today  = date.today().strftime("%B %d, %Y")

    # KPI pills
    kpi_html = f"""
    <table style="border-collapse:collapse;margin:0 0 20px;">
      <tr>
        <td style="padding:10px 18px;background:#f5f3ff;border:1px solid #ddd6fe;
                   border-radius:8px;text-align:center;margin:4px;">
          <div style="font-size:22px;font-weight:800;color:#6366f1;">{th:,.0f}</div>
          <div style="font-size:11px;color:#6b7280;">Total Hours</div>
        </td>
        <td style="width:8px;"></td>
        <td style="padding:10px 18px;background:#fff5f5;border:1px solid #fecaca;
                   border-radius:8px;text-align:center;">
          <div style="font-size:22px;font-weight:800;color:#ef4444;">{tot:,.1f}</div>
          <div style="font-size:11px;color:#6b7280;">OT Hours ({ot_pct:.1f}%)</div>
        </td>
        <td style="width:8px;"></td>
        <td style="padding:10px 18px;background:#f0fdf4;border:1px solid #bbf7d0;
                   border-radius:8px;text-align:center;">
          <div style="font-size:22px;font-weight:800;color:#16a34a;">${tc:,.0f}</div>
          <div style="font-size:11px;color:#6b7280;">Total Labor Cost</div>
        </td>
        <td style="width:8px;"></td>
        <td style="padding:10px 18px;background:#fff7ed;border:1px solid #fed7aa;
                   border-radius:8px;text-align:center;">
          <div style="font-size:22px;font-weight:800;color:#ea580c;">${op:,.0f}</div>
          <div style="font-size:11px;color:#6b7280;">OT Pay</div>
        </td>
        <td style="width:8px;"></td>
        <td style="padding:10px 18px;background:#f0f9ff;border:1px solid #bae6fd;
                   border-radius:8px;text-align:center;">
          <div style="font-size:22px;font-weight:800;color:#0284c7;">{emp}</div>
          <div style="font-size:11px;color:#6b7280;">Employees</div>
        </td>
      </tr>
    </table>"""

    # AI summary — parse prose + markdown tables into proper HTML
    summary_html, _extracted_dfs = _summary_to_email_html(summary)

    # Data tables
    table_labels = [
        ("Department Breakdown", "dept"),
        ("Employee Detail", "emp_ot"),
        ("Position Breakdown", "position"),
        ("OT Risk", "ot_risk"),
        ("Schedule", "schedule"),
        ("Headcount by Day", "headcount_daily"),
    ]
    tables_html = ""
    for label, key in table_labels:
        df = data.get(key)
        if df is not None and not df.empty:
            tables_html += _df_to_html(df.head(30), title=label)

    # Chart images
    charts_html = ""
    for cid, _ in chart_cids:
        charts_html += (
            f'<img src="cid:{cid}" style="max-width:100%;border-radius:10px;'
            f'margin:8px 0 16px;box-shadow:0 2px 10px rgba(0,0,0,.08);" /><br/>'
        )

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f3f4f6;font-family:Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0"
         style="background:#f3f4f6;padding:30px 0;">
    <tr><td align="center">
      <table width="640" cellpadding="0" cellspacing="0"
             style="background:#ffffff;border-radius:14px;
                    box-shadow:0 4px 24px rgba(0,0,0,.08);overflow:hidden;">

        <!-- Header -->
        <tr>
          <td style="background:linear-gradient(135deg,#6366f1,#a855f7);
                     padding:28px 32px;">
            <table><tr>
              <td style="padding-right:12px;">
                <table cellspacing="3"><tr>
                  <td style="width:14px;height:14px;background:#fff;border-radius:3px;opacity:.9;"></td>
                  <td style="width:14px;height:14px;background:#ff4444;border-radius:3px;"></td>
                </tr><tr>
                  <td style="width:14px;height:14px;background:#fff;border-radius:3px;opacity:.9;"></td>
                  <td style="width:14px;height:14px;background:#fff;border-radius:3px;opacity:.9;"></td>
                </tr></table>
              </td>
              <td>
                <div style="font-size:22px;font-weight:800;color:#fff;
                            letter-spacing:-.5px;">AIPilot</div>
                <div style="font-size:12px;color:rgba(255,255,255,.75);
                            margin-top:2px;">Labor Intelligence Report</div>
              </td>
            </tr></table>
          </td>
        </tr>

        <!-- Meta bar -->
        <tr>
          <td style="background:#faf5ff;padding:12px 32px;
                     border-bottom:1px solid #ede9fe;">
            <span style="font-size:12px;color:#6b7280;">
              <strong style="color:#4c1d95;">Hotel:</strong> {hotel} &nbsp;·&nbsp;
              <strong style="color:#4c1d95;">Period:</strong> {period} &nbsp;·&nbsp;
              <strong style="color:#4c1d95;">Generated:</strong> {today}
            </span>
          </td>
        </tr>

        <!-- Question -->
        <tr>
          <td style="padding:20px 32px 4px;">
            <div style="background:#f5f3ff;border-left:4px solid #6366f1;
                        border-radius:0 8px 8px 0;padding:10px 16px;">
              <span style="font-size:11px;font-weight:700;color:#7c3aed;
                           text-transform:uppercase;letter-spacing:.5px;">Question</span>
              <div style="font-size:14px;color:#1f2937;margin-top:4px;">
                {question}
              </div>
            </div>
          </td>
        </tr>

        <!-- KPIs -->
        <tr><td style="padding:20px 32px 0;">{kpi_html}</td></tr>

        <!-- AI Summary -->
        <tr>
          <td style="padding:0 32px 10px;">
            <div style="font-size:15px;font-weight:700;color:#1a1a2e;
                        margin-bottom:10px;">
              ✦ Executive Summary
              <span style="font-size:11px;font-weight:600;
                           background:linear-gradient(135deg,#6366f1,#a855f7);
                           color:#fff;padding:2px 9px;border-radius:20px;
                           margin-left:8px;">AI · Llama 3.3 70B</span>
            </div>
            <div style="background:#f8f9ff;border:1px solid #e0e4f0;
                        border-left:4px solid #6366f1;border-radius:10px;
                        padding:18px 20px;">
              {summary_html}
            </div>
          </td>
        </tr>

        <!-- Charts -->
        {f'<tr><td style="padding:10px 32px 0;"><div style="font-size:15px;font-weight:700;color:#1a1a2e;margin-bottom:10px;">Supporting Charts</div>{charts_html}</td></tr>' if charts_html else ''}

        <!-- Tables -->
        {f'<tr><td style="padding:10px 32px 0;"><div style="font-size:15px;font-weight:700;color:#1a1a2e;margin-bottom:6px;">Data Tables</div>{tables_html}</td></tr>' if tables_html else ''}

        <!-- Footer -->
        <tr>
          <td style="background:#f9fafb;border-top:1px solid #e5e7eb;
                     padding:18px 32px;text-align:center;">
            <span style="font-size:11px;color:#9ca3af;">
              Sent by <strong>LaborPilot AIPilot</strong> &nbsp;·&nbsp;
              For internal use only &nbsp;·&nbsp;
              Log in to view interactive charts
            </span>
          </td>
        </tr>

      </table>
    </td></tr>
  </table>
</body>
</html>"""


def send_aipilot_email(recipients: list, hotel: str, question: str,
                       start: date, end: date, summary: str, data: dict,
                       th: float, tot: float, tc: float,
                       op: float, emp: int, ot_pct: float) -> tuple:
    """
    Build and send the AIPilot report email.
    Returns (success: bool, message: str).
    MIME structure:
      multipart/mixed
        multipart/related
          multipart/alternative  (plain text + HTML)
          image/png × N          (inline CID charts)
        application/xlsx         (Excel attachment, if tables exist)
    """
    try:
        intent    = st.session_state.get("aipilot_last_result", {}).get("intent", "general")
        period    = f"{start.strftime('%b %d')} – {end.strftime('%b %d, %Y')}"
        filename  = f"LaborPilot_{hotel.replace(' ','_')}_{start.strftime('%Y%m%d')}.xlsx"
        subject   = f"AIPilot Labor Insight — {hotel} — {period}"

        # ── Charts (inline PNG) ──
        chart_cids = _make_chart_pngs(intent, data)

        # ── HTML body ──
        html_body = build_email_html(
            hotel, question, start, end, summary, data,
            th, tot, tc, op, emp, ot_pct, chart_cids
        )

        # ── Plain text fallback ──
        plain = (
            f"AIPilot Labor Report — {hotel}\n"
            f"Period: {period}\n\n"
            f"Question: {question}\n\n"
            f"=== KEY METRICS ===\n"
            f"Total Hours: {th:,.0f}h  |  OT Hours: {tot:,.1f}h ({ot_pct:.1f}%)\n"
            f"Total Cost: ${tc:,.2f}  |  OT Pay: ${op:,.2f}  |  Employees: {emp}\n\n"
            f"=== EXECUTIVE SUMMARY ===\n{summary}\n\n"
            f"Log into LaborPilot for interactive charts and full analytics."
        )

        # ── Excel attachment — collect sheets ──
        excel_sheets = {}

        # Tables extracted from the AI-generated markdown in the summary
        _, ai_tables = _summary_to_email_html(summary)
        for i, df in enumerate(ai_tables, 1):
            if not df.empty:
                label = "Schedule" if i == 1 else f"Table_{i}"
                # Try to guess a better name from first column header
                if df.columns[0].lower() in ("department", "dept"):
                    label = f"Schedule_{i}"
                excel_sheets[label] = df

        # Standard data tables from the DB fetch
        db_table_map = {
            "Department":  "dept",
            "Employee OT": "emp_ot",
            "Position":    "position",
            "OT Risk":     "ot_risk",
            "Schedule":    "schedule",
            "Headcount":   "headcount_daily",
        }
        for label, key in db_table_map.items():
            df = data.get(key)
            if df is not None and not df.empty:
                # Don't duplicate if already added from AI tables
                if label not in excel_sheets:
                    excel_sheets[label] = df

        # ── Build MIME structure ──
        # outer: multipart/mixed  (for file attachments)
        outer = MIMEMultipart("mixed")
        outer["From"]    = EMAIL_SENDER
        outer["To"]      = ", ".join(recipients)
        outer["Subject"] = subject

        # inner: multipart/related  (for inline images)
        related = MIMEMultipart("related")

        # innermost: multipart/alternative  (plain + html)
        alt = MIMEMultipart("alternative")
        alt.attach(MIMEText(plain, "plain"))
        alt.attach(MIMEText(html_body, "html"))
        related.attach(alt)

        # Inline chart images (CID)
        for cid, png_bytes in chart_cids:
            img_part = MIMEImage(png_bytes, _subtype="png")
            img_part.add_header("Content-ID", f"<{cid}>")
            img_part.add_header("Content-Disposition", "inline",
                                filename=f"{cid}.png")
            related.attach(img_part)

        outer.attach(related)

        # Excel attachment
        if excel_sheets:
            xlsx_bytes = _make_excel_bytes(excel_sheets)
            from email.mime.base import MIMEBase
            from email import encoders as _enc
            xlsx_part = MIMEBase(
                "application",
                "vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            xlsx_part.set_payload(xlsx_bytes)
            _enc.encode_base64(xlsx_part)
            xlsx_part.add_header(
                "Content-Disposition", "attachment", filename=filename
            )
            outer.attach(xlsx_part)

        # ── Send ──
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as smtp:
            smtp.login(EMAIL_SENDER, EMAIL_PASSWORD)
            smtp.sendmail(EMAIL_SENDER, recipients, outer.as_string())

        attach_note = f" + Excel attachment ({len(excel_sheets)} sheet(s))" if excel_sheets else ""
        return True, f"Report sent to {', '.join(recipients)}{attach_note}"

    except Exception as e:
        return False, f"Failed to send: {e}"


# ─────────────────────────────────────────────────────────────────────────────
# MAIN PAGE
# ─────────────────────────────────────────────────────────────────────────────
def render_aipilot(hotel: str):
    st.markdown("""
    <style>
    /* ── AI card ── */
    .ai-card {
        background: linear-gradient(135deg,#f8f9ff,#ffffff);
        border: 1px solid #e0e4f0;
        border-left: 4px solid #6366f1;
        border-radius: 12px;
        padding: 22px 26px;
        margin: 8px 0 14px;
        font-size: 15px;
        line-height: 1.85;
        color: #1a1a2e;
        white-space: pre-wrap;
        box-shadow: 0 2px 16px rgba(99,102,241,.07);
    }
    .ai-badge {
        background: linear-gradient(135deg,#6366f1,#a855f7);
        color:#fff; font-size:11px; font-weight:700;
        padding:3px 10px; border-radius:20px; letter-spacing:.5px;
    }
    .intent-tag {
        display:inline-block; background:#ede9fe; color:#6d28d9;
        font-size:11px; font-weight:700; padding:2px 10px;
        border-radius:12px; margin-left:8px;
    }
    .mp { display:inline-block; background:#f5f3ff; border:1px solid #ddd6fe;
          border-radius:8px; padding:8px 16px; margin:4px; text-align:center; }
    .mp .v { font-size:22px; font-weight:800; color:#6366f1; }
    .mp .l { font-size:11px; color:#666; margin-top:2px; }

    /* ── ChatGPT-style textarea ── */
    div[data-testid="stTextArea"] textarea {
        border-radius: 16px !important;
        border: 1.5px solid #e2e8f0 !important;
        padding: 16px 20px !important;
        font-size: 15px !important;
        line-height: 1.6 !important;
        box-shadow: 0 4px 24px rgba(0,0,0,0.06) !important;
        background: #ffffff !important;
        transition: border-color 0.2s, box-shadow 0.2s !important;
        resize: none !important;
    }
    div[data-testid="stTextArea"] textarea:focus {
        border-color: #818cf8 !important;
        box-shadow: 0 0 0 4px rgba(99,102,241,0.12), 0 4px 24px rgba(0,0,0,0.06) !important;
        outline: none !important;
    }
    div[data-testid="stTextArea"] label {
        font-size: 13px !important;
        font-weight: 600 !important;
        color: #64748b !important;
        letter-spacing: 0.3px !important;
    }

    /* ── AI Generate button ── */
    div[data-testid="stButton"] > button[kind="primary"] {
        background: linear-gradient(135deg, #6366f1 0%, #8b5cf6 50%, #a855f7 100%) !important;
        border: none !important;
        border-radius: 14px !important;
        font-weight: 700 !important;
        font-size: 14px !important;
        letter-spacing: 0.4px !important;
        padding: 12px 28px !important;
        box-shadow: 0 4px 20px rgba(99,102,241,0.45) !important;
        color: #fff !important;
        transition: all 0.2s ease !important;
    }
    div[data-testid="stButton"] > button[kind="primary"]:hover {
        box-shadow: 0 6px 28px rgba(99,102,241,0.6) !important;
        transform: translateY(-1px) !important;
    }
    </style>
    """, unsafe_allow_html=True)

    # ── Header ──
    st.markdown("""
    <div style="display:flex;align-items:center;gap:12px;margin-bottom:6px;">
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:4px;">
            <div style="width:18px;height:18px;background:#3a3a3a;border-radius:4px;"></div>
            <div style="width:18px;height:18px;background:#e02020;border-radius:4px;"></div>
            <div style="width:18px;height:18px;background:#3a3a3a;border-radius:4px;"></div>
            <div style="width:18px;height:18px;background:#3a3a3a;border-radius:4px;"></div>
        </div>
        <div>
            <span style="font-size:22px;font-weight:800;color:#1a1a2e;letter-spacing:-.5px;">AIPilot</span>
            <span style="font-size:13px;color:#888;margin-left:8px;">Labor Intelligence</span>
        </div>
    </div>
    <p style="color:#64748b;font-size:13px;margin-bottom:20px;">
        Ask any labor question in plain English — OT, risk, cost, scheduling, comparisons.
        Include the time period you want (e.g. "last week", "this month", "YTD").
    </p>
    """, unsafe_allow_html=True)

    # ── Model selector ──
    MODELS = {
        "llama-3.3-70b-versatile":                        "⚡ Llama 3.3 70B Versatile  — Recommended. Fast, balanced, great for all labor reports.",
        "meta-llama/llama-4-maverick-17b-128e-instruct":  "🦅 Llama 4 Maverick 17B  — Newest & smartest. Best quality responses.",
        "meta-llama/llama-4-scout-17b-16e-instruct":      "🔭 Llama 4 Scout 17B  — Newer model, fast & capable.",
        "qwen-qwq-32b":                                   "🧠 Qwen QwQ 32B  — Deep reasoning. Best for complex analysis & comparisons.",
        "gemma2-9b-it":                                   "💎 Gemma 2 9B  — Google model. Fast, reliable, low token usage.",
        "llama-3.1-8b-instant":                           "🪶 Llama 3.1 8B Instant  — Fastest & most token-efficient. Saves daily quota.",
    }
    selected_model = st.selectbox(
        "AI Model",
        options=list(MODELS.keys()),
        format_func=lambda m: MODELS[m],
        index=list(MODELS.keys()).index(
            st.session_state.get("aipilot_model", "llama-3.3-70b-versatile")
        ),
        key="_model_select",
    )
    st.session_state["aipilot_model"] = selected_model

    st.markdown("<div style='margin-top:12px;'></div>", unsafe_allow_html=True)

    # ── ChatGPT-style input area ──
    question = st.text_area(
        "Ask a labor question",
        placeholder=(
            "e.g.  What was our overtime last week?   ·   "
            "Is anyone at OT risk this week?   ·   "
            "Compare this month's labor cost to last month   ·   "
            "Create a mockup schedule for this week"
        ),
        height=110,
        key="ai_question",
    )

    run_btn = st.button("✦ Generate Insight", type="primary")

    # ── PHASE 1: Generate (only when button clicked) ──
    if run_btn:
        if not question.strip():
            st.warning("Please enter a question.")
        else:
            intents    = detect_intents(question)
            intent     = detect_intent(question)   # primary intent for chart/display
            start_date, end_date = parse_date_range(question)

            with st.spinner("Fetching your data..."):
                try:
                    data = fetch_data(hotel, start_date, end_date, intents)
                except Exception as e:
                    st.error(f"Database error: {e}")
                    st.stop()

            t      = data["totals"].iloc[0] if not data["totals"].empty else {}
            th     = float(t.get("total_hours", 0))
            tot    = float(t.get("total_ot", 0))
            rp     = float(t.get("reg_pay", 0))
            op     = float(t.get("ot_pay", 0))
            tc     = rp + op
            emp    = int(t.get("employees", 0))
            ot_pct = (tot / th * 100) if th else 0

            with st.spinner("Generating executive summary..."):
                try:
                    prompt  = build_prompt(hotel, start_date, end_date,
                                          question, intents, data)
                    summary = call_groq(prompt)
                except GroqRateLimitError as e:
                    st.markdown(f"""
                    <div style="background:#fff7ed;border:1px solid #fed7aa;
                                border-left:4px solid #f97316;border-radius:10px;
                                padding:18px 22px;margin-top:16px;">
                        <div style="font-size:15px;font-weight:700;color:#c2410c;
                                    margin-bottom:6px;">⏳ AI Token Limit Reached</div>
                        <div style="font-size:13px;color:#9a3412;line-height:1.7;">
                            {str(e)}<br><br>
                            The free Groq tier allows 100,000 tokens per day.
                            Tokens reset every 24 hours — try again shortly or
                            <a href="https://console.groq.com/settings/billing"
                               target="_blank" style="color:#c2410c;font-weight:600;">
                               upgrade your Groq plan</a> for higher limits.
                        </div>
                    </div>""", unsafe_allow_html=True)
                    st.stop()
                except Exception as e:
                    st.error(f"AI error: {e}")
                    st.stop()

            # Persist everything — survives any subsequent button click
            st.session_state["aipilot_last_result"] = {
                "hotel": hotel, "question": question,
                "start": start_date, "end": end_date,
                "summary": summary, "data": data,
                "th": th, "tot": tot, "tc": tc,
                "op": op, "emp": emp, "ot_pct": ot_pct,
                "intent": intent,
                "intents": intents,
                "model": selected_model,
            }

    # ── PHASE 2: Display (always, if results exist in session state) ──
    r = st.session_state.get("aipilot_last_result")

    if not r:
        # No results yet — show placeholder
        st.markdown("""
        <div style="background:linear-gradient(135deg,#faf5ff,#f0f4ff);
                    border:1px solid #e9d5ff;border-radius:14px;
                    padding:32px;text-align:center;margin-top:20px;">
            <div style="font-size:36px;margin-bottom:10px;">✦</div>
            <div style="font-size:15px;font-weight:700;color:#4c1d95;margin-bottom:6px;">
                Your AI Labor Advisor is ready
            </div>
            <div style="font-size:13px;color:#7c3aed;line-height:1.9;">
                "What was our overtime last week?" &nbsp;·&nbsp;
                "Is anyone at OT risk this week?" &nbsp;·&nbsp;
                "Which department cost the most this month?"
            </div>
        </div>""", unsafe_allow_html=True)
        return

    # Unpack stored result
    summary    = r["summary"]
    data       = r["data"]
    intent     = r["intent"]
    start_date = r["start"]
    end_date   = r["end"]
    th, tot, tc, op, emp, ot_pct = (
        r["th"], r["tot"], r["tc"], r["op"], r["emp"], r["ot_pct"]
    )

    # Period / intent tag
    st.markdown(
        f'<span style="font-size:12px;color:#888;">Period: '
        f'<b>{start_date.strftime("%b %d")} – {end_date.strftime("%b %d, %Y")}</b></span>'
        f'<span class="intent-tag">{intent.upper()}</span>',
        unsafe_allow_html=True,
    )

    # ── KPI Pills ──
    if th > 0:
        st.markdown(f"""
        <div style="display:flex;flex-wrap:wrap;gap:0;margin:14px 0 6px;">
            <div class="mp"><div class="v">{th:,.0f}</div><div class="l">Total Hours</div></div>
            <div class="mp"><div class="v" style="color:#e53935;">{tot:,.1f}</div><div class="l">OT Hours ({ot_pct:.1f}%)</div></div>
            <div class="mp"><div class="v">${tc:,.0f}</div><div class="l">Total Labor Cost</div></div>
            <div class="mp"><div class="v" style="color:#e53935;">${op:,.0f}</div><div class="l">OT Pay</div></div>
            <div class="mp"><div class="v">{emp}</div><div class="l">Employees</div></div>
        </div>
        """, unsafe_allow_html=True)

    # ── AI Summary ──
    used_model = r.get("model", "llama-3.3-70b-versatile")
    model_label = {
        "llama-3.3-70b-versatile":                       "Llama 3.3 70B",
        "meta-llama/llama-4-maverick-17b-128e-instruct": "Llama 4 Maverick",
        "meta-llama/llama-4-scout-17b-16e-instruct":     "Llama 4 Scout",
        "qwen-qwq-32b":                                  "Qwen QwQ 32B",
        "gemma2-9b-it":                                  "Gemma 2 9B",
        "llama-3.1-8b-instant":                          "Llama 3.1 8B",
    }.get(used_model, used_model)
    st.markdown(
        '<div style="display:flex;align-items:center;gap:10px;margin-top:18px;">'
        '<span style="font-size:16px;font-weight:700;color:#1a1a2e;">Executive Summary</span>'
        f'<span class="ai-badge">AI · {model_label}</span></div>',
        unsafe_allow_html=True,
    )
    render_ai_response(summary)

    # ── Charts ──
    st.markdown("---")
    st.markdown("#### Supporting Analysis")
    render_charts(intent, data)

    # ── Raw data expander ──
    with st.expander("View Raw Data"):
        labels = [("Department", "dept"), ("Employee Detail", "emp_ot"),
                  ("Position", "position"), ("Daily Trend", "daily"),
                  ("OT Risk", "ot_risk"), ("Schedule", "schedule"),
                  ("Headcount", "headcount_daily"), ("Roster", "roster"),
                  ("Rooms", "rooms"), ("Prev Totals", "prev_totals")]
        for label, key in labels:
            df = data.get(key)
            if df is not None and not df.empty:
                st.markdown(f"**{label}**")
                st.dataframe(df, use_container_width=True, hide_index=True)

    # ── Email Report Section ──
    st.markdown("---")
    st.markdown("""
    <div style="display:flex;align-items:center;gap:10px;margin-bottom:12px;">
        <span style="font-size:20px;">✉️</span>
        <span style="font-size:15px;font-weight:700;color:#1a1a2e;">Email This Report</span>
    </div>
    <p style="font-size:13px;color:#64748b;margin:0 0 14px;">
        Sends the executive summary, KPI metrics, data tables, and supporting charts
        directly to any email address.
    </p>
    """, unsafe_allow_html=True)

    email_input = st.text_input(
        "Recipient email address(es)",
        placeholder="manager@hotel.com, gm@hotel.com",
        key="aipilot_email_to",
        label_visibility="collapsed",
    )

    send_col, _ = st.columns([1, 3])
    with send_col:
        send_clicked = st.button("✉️ Send Report", key="aipilot_send_email")

    if send_clicked:
        raw_addrs = [e.strip() for e in email_input.split(",") if e.strip()]
        if not raw_addrs:
            st.warning("Please enter at least one recipient email address.")
        else:
            with st.spinner("Building and sending email..."):
                ok, msg = send_aipilot_email(
                    recipients=raw_addrs,
                    hotel=r["hotel"],
                    question=r["question"],
                    start=r["start"],
                    end=r["end"],
                    summary=r["summary"],
                    data=r["data"],
                    th=r["th"], tot=r["tot"], tc=r["tc"],
                    op=r["op"], emp=r["emp"], ot_pct=r["ot_pct"],
                )
            if ok:
                st.success(f"✅ {msg}")
            else:
                st.error(f"❌ {msg}")
