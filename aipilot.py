"""
aipilot.py — AI-powered labor intelligence for LaborPilot
Dynamic, conversational, executive-grade insights from your real data.
"""

import os
import re
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date, timedelta, datetime
from groq import Groq
from sqlalchemy import text
from db import ENGINE


# ── Natural-language date parser ─────────────────────────────────────────────
def parse_dates_from_question(question: str):
    """Infer a (start, end) date range from natural language in the question."""
    q = question.lower()
    today = date.today()

    if "today" in q:
        return today, today
    if "yesterday" in q:
        d = today - timedelta(days=1)
        return d, d
    if "this week" in q:
        start = today - timedelta(days=today.weekday())
        return start, today
    if "last week" in q:
        start = today - timedelta(days=today.weekday() + 7)
        end   = start + timedelta(days=6)
        return start, end
    if "last 7 days" in q or "past 7 days" in q or "last week" in q:
        return today - timedelta(days=6), today
    if "last 14 days" in q or "past 14 days" in q or "two weeks" in q:
        return today - timedelta(days=13), today
    if "this month" in q:
        start = today.replace(day=1)
        return start, today
    if "last month" in q:
        first_this = today.replace(day=1)
        last_prev  = first_this - timedelta(days=1)
        start      = last_prev.replace(day=1)
        return start, last_prev
    if "last 30 days" in q or "past 30 days" in q or "past month" in q:
        return today - timedelta(days=29), today
    if "last 60 days" in q or "past 60 days" in q or "two months" in q:
        return today - timedelta(days=59), today
    if "last 90 days" in q or "quarter" in q or "past quarter" in q:
        return today - timedelta(days=89), today
    if "this year" in q or "ytd" in q or "year to date" in q:
        return today.replace(month=1, day=1), today

    # Try to find explicit month names
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
            import calendar
            last_day = calendar.monthrange(year, month_num)[1]
            return date(year, month_num, 1), date(year, month_num, last_day)

    # Default: last 30 days
    return today - timedelta(days=29), today


# ── Dynamic data fetching based on question intent ───────────────────────────
def detect_intent(question: str) -> list:
    """Detect what the user is asking about to pull the right data."""
    q = question.lower()
    intents = []
    if any(w in q for w in ["overtime", "ot ", "ot,", "overti"]):
        intents.append("overtime")
    if any(w in q for w in ["cost", "pay", "spend", "budget", "wage", "dollar", "expensive"]):
        intents.append("cost")
    if any(w in q for w in ["department", "dept", "housekeeping", "front desk", "food", "f&b", "engineering"]):
        intents.append("department")
    if any(w in q for w in ["employee", "staff", "worker", "who", "top", "highest", "most"]):
        intents.append("employee")
    if any(w in q for w in ["trend", "daily", "day by day", "week by week", "over time", "pattern"]):
        intents.append("trend")
    if any(w in q for w in ["schedule", "scheduled", "shift", "roster"]):
        intents.append("schedule")
    if any(w in q for w in ["room", "occupancy", "occ", "occupied"]):
        intents.append("rooms")
    if not intents:
        intents = ["overtime", "cost", "department", "employee", "trend"]
    return intents


def _query(sql: str, params: dict) -> pd.DataFrame:
    with ENGINE.connect() as conn:
        return pd.read_sql_query(text(sql), conn, params=params)


def fetch_data(hotel: str, start: date, end: date, intents: list) -> dict:
    p = {"hotel": hotel, "start": str(start), "end": str(end)}
    result = {}

    # Always fetch totals
    result["totals"] = _query("""
        SELECT COALESCE(SUM(a.hours), 0)    AS total_hours,
               COALESCE(SUM(a.ot_hours), 0) AS total_ot_hours,
               COALESCE(SUM(a.reg_pay), 0)  AS total_reg_pay,
               COALESCE(SUM(a.ot_pay), 0)   AS total_ot_pay,
               COUNT(DISTINCT a.emp_id)      AS unique_employees,
               COUNT(DISTINCT a.date)        AS active_days
        FROM   actual a
        WHERE  a.hotel_name = :hotel
          AND  a.date BETWEEN :start AND :end
    """, p)

    if "department" in intents or "cost" in intents or "overtime" in intents:
        result["by_dept"] = _query("""
            SELECT e.department,
                   COALESCE(SUM(a.hours), 0)    AS total_hours,
                   COALESCE(SUM(a.ot_hours), 0) AS ot_hours,
                   COALESCE(SUM(a.reg_pay), 0)  AS reg_pay,
                   COALESCE(SUM(a.ot_pay), 0)   AS ot_pay,
                   COUNT(DISTINCT a.emp_id)      AS employees
            FROM   actual a
            JOIN   employee e ON e.id = a.emp_id AND e.hotel_name = :hotel
            WHERE  a.hotel_name = :hotel
              AND  a.date BETWEEN :start AND :end
            GROUP  BY e.department
            ORDER  BY total_hours DESC
        """, p)

    if "employee" in intents or "overtime" in intents:
        result["top_employees"] = _query("""
            SELECT e.name, e.department,
                   COALESCE(SUM(a.hours), 0)    AS total_hours,
                   COALESCE(SUM(a.ot_hours), 0) AS ot_hours,
                   COALESCE(SUM(a.reg_pay + a.ot_pay), 0) AS total_pay
            FROM   actual a
            JOIN   employee e ON e.id = a.emp_id AND e.hotel_name = :hotel
            WHERE  a.hotel_name = :hotel
              AND  a.date BETWEEN :start AND :end
            GROUP  BY e.name, e.department
            ORDER  BY ot_hours DESC, total_hours DESC
            LIMIT  12
        """, p)

    if "trend" in intents or "cost" in intents or "overtime" in intents:
        result["daily"] = _query("""
            SELECT a.date,
                   COALESCE(SUM(a.hours), 0)              AS total_hours,
                   COALESCE(SUM(a.ot_hours), 0)            AS ot_hours,
                   COALESCE(SUM(a.reg_pay + a.ot_pay), 0)  AS total_cost
            FROM   actual a
            WHERE  a.hotel_name = :hotel
              AND  a.date BETWEEN :start AND :end
            GROUP  BY a.date
            ORDER  BY a.date
        """, p)

    if "schedule" in intents:
        result["schedule"] = _query("""
            SELECT s.day AS date,
                   COUNT(DISTINCT s.emp_id) AS headcount,
                   s.shift_type
            FROM   schedule s
            WHERE  s.hotel_name = :hotel
              AND  s.day BETWEEN :start AND :end
            GROUP  BY s.day, s.shift_type
            ORDER  BY s.day
        """, p)

    if "rooms" in intents:
        result["rooms"] = _query("""
            SELECT date, occupied
            FROM   rooms
            WHERE  hotel_name = :hotel
              AND  date BETWEEN :start AND :end
            ORDER  BY date
        """, p)

    return result


def build_dynamic_prompt(hotel: str, start: date, end: date,
                          question: str, data: dict, intents: list) -> str:
    today = date.today()
    days = (end - start).days + 1

    # Format date label naturally
    if start == end:
        period_label = start.strftime("%B %d, %Y")
    elif start == today - timedelta(days=6) and end == today:
        period_label = "the last 7 days"
    elif start == today.replace(day=1):
        period_label = f"this month ({start.strftime('%B %Y')})"
    else:
        period_label = f"{start.strftime('%b %d')} – {end.strftime('%b %d, %Y')}"

    sections = []

    # Totals
    t = data.get("totals", pd.DataFrame())
    if not t.empty:
        r = t.iloc[0]
        total_hours   = float(r.get("total_hours", 0))
        total_ot      = float(r.get("total_ot_hours", 0))
        reg_pay       = float(r.get("total_reg_pay", 0))
        ot_pay        = float(r.get("total_ot_pay", 0))
        total_cost    = reg_pay + ot_pay
        unique_emps   = int(r.get("unique_employees", 0))
        active_days   = int(r.get("active_days", 0))
        ot_pct        = (total_ot / total_hours * 100) if total_hours > 0 else 0
        ot_cost_pct   = (ot_pay / total_cost * 100) if total_cost > 0 else 0
        sections.append(f"""SUMMARY TOTALS ({period_label}, {active_days} days of data):
- Total Hours: {total_hours:,.1f} across {unique_emps} employees
- OT Hours: {total_ot:,.1f} ({ot_pct:.1f}% of total)
- Regular Pay: ${reg_pay:,.2f}
- OT Pay: ${ot_pay:,.2f} ({ot_cost_pct:.1f}% of total cost)
- Total Labor Cost: ${total_cost:,.2f}""")

    # Department breakdown
    dept_df = data.get("by_dept", pd.DataFrame())
    if not dept_df.empty:
        sections.append("DEPARTMENT BREAKDOWN:\n" + dept_df.to_string(index=False))

    # Employees
    emp_df = data.get("top_employees", pd.DataFrame())
    if not emp_df.empty:
        sections.append("TOP EMPLOYEES (by OT then total hours):\n" + emp_df.head(10).to_string(index=False))

    # Daily trend (summarized)
    daily_df = data.get("daily", pd.DataFrame())
    if not daily_df.empty and len(daily_df) <= 14:
        sections.append("DAILY BREAKDOWN:\n" + daily_df.to_string(index=False))
    elif not daily_df.empty:
        high_day  = daily_df.loc[daily_df["total_hours"].idxmax()]
        high_ot   = daily_df.loc[daily_df["ot_hours"].idxmax()]
        avg_hours = daily_df["total_hours"].mean()
        sections.append(f"""DAILY TREND SUMMARY:
- Average daily hours: {avg_hours:,.1f}
- Highest hours day: {high_day['date']} ({high_day['total_hours']:,.1f} hrs)
- Highest OT day: {high_ot['date']} ({high_ot['ot_hours']:,.1f} OT hrs)""")

    # Rooms
    rooms_df = data.get("rooms", pd.DataFrame())
    if not rooms_df.empty:
        avg_occ = rooms_df["occupied"].mean()
        sections.append(f"ROOM OCCUPANCY: Average {avg_occ:.0f} rooms/night over {len(rooms_df)} days.")

    data_block = "\n\n".join(sections) if sections else "No data found for this period."

    return f"""You are a world-class hotel labor analytics advisor. You speak directly to a hotel owner or CEO.
Your voice: confident, sharp, plain English. You answer EXACTLY what was asked — nothing more, nothing less.

HOTEL: {hotel}
USER'S EXACT QUESTION: "{question}"
ANALYSIS PERIOD: {period_label}

--- DATA ---
{data_block}

--- HOW TO RESPOND ---
1. Answer the question directly in 1-2 bold sentences. Lead with the most important number.
2. Follow with 2-3 sentences of supporting context — what's driving it, what it means for the business.
3. Give 2-3 specific, numbered action points — each with a real number, not vague advice.
4. End with one sharp "Bottom Line" sentence — the single thing they should act on TODAY.

Rules:
- Respond ONLY to what was asked. Do not add topics they didn't ask about.
- Use dollar signs, percentages, and hours where relevant.
- Keep total response under 250 words.
- Write in paragraphs, not bullet lists (except the action points).
- No markdown headers or bold markup in your output — write naturally."""


def call_groq(prompt: str) -> str:
    api_key = os.environ.get("GROQ_API_KEY", "")
    if not api_key:
        return "GROQ_API_KEY is not configured. Please add it in the Secrets panel."
    client = Groq(api_key=api_key)
    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.6,
        max_tokens=600,
    )
    return response.choices[0].message.content.strip()


def render_smart_charts(data: dict, intents: list, question: str):
    """Render only charts relevant to the question."""
    charts_rendered = 0

    dept_df  = data.get("by_dept", pd.DataFrame())
    daily_df = data.get("daily", pd.DataFrame())
    emp_df   = data.get("top_employees", pd.DataFrame())

    chart_cols = st.columns(2)
    col_idx = 0

    CHART_BG    = "rgba(0,0,0,0)"
    GRID_COLOR  = "rgba(0,0,0,0.06)"
    FONT_COLOR  = "#444"
    ACCENT1     = "#2196F3"
    ACCENT2     = "#4CAF50"
    ACCENT_OT   = "#FF5722"

    base_layout = dict(
        template="plotly_white",
        paper_bgcolor=CHART_BG,
        plot_bgcolor=CHART_BG,
        font=dict(color=FONT_COLOR, size=11),
        margin=dict(t=40, b=30, l=10, r=10),
        xaxis=dict(gridcolor=GRID_COLOR, showline=False),
        yaxis=dict(gridcolor=GRID_COLOR, showline=False),
        title_font=dict(size=13, color="#3D52A0"),
    )

    # Chart: Hours by Department
    if not dept_df.empty and ("department" in intents or "cost" in intents or len(intents) >= 3):
        with chart_cols[col_idx % 2]:
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=dept_df["department"], y=dept_df["total_hours"],
                name="Reg Hours", marker_color=ACCENT1, opacity=0.85,
            ))
            fig.add_trace(go.Bar(
                x=dept_df["department"], y=dept_df["ot_hours"],
                name="OT Hours", marker_color=ACCENT_OT, opacity=0.9,
            ))
            fig.update_layout(**base_layout, title="Hours by Department",
                              barmode="stack",
                              legend=dict(orientation="h", y=1.12, x=0),
                              xaxis_tickangle=-25)
            st.plotly_chart(fig, use_container_width=True)
            charts_rendered += 1
        col_idx += 1

    # Chart: Cost breakdown by department
    if not dept_df.empty and "cost" in intents:
        with chart_cols[col_idx % 2]:
            dept_df["total_cost"] = dept_df["reg_pay"] + dept_df["ot_pay"]
            fig2 = go.Figure()
            fig2.add_trace(go.Bar(
                x=dept_df["department"], y=dept_df["reg_pay"],
                name="Regular Pay", marker_color=ACCENT2, opacity=0.85,
            ))
            fig2.add_trace(go.Bar(
                x=dept_df["department"], y=dept_df["ot_pay"],
                name="OT Pay", marker_color=ACCENT_OT, opacity=0.9,
            ))
            fig2.update_layout(**base_layout, title="Labor Cost by Department",
                               barmode="stack",
                               legend=dict(orientation="h", y=1.12, x=0),
                               xaxis_tickangle=-25)
            st.plotly_chart(fig2, use_container_width=True)
            charts_rendered += 1
        col_idx += 1

    # Chart: Daily trend
    if not daily_df.empty and ("trend" in intents or "overtime" in intents or charts_rendered < 2):
        with chart_cols[col_idx % 2]:
            fig3 = go.Figure()
            fig3.add_trace(go.Scatter(
                x=daily_df["date"], y=daily_df["total_hours"],
                mode="lines+markers", name="Total Hours",
                line=dict(color=ACCENT1, width=2.5),
                fill="tozeroy", fillcolor="rgba(123,104,238,0.1)",
            ))
            fig3.add_trace(go.Scatter(
                x=daily_df["date"], y=daily_df["ot_hours"],
                mode="lines+markers", name="OT Hours",
                line=dict(color=ACCENT_OT, width=2, dash="dot"),
            ))
            fig3.update_layout(**base_layout, title="Daily Hours Trend",
                               legend=dict(orientation="h", y=1.12, x=0))
            st.plotly_chart(fig3, use_container_width=True)
            charts_rendered += 1
        col_idx += 1

    # Chart: Top OT employees
    if not emp_df.empty and ("employee" in intents or "overtime" in intents or charts_rendered < 2):
        ot_emp = emp_df[emp_df["ot_hours"] > 0].head(8)
        if not ot_emp.empty:
            with chart_cols[col_idx % 2]:
                fig4 = go.Figure(go.Bar(
                    x=ot_emp["ot_hours"], y=ot_emp["name"],
                    orientation="h",
                    marker=dict(
                        color=ot_emp["ot_hours"],
                        colorscale=[[0, ACCENT1], [1, ACCENT_OT]],
                        showscale=False,
                    ),
                    text=ot_emp["ot_hours"].apply(lambda x: f"{x:.1f}h"),
                    textposition="inside",
                ))
                fig4.update_layout(**base_layout, title="Top OT Employees",
                                   yaxis=dict(autorange="reversed",
                                              gridcolor=GRID_COLOR))
                st.plotly_chart(fig4, use_container_width=True)
                charts_rendered += 1
            col_idx += 1

    return charts_rendered


# ── Main page renderer ────────────────────────────────────────────────────────
def render_aipilot(hotel: str):
    st.markdown("""
    <style>
    .ai-hero {
        background: linear-gradient(135deg, #3D52A0 0%, #5C6FBF 60%, #8697C4 100%);
        border-radius: 14px; padding: 28px 32px 24px; margin-bottom: 20px;
        box-shadow: 0 4px 20px rgba(61,82,160,0.2);
    }
    .ai-title { font-size: 26px; font-weight: 900; color: #ffffff; letter-spacing: -0.5px; line-height: 1.15; margin-bottom: 2px; }
    .ai-subtitle { color: rgba(255,255,255,0.72); font-size: 13px; margin-top: 2px; }
    .ai-live-badge {
        display: inline-flex; align-items: center; gap: 5px;
        background: rgba(255,255,255,0.15); border: 1px solid rgba(255,255,255,0.3);
        border-radius: 20px; padding: 3px 10px; font-size: 11px; color: #ffffff; font-weight: 600;
    }
    .ai-live-dot {
        width: 6px; height: 6px; border-radius: 50%; background: #4CAF50;
        animation: ai-pulse 1.6s ease-in-out infinite; display: inline-block;
    }
    @keyframes ai-pulse { 0%, 100% { opacity: 1; transform: scale(1); } 50% { opacity: 0.3; transform: scale(0.7); } }
    .ai-response-card {
        background: linear-gradient(135deg, #f8f9ff 0%, #ffffff 100%);
        border: 1px solid #e0e4f0; border-left: 4px solid #3D52A0;
        border-radius: 10px; padding: 22px 26px; margin: 12px 0 16px 0;
        font-size: 14.5px; line-height: 1.8; color: #1a1a2e; white-space: pre-wrap;
        box-shadow: 0 2px 12px rgba(61,82,160,0.08);
    }
    .ai-model-tag {
        display: inline-flex; align-items: center; gap: 6px;
        background: linear-gradient(135deg, #3D52A0, #8697C4);
        border-radius: 20px; padding: 3px 12px; font-size: 11px; color: #fff; font-weight: 600; margin-bottom: 10px;
    }
    .ai-metrics-row { display: flex; flex-wrap: wrap; gap: 10px; margin: 16px 0 4px 0; }
    .ai-metric { background: #f0f4ff; border: 1px solid #d0d8f5; border-radius: 10px; padding: 10px 16px; min-width: 110px; text-align: center; }
    .ai-metric .val { font-size: 20px; font-weight: 800; color: #3D52A0; line-height: 1.2; }
    .ai-metric .val.red { color: #FF5722; }
    .ai-metric .val.teal { color: #2196F3; }
    .ai-metric .lbl { font-size: 10px; color: #888; margin-top: 2px; }
    .ai-period-badge {
        display: inline-flex; align-items: center; gap: 5px;
        background: #eef2ff; border: 1px solid #c7d2fe; border-radius: 8px; padding: 4px 12px;
        font-size: 11px; color: #3D52A0; margin-bottom: 10px;
    }
    .ai-charts-header { color: #3D52A0; font-size: 12px; font-weight: 700; letter-spacing: 0.6px; text-transform: uppercase; margin: 20px 0 6px 0; }
    .ai-charts-divider { height: 1px; background: linear-gradient(90deg, #3D52A0, transparent); margin-bottom: 12px; opacity: 0.2; }
    .ai-empty { background: #f8f9ff; border: 1px dashed #c7d2fe; border-radius: 12px; padding: 48px 24px; text-align: center; margin-top: 8px; }
    .ai-empty-icon { font-size: 40px; margin-bottom: 12px; }
    .ai-empty-title { color: #3D52A0; font-size: 15px; font-weight: 700; margin-bottom: 6px; }
    .ai-empty-sub { color: #888; font-size: 13px; }
    .ai-suggestion { display: inline-block; background: #eef2ff; border: 1px solid #c7d2fe; border-radius: 8px; padding: 6px 14px; margin: 4px; font-size: 12px; color: #3D52A0; cursor: pointer; }
    </style>
    """, unsafe_allow_html=True)

    # ── Hero header ──
    st.markdown(f"""
    <div class="ai-hero">
        <div style="display:flex;justify-content:space-between;align-items:flex-start;">
            <div>
                <div class="ai-title">AIPilot</div>
                <div class="ai-subtitle">Labor Intelligence for {hotel}</div>
            </div>
            <div class="ai-live-badge">
                <span class="ai-live-dot"></span> LIVE DATA
            </div>
        </div>
        <div style="margin-top:14px;color:rgba(200,184,255,0.5);font-size:13px;line-height:1.6;">
            Ask anything about your labor operations in plain English.<br>
            I'll pull your real data and give you a straight answer — CEO-grade, no fluff.
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Search input ──
    question = st.text_area(
        "",
        placeholder='e.g. "Who had the most overtime last week?" · "What is our total labor cost this month?" · "Which department is most expensive?"',
        height=80,
        key="ai_question",
        label_visibility="collapsed",
    )

    st.markdown("""
    <style>
    div[data-testid="stTextArea"] textarea {
        background: #ffffff !important;
        border: 1.5px solid #c7d2fe !important;
        border-radius: 10px !important;
        color: #1a1a2e !important;
        font-size: 14px !important;
        padding: 14px 16px !important;
        box-shadow: 0 2px 8px rgba(61,82,160,0.06) !important;
        transition: border-color 0.2s, box-shadow 0.2s !important;
    }
    div[data-testid="stTextArea"] textarea:focus {
        border-color: #3D52A0 !important;
        box-shadow: 0 0 0 3px rgba(61,82,160,0.1) !important;
    }
    div[data-testid="stTextArea"] textarea::placeholder {
        color: rgba(61,82,160,0.4) !important;
    }
    </style>
    """, unsafe_allow_html=True)

    col_btn, col_hint = st.columns([2, 5])
    with col_btn:
        run = st.button("✦ Ask AIPilot", type="primary", use_container_width=True)

    if not run:
        st.markdown("""
        <div class="ai-empty">
            <div class="ai-empty-icon">✦</div>
            <div class="ai-empty-title">What do you want to know?</div>
            <div class="ai-empty-sub">Try one of these:</div>
            <div style="margin-top:10px;">
                <span class="ai-suggestion">Who had the most OT last week?</span>
                <span class="ai-suggestion">What's our total labor cost this month?</span>
                <span class="ai-suggestion">Which department is over budget?</span>
                <span class="ai-suggestion">Show me the daily hours trend</span>
                <span class="ai-suggestion">Who are our top overtime earners?</span>
            </div>
        </div>
        """, unsafe_allow_html=True)
        return

    if not question.strip():
        st.warning("Please type a question first.")
        return

    # ── Infer date range from the question ──
    start_date, end_date = parse_dates_from_question(question)
    intents = detect_intent(question)

    days = (end_date - start_date).days + 1
    if start_date == end_date:
        period_label = start_date.strftime("%B %d, %Y")
    elif start_date == date.today() - timedelta(days=6) and end_date == date.today():
        period_label = "Last 7 days"
    else:
        period_label = f"{start_date.strftime('%b %d')} – {end_date.strftime('%b %d, %Y')}"

    # ── Fetch data ──
    with st.spinner(""):
        st.markdown('<div style="color:rgba(167,139,250,0.6);font-size:12px;margin:4px 0 0 2px;">Pulling your data...</div>', unsafe_allow_html=True)
        try:
            data = fetch_data(hotel, start_date, end_date, intents)
        except Exception as e:
            st.error(f"Database error: {e}")
            return

    totals = data.get("totals", pd.DataFrame())
    if totals.empty or float(totals.iloc[0].get("total_hours", 0)) == 0:
        st.warning(f"No labor data found for **{hotel}** in the period **{period_label}**. Make sure actual hours have been uploaded for this timeframe.")
        return

    t = totals.iloc[0]
    total_hours   = float(t.get("total_hours", 0))
    total_ot      = float(t.get("total_ot_hours", 0))
    reg_pay       = float(t.get("total_reg_pay", 0))
    ot_pay        = float(t.get("total_ot_pay", 0))
    total_cost    = reg_pay + ot_pay
    unique_emps   = int(t.get("unique_employees", 0))
    ot_pct        = (total_ot / total_hours * 100) if total_hours > 0 else 0

    # ── Period badge ──
    st.markdown(f"""
    <div class="ai-period-badge">
        ◈ Analyzing: {period_label} · {days} day{'s' if days > 1 else ''}
    </div>
    """, unsafe_allow_html=True)

    # ── KPI metrics ──
    st.markdown(f"""
    <div class="ai-metrics-row">
        <div class="ai-metric">
            <div class="val">{total_hours:,.0f}</div>
            <div class="lbl">Total Hours</div>
        </div>
        <div class="ai-metric">
            <div class="val red">{total_ot:,.1f}</div>
            <div class="lbl">OT Hours ({ot_pct:.1f}%)</div>
        </div>
        <div class="ai-metric">
            <div class="val teal">${total_cost:,.0f}</div>
            <div class="lbl">Labor Cost</div>
        </div>
        <div class="ai-metric">
            <div class="val red">${ot_pay:,.0f}</div>
            <div class="lbl">OT Pay</div>
        </div>
        <div class="ai-metric">
            <div class="val">{unique_emps}</div>
            <div class="lbl">Employees</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── AI Summary ──
    with st.spinner("Generating your answer..."):
        try:
            prompt  = build_dynamic_prompt(hotel, start_date, end_date, question, data, intents)
            summary = call_groq(prompt)
        except Exception as e:
            st.error(f"AI error: {e}")
            return

    st.markdown(f"""
    <div class="ai-model-tag">✦ Llama 3.3 · 70B</div>
    <div class="ai-response-card">{summary}</div>
    """, unsafe_allow_html=True)

    # ── Charts ──
    st.markdown("""
    <div class="ai-charts-header">◈ Supporting Charts</div>
    <div class="ai-charts-divider"></div>
    """, unsafe_allow_html=True)

    charts = render_smart_charts(data, intents, question)
    if charts == 0:
        st.info("No chart data available for this query.")
