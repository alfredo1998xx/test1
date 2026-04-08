"""
aipilot.py — AI-powered labor intelligence page for LaborPilot.
Detects the intent of the question, fetches the right data, builds a
focused executive prompt, and shows relevant charts.
"""

import os
import re
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date, timedelta
from groq import Groq
from sqlalchemy import text
from db import ENGINE


# ─────────────────────────────────────────────────────────────────────────────
# 1. INTENT DETECTION
# ─────────────────────────────────────────────────────────────────────────────
INTENT_KEYWORDS = {
    "ot":         ["overtime", "ot ", "over time", "ot hours", "ot pay", "ot cost"],
    "risk":       ["risk", "at risk", "approaching", "close to 40", "near 40", "threshold", "danger"],
    "schedule":   ["schedule", "scheduling", "mockup", "mock up", "who is working", "who's working",
                   "shift", "shifts", "coverage", "staffing", "staff this", "headcount"],
    "cost":       ["cost", "pay", "payroll", "spend", "spending", "budget", "wage", "salary", "expense"],
    "department": ["department", "dept", "housekeeping", "front desk", "food", "maintenance", "engineering"],
    "employee":   ["employee", "staff", "worker", "who worked", "team member"],
}

def detect_intent(question: str) -> str:
    q = question.lower()
    for intent, keywords in INTENT_KEYWORDS.items():
        if any(kw in q for kw in keywords):
            return intent
    return "general"


# ─────────────────────────────────────────────────────────────────────────────
# 2. DATE RANGE PARSING
# ─────────────────────────────────────────────────────────────────────────────
def parse_date_range(question: str):
    today = date.today()
    q = question.lower()

    if "yesterday" in q:
        s = today - timedelta(days=1)
        return s, s
    if "last week" in q or "previous week" in q:
        s = today - timedelta(days=today.weekday() + 7)
        return s, s + timedelta(days=6)
    if "this week" in q or "current week" in q:
        s = today - timedelta(days=today.weekday())
        return s, today
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
    # Default
    return today - timedelta(days=29), today


# ─────────────────────────────────────────────────────────────────────────────
# 3. DATA FETCHING — targeted by intent
# ─────────────────────────────────────────────────────────────────────────────
def _q(sql: str, params: dict) -> pd.DataFrame:
    with ENGINE.connect() as conn:
        return pd.read_sql_query(text(sql), conn, params=params)


def fetch_data(hotel: str, start: date, end: date, intent: str) -> dict:
    p = {"hotel": hotel, "start": str(start), "end": str(end)}
    data = {}

    # ── Always fetch: totals + dept breakdown ──
    data["totals"] = _q("""
        SELECT COALESCE(SUM(a.hours),0) total_hours,
               COALESCE(SUM(a.ot_hours),0) total_ot,
               COALESCE(SUM(a.reg_pay),0) reg_pay,
               COALESCE(SUM(a.ot_pay),0) ot_pay,
               COUNT(DISTINCT a.emp_id) employees
        FROM actual a WHERE a.hotel_name=:hotel AND a.date BETWEEN :start AND :end
    """, p)

    data["dept"] = _q("""
        SELECT d.name department,
               COALESCE(SUM(a.hours),0) total_hours,
               COALESCE(SUM(a.ot_hours),0) ot_hours,
               COALESCE(SUM(a.reg_pay),0) reg_pay,
               COALESCE(SUM(a.ot_pay),0) ot_pay
        FROM actual a
        JOIN positions pos ON pos.id=a.position_id
        JOIN departments d ON d.id=pos.department_id
        WHERE a.hotel_name=:hotel AND a.date BETWEEN :start AND :end
        GROUP BY d.name ORDER BY total_hours DESC
    """, p)

    # ── OT / Risk: per-employee OT detail ──
    if intent in ("ot", "risk", "employee", "general"):
        data["emp_ot"] = _q("""
            SELECT e.name, e.department,
                   COALESCE(SUM(a.hours),0) total_hours,
                   COALESCE(SUM(a.ot_hours),0) ot_hours,
                   COALESCE(SUM(a.ot_pay),0) ot_pay
            FROM actual a
            JOIN employee e ON e.id=a.emp_id AND e.hotel_name=:hotel
            WHERE a.hotel_name=:hotel AND a.date BETWEEN :start AND :end
            GROUP BY e.name, e.department
            HAVING SUM(a.ot_hours)>0 OR SUM(a.hours)>0
            ORDER BY ot_hours DESC
        """, p)

    # ── Schedule / Risk: this week's scheduled shifts ──
    if intent in ("schedule", "risk", "general"):
        data["schedule"] = _q("""
            SELECT e.name, e.department, e.role position,
                   s.day, s.shift_type
            FROM schedule s
            JOIN employee e ON e.id=s.emp_id AND e.hotel_name=:hotel
            WHERE s.hotel_name=:hotel AND s.day BETWEEN :start AND :end
              AND s.shift_type NOT IN ('OFF','off','Off')
            ORDER BY s.day, e.department, e.name
        """, p)

        # Headcount per day
        data["headcount"] = _q("""
            SELECT s.day, COUNT(DISTINCT s.emp_id) headcount,
                   COUNT(*) shifts
            FROM schedule s
            WHERE s.hotel_name=:hotel AND s.day BETWEEN :start AND :end
              AND s.shift_type NOT IN ('OFF','off','Off')
            GROUP BY s.day ORDER BY s.day
        """, p)

    # ── Risk: projected OT — scheduled shifts this week + actual hours so far ──
    if intent == "risk":
        today = date.today()
        week_start = today - timedelta(days=today.weekday())
        week_end   = week_start + timedelta(days=6)
        rp = {"hotel": hotel, "ws": str(week_start), "we": str(week_end), "today": str(today)}

        data["ot_risk"] = _q("""
            SELECT e.name, e.department,
                   COUNT(DISTINCT s.day) scheduled_days,
                   COALESCE(SUM(a.hours),0) actual_hours_so_far,
                   COUNT(DISTINCT s.day) * 8.0 projected_total
            FROM employee e
            LEFT JOIN schedule s ON s.emp_id=e.id AND s.hotel_name=:hotel
                AND s.day BETWEEN :ws AND :we
                AND s.shift_type NOT IN ('OFF','off','Off')
            LEFT JOIN actual a ON a.emp_id=e.id AND a.hotel_name=:hotel
                AND a.date BETWEEN :ws AND :today
            WHERE e.hotel_name=:hotel
            GROUP BY e.name, e.department
            HAVING COUNT(DISTINCT s.day)>0
            ORDER BY projected_total DESC
        """, rp)

    # ── Schedule / Mockup: employee roster for AI to build a schedule ──
    if intent == "schedule":
        data["employees"] = _q("""
            SELECT e.name, e.department, e.role position, e.emp_type
            FROM employee e WHERE e.hotel_name=:hotel
            ORDER BY e.department, e.name
        """, p)

    # ── Cost: daily cost trend ──
    if intent in ("cost", "general"):
        data["daily"] = _q("""
            SELECT a.date,
                   COALESCE(SUM(a.reg_pay),0) reg_pay,
                   COALESCE(SUM(a.ot_pay),0) ot_pay,
                   COALESCE(SUM(a.hours),0) hours
            FROM actual a WHERE a.hotel_name=:hotel AND a.date BETWEEN :start AND :end
            GROUP BY a.date ORDER BY a.date
        """, p)

    # ── OT: daily OT trend ──
    if intent == "ot":
        data["daily_ot"] = _q("""
            SELECT a.date,
                   COALESCE(SUM(a.ot_hours),0) ot_hours,
                   COALESCE(SUM(a.ot_pay),0) ot_pay,
                   COALESCE(SUM(a.hours),0) total_hours
            FROM actual a WHERE a.hotel_name=:hotel AND a.date BETWEEN :start AND :end
            GROUP BY a.date ORDER BY a.date
        """, p)

    return data


# ─────────────────────────────────────────────────────────────────────────────
# 4. PROMPT BUILDER — question-focused, intent-aware
# ─────────────────────────────────────────────────────────────────────────────
def build_prompt(hotel: str, start: date, end: date, question: str,
                 intent: str, data: dict) -> str:
    today = date.today()
    period = f"{start.strftime('%B %d, %Y')} — {end.strftime('%B %d, %Y')}"
    days   = (end - start).days + 1

    t = data["totals"].iloc[0] if not data["totals"].empty else {}
    th   = float(t.get("total_hours", 0))
    tot  = float(t.get("total_ot", 0))
    rp   = float(t.get("reg_pay", 0))
    op   = float(t.get("ot_pay", 0))
    tc   = rp + op
    emps = int(t.get("employees", 0))
    ot_pct = (tot / th * 100) if th else 0

    dept_txt = data["dept"].to_string(index=False) if not data["dept"].empty else "No department data."

    sections = [
        f"You are an elite hotel labor analytics advisor. Your ONLY job right now is to answer this specific question:\n\n\"{question}\"\n",
        f"HOTEL: {hotel}  |  PERIOD: {period} ({days} days)  |  TODAY: {today.strftime('%A, %B %d, %Y')}",
        f"\n--- OVERALL METRICS ---",
        f"Total Hours: {th:,.1f}   OT Hours: {tot:,.1f} ({ot_pct:.1f}%)   Employees: {emps}",
        f"Regular Pay: ${rp:,.2f}   OT Pay: ${op:,.2f}   Total Cost: ${tc:,.2f}",
        f"\n--- BY DEPARTMENT ---\n{dept_txt}",
    ]

    # Intent-specific data sections
    if intent == "ot" and "emp_ot" in data and not data["emp_ot"].empty:
        sections.append(f"\n--- EMPLOYEE OT DETAIL ---\n{data['emp_ot'].to_string(index=False)}")

    if intent == "risk" and "ot_risk" in data and not data["ot_risk"].empty:
        risk_df = data["ot_risk"]
        at_risk = risk_df[risk_df["projected_total"] >= 36]
        sections.append(f"\n--- OT RISK THIS WEEK (projected hours ≥ 36) ---\n{at_risk.to_string(index=False) if not at_risk.empty else 'No employees currently at OT risk.'}")
        sections.append(f"\n--- ALL SCHEDULED EMPLOYEES THIS WEEK ---\n{risk_df.to_string(index=False)}")

    if intent == "schedule":
        if "schedule" in data and not data["schedule"].empty:
            sections.append(f"\n--- CURRENT SCHEDULE DATA ---\n{data['schedule'].to_string(index=False)}")
        if "employees" in data and not data["employees"].empty:
            sections.append(f"\n--- EMPLOYEE ROSTER ---\n{data['employees'].to_string(index=False)}")

    if intent in ("cost",) and "daily" in data and not data["daily"].empty:
        sections.append(f"\n--- DAILY COST TREND ---\n{data['daily'].to_string(index=False)}")

    sections.append(f"""
--- YOUR RESPONSE RULES ---
1. Answer the user's question DIRECTLY in the first sentence. Lead with the number.
2. Write an executive-grade narrative (3–5 sentences max). CEO language: confident, clear, no jargon.
3. Give exactly 3 "Key Takeaways" bullets — each with a specific number and an actionable implication.
4. End with one "Recommendation" — a single decisive action for this week.
5. If the question asks to create a schedule or suggest staffing — output a clean schedule table showing department, employee, and day/shift. Format it clearly.
6. If there is no relevant data, say so directly in one sentence. Do not fabricate numbers.
7. Keep the entire response under 350 words. No markdown headers — use natural line breaks.""")

    return "\n".join(sections)


# ─────────────────────────────────────────────────────────────────────────────
# 5. GROQ API CALL
# ─────────────────────────────────────────────────────────────────────────────
def call_groq(prompt: str) -> str:
    api_key = os.environ.get("GROQ_API_KEY", "")
    if not api_key:
        return "⚠️ GROQ_API_KEY is not configured."
    client = Groq(api_key=api_key)
    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3,
        max_tokens=800,
    )
    return response.choices[0].message.content.strip()


# ─────────────────────────────────────────────────────────────────────────────
# 6. DYNAMIC CHARTS — based on intent
# ─────────────────────────────────────────────────────────────────────────────
def render_charts(intent: str, data: dict):
    dept = data.get("dept", pd.DataFrame())
    shown = False

    # ── OT intent ──
    if intent == "ot":
        emp_ot = data.get("emp_ot", pd.DataFrame())
        daily_ot = data.get("daily_ot", pd.DataFrame())
        c1, c2 = st.columns(2)
        with c1:
            if not emp_ot.empty:
                fig = px.bar(emp_ot.head(10), x="ot_hours", y="name", orientation="h",
                             color="ot_hours", color_continuous_scale=["#FFF9C4", "#FF5722"],
                             title="Overtime Hours by Employee",
                             labels={"ot_hours": "OT Hours", "name": ""},
                             template="plotly_white")
                fig.update_layout(yaxis=dict(autorange="reversed"), margin=dict(t=40, b=10), title_font_size=14)
                st.plotly_chart(fig, use_container_width=True)
                shown = True
        with c2:
            if not daily_ot.empty:
                fig2 = go.Figure()
                fig2.add_trace(go.Bar(x=daily_ot["date"], y=daily_ot["total_hours"],
                                       name="Total Hours", marker_color="#90CAF9"))
                fig2.add_trace(go.Bar(x=daily_ot["date"], y=daily_ot["ot_hours"],
                                       name="OT Hours", marker_color="#FF5722"))
                fig2.update_layout(barmode="overlay", title="Daily Hours vs OT",
                                   template="plotly_white", margin=dict(t=40, b=10),
                                   title_font_size=14, legend=dict(orientation="h", y=1.1))
                st.plotly_chart(fig2, use_container_width=True)
                shown = True

        if not dept.empty:
            fig3 = px.bar(dept, x="department", y=["total_hours", "ot_hours"],
                          barmode="group", title="Hours vs OT by Department",
                          labels={"value": "Hours", "department": ""},
                          color_discrete_map={"total_hours": "#90CAF9", "ot_hours": "#FF5722"},
                          template="plotly_white")
            fig3.update_layout(margin=dict(t=40, b=10), title_font_size=14,
                               legend=dict(title="", orientation="h", y=1.1))
            fig3.for_each_trace(lambda t: t.update(name="Total Hours" if t.name == "total_hours" else "OT Hours"))
            st.plotly_chart(fig3, use_container_width=True)
            shown = True

    # ── Risk intent ──
    elif intent == "risk":
        risk_df = data.get("ot_risk", pd.DataFrame())
        if not risk_df.empty:
            risk_df = risk_df.copy()
            risk_df["status"] = risk_df["projected_total"].apply(
                lambda x: "At Risk (≥40h)" if x >= 40 else ("Approaching (≥36h)" if x >= 36 else "Safe")
            )
            color_map = {"At Risk (≥40h)": "#e53935", "Approaching (≥36h)": "#FF9800", "Safe": "#43A047"}
            fig = px.bar(risk_df.sort_values("projected_total", ascending=True),
                         x="projected_total", y="name", orientation="h",
                         color="status", color_discrete_map=color_map,
                         title="Projected Weekly Hours — OT Risk Assessment",
                         labels={"projected_total": "Projected Hours", "name": ""},
                         template="plotly_white")
            fig.add_vline(x=40, line_dash="dash", line_color="red",
                          annotation_text="40h OT threshold", annotation_position="top right")
            fig.update_layout(margin=dict(t=50, b=10), title_font_size=14,
                              legend=dict(title="", orientation="h", y=1.12))
            st.plotly_chart(fig, use_container_width=True)
            shown = True

        hc = data.get("headcount", pd.DataFrame())
        if not hc.empty:
            fig2 = px.bar(hc, x="day", y="headcount", title="Daily Headcount This Week",
                          color="headcount", color_continuous_scale=["#E3F2FD", "#1565C0"],
                          labels={"headcount": "Employees", "day": ""},
                          template="plotly_white")
            fig2.update_layout(margin=dict(t=40, b=10), title_font_size=14)
            st.plotly_chart(fig2, use_container_width=True)
            shown = True

    # ── Schedule intent ──
    elif intent == "schedule":
        hc = data.get("headcount", pd.DataFrame())
        sched = data.get("schedule", pd.DataFrame())
        c1, c2 = st.columns(2)
        with c1:
            if not hc.empty:
                fig = px.bar(hc, x="day", y="headcount",
                             title="Scheduled Headcount by Day",
                             color="headcount", color_continuous_scale=["#E8F5E9", "#2E7D32"],
                             labels={"headcount": "Employees", "day": ""},
                             template="plotly_white")
                fig.update_layout(margin=dict(t=40, b=10), title_font_size=14)
                st.plotly_chart(fig, use_container_width=True)
                shown = True
        with c2:
            if not sched.empty and "department" in sched.columns:
                dept_day = sched.groupby(["department", "day"]).size().reset_index(name="count")
                fig2 = px.bar(dept_day, x="day", y="count", color="department",
                              title="Shifts by Department per Day", barmode="stack",
                              labels={"count": "Shifts", "day": ""},
                              template="plotly_white")
                fig2.update_layout(margin=dict(t=40, b=10), title_font_size=14,
                                   legend=dict(title="", orientation="h", y=1.12))
                st.plotly_chart(fig2, use_container_width=True)
                shown = True

    # ── Cost intent ──
    elif intent == "cost":
        daily = data.get("daily", pd.DataFrame())
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
                fig.update_layout(title="Daily Labor Cost Trend", template="plotly_white",
                                  margin=dict(t=40, b=10), title_font_size=14,
                                  legend=dict(orientation="h", y=1.1))
                st.plotly_chart(fig, use_container_width=True)
                shown = True
        with c2:
            if not dept.empty:
                dept["total_cost"] = dept["reg_pay"] + dept["ot_pay"]
                fig2 = px.pie(dept, names="department", values="total_cost",
                              title="Cost Share by Department",
                              template="plotly_white",
                              color_discrete_sequence=px.colors.qualitative.Set2)
                fig2.update_layout(margin=dict(t=40, b=10), title_font_size=14)
                st.plotly_chart(fig2, use_container_width=True)
                shown = True

    # ── General / employee / department ──
    else:
        c1, c2 = st.columns(2)
        with c1:
            if not dept.empty:
                fig = px.bar(dept, x="department", y="total_hours",
                             color="ot_hours", color_continuous_scale=["#4CAF50", "#FF5722"],
                             title="Hours by Department", template="plotly_white",
                             labels={"total_hours": "Hours", "department": "",
                                     "ot_hours": "OT Hours"})
                fig.update_layout(margin=dict(t=40, b=10), title_font_size=14, xaxis_tickangle=-20)
                st.plotly_chart(fig, use_container_width=True)
                shown = True
        with c2:
            if not dept.empty:
                dept["total_cost"] = dept["reg_pay"] + dept["ot_pay"]
                fig2 = px.bar(dept, x="department", y=["reg_pay", "ot_pay"],
                              barmode="stack", title="Labor Cost by Department",
                              color_discrete_map={"reg_pay": "#2196F3", "ot_pay": "#FF5722"},
                              labels={"value": "Cost ($)", "department": ""},
                              template="plotly_white")
                fig2.update_layout(margin=dict(t=40, b=10), title_font_size=14,
                                   xaxis_tickangle=-20,
                                   legend=dict(title="", orientation="h", y=1.1))
                fig2.for_each_trace(lambda t: t.update(
                    name="Regular Pay" if t.name == "reg_pay" else "OT Pay"))
                st.plotly_chart(fig2, use_container_width=True)
                shown = True

        emp_ot = data.get("emp_ot", pd.DataFrame())
        if not emp_ot.empty and not emp_ot[emp_ot["ot_hours"] > 0].empty:
            top = emp_ot[emp_ot["ot_hours"] > 0].head(8)
            fig3 = px.bar(top, x="ot_hours", y="name", orientation="h",
                          title="Top OT Employees", template="plotly_white",
                          color="ot_hours", color_continuous_scale=["#FFF9C4", "#FF5722"],
                          labels={"ot_hours": "OT Hours", "name": ""})
            fig3.update_layout(yaxis=dict(autorange="reversed"), margin=dict(t=40, b=10), title_font_size=14)
            st.plotly_chart(fig3, use_container_width=True)
            shown = True

    if not shown:
        st.info("No chart data available for the selected period.")


# ─────────────────────────────────────────────────────────────────────────────
# 7. MAIN PAGE RENDERER
# ─────────────────────────────────────────────────────────────────────────────
def render_aipilot(hotel: str):
    st.markdown("""
    <style>
    .ai-card {
        background: linear-gradient(135deg,#f8f9ff,#ffffff);
        border: 1px solid #e0e4f0;
        border-left: 4px solid #3D52A0;
        border-radius: 10px;
        padding: 24px 28px;
        margin: 12px 0 20px;
        font-size: 15px;
        line-height: 1.8;
        color: #1a1a2e;
        white-space: pre-wrap;
        box-shadow: 0 2px 12px rgba(61,82,160,.08);
    }
    .ai-badge {
        background: linear-gradient(135deg,#3D52A0,#8697C4);
        color:#fff; font-size:11px; font-weight:700;
        padding:3px 10px; border-radius:20px; letter-spacing:.5px;
    }
    .mp { display:inline-block; background:#f0f4ff; border:1px solid #d0d8f5;
          border-radius:8px; padding:8px 16px; margin:4px; text-align:center; }
    .mp .v { font-size:22px; font-weight:800; color:#3D52A0; }
    .mp .l { font-size:11px; color:#666; margin-top:2px; }
    .intent-tag { display:inline-block; background:#e8f5e9; color:#2E7D32;
                  font-size:11px; font-weight:700; padding:2px 10px;
                  border-radius:12px; margin-left:8px; }
    </style>
    """, unsafe_allow_html=True)

    # Header
    st.markdown("""
    <div style="display:flex;align-items:center;gap:12px;margin-bottom:4px;">
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
    <p style="color:#555;font-size:14px;margin-bottom:20px;">
        Ask any labor question in plain English — OT analysis, scheduling, cost, risk, you name it.
        Include the time period in your question (e.g. "last week", "this month", "YTD").
    </p>
    """, unsafe_allow_html=True)

    question = st.text_area(
        "What do you want to know?",
        placeholder=(
            "e.g.  'What was our overtime last week?'  ·  "
            "'Is anyone at OT risk this week?'  ·  "
            "'Create a mockup schedule for this week'  ·  "
            "'Which department had the highest labor cost this month?'"
        ),
        height=90,
        key="ai_question",
    )

    run_btn = st.button("Generate Report", type="primary")

    if not run_btn:
        st.markdown("""
        <div style="background:#f8f9ff;border:1px dashed #c5cde8;border-radius:10px;
                    padding:32px;text-align:center;color:#888;margin-top:16px;">
            <div style="font-size:34px;margin-bottom:8px;">🤖</div>
            <div style="font-size:15px;font-weight:600;color:#555;">Ready to answer any labor question</div>
            <div style="font-size:13px;margin-top:6px;line-height:1.8;">
                Try: <i>"What was our OT last week?"</i> &nbsp;·&nbsp;
                <i>"Any OT risk this week?"</i> &nbsp;·&nbsp;
                <i>"Summarize labor cost this month"</i>
            </div>
        </div>
        """, unsafe_allow_html=True)
        return

    if not question.strip():
        st.warning("Please enter a question.")
        return

    intent = detect_intent(question)
    start_date, end_date = parse_date_range(question)

    st.markdown(
        f'<span style="font-size:12px;color:#888;">Analyzing: '
        f'<b>{start_date.strftime("%b %d")} – {end_date.strftime("%b %d, %Y")}</b></span>'
        f'<span class="intent-tag">{intent.upper()}</span>',
        unsafe_allow_html=True,
    )

    # Fetch data
    with st.spinner("Fetching your data..."):
        try:
            data = fetch_data(hotel, start_date, end_date, intent)
        except Exception as e:
            st.error(f"Database error: {e}")
            return

    # KPI pills (skip for pure scheduling / risk with no actual data)
    t = data["totals"].iloc[0] if not data["totals"].empty else {}
    th  = float(t.get("total_hours", 0))
    tot = float(t.get("total_ot", 0))
    rp  = float(t.get("reg_pay", 0))
    op  = float(t.get("ot_pay", 0))
    tc  = rp + op
    emp = int(t.get("employees", 0))
    ot_pct = (tot / th * 100) if th else 0

    if th > 0:
        st.markdown(f"""
        <div style="display:flex;flex-wrap:wrap;gap:0;margin:16px 0 8px;">
            <div class="mp"><div class="v">{th:,.0f}</div><div class="l">Total Hours</div></div>
            <div class="mp"><div class="v" style="color:#e53935;">{tot:,.1f}</div><div class="l">OT Hours ({ot_pct:.1f}%)</div></div>
            <div class="mp"><div class="v">${tc:,.0f}</div><div class="l">Total Labor Cost</div></div>
            <div class="mp"><div class="v" style="color:#e53935;">${op:,.0f}</div><div class="l">OT Pay</div></div>
            <div class="mp"><div class="v">{emp}</div><div class="l">Employees</div></div>
        </div>
        """, unsafe_allow_html=True)

    # AI summary
    with st.spinner("Generating executive summary..."):
        try:
            prompt  = build_prompt(hotel, start_date, end_date, question, intent, data)
            summary = call_groq(prompt)
        except Exception as e:
            st.error(f"AI error: {e}")
            return

    st.markdown(
        f'<div style="display:flex;align-items:center;gap:10px;margin-top:20px;">'
        f'<span style="font-size:16px;font-weight:700;color:#1a1a2e;">Executive Summary</span>'
        f'<span class="ai-badge">AI · Llama 3.3 70B</span></div>'
        f'<div class="ai-card">{summary}</div>',
        unsafe_allow_html=True,
    )

    # Charts
    st.markdown("---")
    st.markdown("#### Supporting Analysis")
    render_charts(intent, data)

    # Raw data
    with st.expander("View Raw Data"):
        for label, key in [("Department Breakdown", "dept"), ("Employee OT Detail", "emp_ot"),
                            ("Schedule", "schedule"), ("Headcount", "headcount"),
                            ("OT Risk", "ot_risk"), ("Daily Trend", "daily"),
                            ("Daily OT", "daily_ot"), ("Employee Roster", "employees")]:
            df = data.get(key)
            if df is not None and not df.empty:
                st.markdown(f"**{label}**")
                st.dataframe(df, use_container_width=True)
