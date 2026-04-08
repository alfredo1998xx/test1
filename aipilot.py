"""
aipilot.py — AI-powered labor intelligence page for LaborPilot
Generates executive-style summaries + charts from real DB data using Groq LLM.
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date, timedelta
from groq import Groq
from sqlalchemy import text
from db import ENGINE


# ── Helper: run a raw SQL query scoped to the current hotel ──────────────────
def _query(sql: str, params: dict) -> pd.DataFrame:
    with ENGINE.connect() as conn:
        return pd.read_sql_query(text(sql), conn, params=params)


# ── Fetch & aggregate labor data for the date range ─────────────────────────
def fetch_labor_data(hotel: str, start: date, end: date) -> dict:
    p = {"hotel": hotel, "start": str(start), "end": str(end)}

    # 1. Actual hours + pay by department
    dept_df = _query("""
        SELECT e.department,
               COALESCE(SUM(a.hours), 0)    AS total_hours,
               COALESCE(SUM(a.ot_hours), 0) AS ot_hours,
               COALESCE(SUM(a.reg_pay), 0)  AS reg_pay,
               COALESCE(SUM(a.ot_pay), 0)   AS ot_pay
        FROM   actual a
        JOIN   positions pos ON pos.id = a.position_id
        JOIN   departments d  ON d.id  = pos.department_id
        JOIN   employee e     ON e.hotel_name = :hotel
               AND e.department = d.name
        WHERE  a.hotel_name = :hotel
          AND  a.date BETWEEN :start AND :end
        GROUP  BY e.department
        ORDER  BY total_hours DESC
    """, p)

    # 2. Daily trend
    daily_df = _query("""
        SELECT a.date,
               COALESCE(SUM(a.hours), 0)    AS total_hours,
               COALESCE(SUM(a.ot_hours), 0) AS ot_hours,
               COALESCE(SUM(a.reg_pay + a.ot_pay), 0) AS total_cost
        FROM   actual a
        WHERE  a.hotel_name = :hotel
          AND  a.date BETWEEN :start AND :end
        GROUP  BY a.date
        ORDER  BY a.date
    """, p)

    # 3. Top OT employees
    top_ot_df = _query("""
        SELECT e.name, e.department,
               COALESCE(SUM(a.ot_hours), 0) AS ot_hours,
               COALESCE(SUM(a.ot_pay), 0)   AS ot_pay
        FROM   actual a
        JOIN   employee e ON e.hotel_name = :hotel
        WHERE  a.hotel_name = :hotel
          AND  a.date BETWEEN :start AND :end
          AND  a.emp_id = e.id
        GROUP  BY e.name, e.department
        HAVING SUM(a.ot_hours) > 0
        ORDER  BY ot_hours DESC
        LIMIT  10
    """, p)

    # 4. Scheduled vs actual (where both exist)
    sched_df = _query("""
        SELECT s.day AS date,
               COUNT(DISTINCT s.emp_id) AS scheduled_count,
               COUNT(DISTINCT s.shift_type) AS shift_types
        FROM   schedule s
        WHERE  s.hotel_name = :hotel
          AND  s.day BETWEEN :start AND :end
        GROUP  BY s.day
        ORDER  BY s.day
    """, p)

    # 5. Summary totals
    totals = _query("""
        SELECT COALESCE(SUM(a.hours), 0)    AS total_hours,
               COALESCE(SUM(a.ot_hours), 0) AS total_ot_hours,
               COALESCE(SUM(a.reg_pay), 0)  AS total_reg_pay,
               COALESCE(SUM(a.ot_pay), 0)   AS total_ot_pay,
               COUNT(DISTINCT a.emp_id)      AS unique_employees
        FROM   actual a
        WHERE  a.hotel_name = :hotel
          AND  a.date BETWEEN :start AND :end
    """, p)

    return {
        "dept_df":   dept_df,
        "daily_df":  daily_df,
        "top_ot_df": top_ot_df,
        "sched_df":  sched_df,
        "totals":    totals,
    }


# ── Build the prompt for the AI ──────────────────────────────────────────────
def build_prompt(hotel: str, start: date, end: date, question: str, data: dict) -> str:
    totals = data["totals"]
    dept_df = data["dept_df"]
    top_ot_df = data["top_ot_df"]

    t = totals.iloc[0] if not totals.empty else {}
    total_hours    = float(t.get("total_hours", 0))
    total_ot_hours = float(t.get("total_ot_hours", 0))
    total_reg_pay  = float(t.get("total_reg_pay", 0))
    total_ot_pay   = float(t.get("total_ot_pay", 0))
    unique_emps    = int(t.get("unique_employees", 0))
    total_cost     = total_reg_pay + total_ot_pay
    ot_pct         = (total_ot_hours / total_hours * 100) if total_hours > 0 else 0
    ot_cost_pct    = (total_ot_pay / total_cost * 100) if total_cost > 0 else 0

    dept_summary = dept_df.to_string(index=False) if not dept_df.empty else "No department data available."
    ot_summary   = top_ot_df.to_string(index=False) if not top_ot_df.empty else "No overtime recorded."

    date_range = f"{start.strftime('%B %d, %Y')} to {end.strftime('%B %d, %Y')}"
    days = (end - start).days + 1

    return f"""You are an elite hotel labor analytics advisor delivering a board-level executive briefing.
Your tone is confident, concise, and insight-driven — like a Chief People Officer presenting to the CEO.
Use plain English. No jargon. Lead with the most important insight. Be direct.

HOTEL: {hotel}
PERIOD: {date_range} ({days} days)
SPECIFIC QUESTION FROM USER: {question}

--- KEY METRICS ---
Total Hours Worked:  {total_hours:,.1f} hrs across {unique_emps} employees
Overtime Hours:      {total_ot_hours:,.1f} hrs ({ot_pct:.1f}% of total)
Regular Pay:         ${total_reg_pay:,.2f}
Overtime Pay:        ${total_ot_pay:,.2f} ({ot_cost_pct:.1f}% of total cost)
Total Labor Cost:    ${total_cost:,.2f}

--- DEPARTMENT BREAKDOWN ---
{dept_summary}

--- TOP OVERTIME EMPLOYEES ---
{ot_summary}

--- INSTRUCTIONS ---
1. Answer the user's specific question first (1-2 sentences, bold key numbers).
2. Write a 3-4 sentence executive summary covering: total labor cost, OT exposure, and the biggest risk or opportunity.
3. List exactly 3 "Key Takeaways" as bullet points — each one actionable and specific with a number.
4. Write one "CEO Recommendation" — a single, decisive action the leadership team should take this week.
Keep the entire response under 300 words. No markdown headers — use natural paragraph breaks."""


# ── Call Groq API ─────────────────────────────────────────────────────────────
def call_groq(prompt: str) -> str:
    api_key = os.environ.get("GROQ_API_KEY", "")
    if not api_key:
        return "⚠️ GROQ_API_KEY is not configured. Please add it in the Secrets panel."
    client = Groq(api_key=api_key)
    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.4,
        max_tokens=700,
    )
    return response.choices[0].message.content.strip()


# ── Charts ────────────────────────────────────────────────────────────────────
def render_charts(data: dict):
    dept_df  = data["dept_df"]
    daily_df = data["daily_df"]
    top_ot_df = data["top_ot_df"]

    if dept_df.empty and daily_df.empty:
        st.info("No chart data available for the selected period.")
        return

    col1, col2 = st.columns(2)

    # Chart 1: Hours by Department
    with col1:
        if not dept_df.empty:
            fig = px.bar(
                dept_df, x="department", y="total_hours",
                color="ot_hours",
                color_continuous_scale=["#4CAF50", "#FF5722"],
                labels={"total_hours": "Total Hours", "department": "Department", "ot_hours": "OT Hours"},
                title="Hours by Department",
                template="plotly_white",
            )
            fig.update_layout(
                title_font_size=14, margin=dict(t=40, b=20),
                coloraxis_colorbar=dict(title="OT hrs"),
                xaxis_tickangle=-30,
            )
            st.plotly_chart(fig, use_container_width=True)

    # Chart 2: Labor Cost Breakdown
    with col2:
        if not dept_df.empty:
            dept_df["total_cost"] = dept_df["reg_pay"] + dept_df["ot_pay"]
            fig2 = px.bar(
                dept_df, x="department",
                y=["reg_pay", "ot_pay"],
                labels={"value": "Cost ($)", "department": "Department", "variable": "Type"},
                title="Labor Cost by Department",
                color_discrete_map={"reg_pay": "#2196F3", "ot_pay": "#FF5722"},
                template="plotly_white",
                barmode="stack",
            )
            fig2.update_layout(
                title_font_size=14, margin=dict(t=40, b=20),
                xaxis_tickangle=-30,
                legend=dict(title="", orientation="h", y=1.05),
            )
            fig2.for_each_trace(lambda t: t.update(
                name="Regular Pay" if t.name == "reg_pay" else "Overtime Pay"
            ))
            st.plotly_chart(fig2, use_container_width=True)

    # Chart 3: Daily Hours Trend
    if not daily_df.empty:
        col3, col4 = st.columns(2)
        with col3:
            fig3 = go.Figure()
            fig3.add_trace(go.Scatter(
                x=daily_df["date"], y=daily_df["total_hours"],
                mode="lines+markers", name="Total Hours",
                line=dict(color="#2196F3", width=2),
            ))
            fig3.add_trace(go.Scatter(
                x=daily_df["date"], y=daily_df["ot_hours"],
                mode="lines+markers", name="OT Hours",
                line=dict(color="#FF5722", width=2, dash="dash"),
            ))
            fig3.update_layout(
                title="Daily Hours Trend", template="plotly_white",
                title_font_size=14, margin=dict(t=40, b=20),
                legend=dict(orientation="h", y=1.05),
                xaxis_title="Date", yaxis_title="Hours",
            )
            st.plotly_chart(fig3, use_container_width=True)

        # Chart 4: Top OT Employees
        with col4:
            if not top_ot_df.empty:
                fig4 = px.bar(
                    top_ot_df.head(8), x="ot_hours", y="name",
                    orientation="h",
                    color="ot_pay",
                    color_continuous_scale=["#FFF176", "#FF5722"],
                    labels={"ot_hours": "OT Hours", "name": "Employee", "ot_pay": "OT Pay ($)"},
                    title="Top Overtime Employees",
                    template="plotly_white",
                )
                fig4.update_layout(
                    title_font_size=14, margin=dict(t=40, b=20),
                    yaxis=dict(autorange="reversed"),
                )
                st.plotly_chart(fig4, use_container_width=True)
            else:
                st.info("No overtime recorded in this period.")


# ── Main page renderer ────────────────────────────────────────────────────────
def render_aipilot(hotel: str):
    st.markdown("""
    <style>
    .ai-card {
        background: linear-gradient(135deg, #f8f9ff 0%, #ffffff 100%);
        border: 1px solid #e0e4f0;
        border-left: 4px solid #3D52A0;
        border-radius: 10px;
        padding: 24px 28px;
        margin: 12px 0 20px 0;
        font-size: 15px;
        line-height: 1.75;
        color: #1a1a2e;
        white-space: pre-wrap;
        box-shadow: 0 2px 12px rgba(61,82,160,0.08);
    }
    .ai-header {
        display: flex; align-items: center; gap: 12px;
        margin-bottom: 6px;
    }
    .ai-badge {
        background: linear-gradient(135deg, #3D52A0, #8697C4);
        color: white; font-size: 11px; font-weight: 700;
        padding: 3px 10px; border-radius: 20px; letter-spacing: 0.5px;
    }
    .metric-pill {
        display: inline-block;
        background: #f0f4ff;
        border: 1px solid #d0d8f5;
        border-radius: 8px;
        padding: 8px 16px;
        margin: 4px;
        text-align: center;
    }
    .metric-pill .value { font-size: 22px; font-weight: 800; color: #3D52A0; }
    .metric-pill .label { font-size: 11px; color: #666; margin-top: 2px; }
    </style>
    """, unsafe_allow_html=True)

    # ── Page header ──
    st.markdown("""
    <div style="display:flex; align-items:center; gap:12px; margin-bottom:4px;">
        <div style="display:grid; grid-template-columns:1fr 1fr; gap:4px;">
            <div style="width:18px;height:18px;background:#3a3a3a;border-radius:4px;"></div>
            <div style="width:18px;height:18px;background:#e02020;border-radius:4px;"></div>
            <div style="width:18px;height:18px;background:#3a3a3a;border-radius:4px;"></div>
            <div style="width:18px;height:18px;background:#3a3a3a;border-radius:4px;"></div>
        </div>
        <div>
            <span style="font-size:22px;font-weight:800;color:#1a1a2e;letter-spacing:-0.5px;">AIPilot</span>
            <span style="font-size:13px;color:#888;margin-left:8px;">Labor Intelligence</span>
        </div>
    </div>
    <p style="color:#555;font-size:14px;margin-bottom:20px;">
        Ask anything about your labor data. Get an executive-grade summary with supporting charts.
    </p>
    """, unsafe_allow_html=True)

    # ── Inputs ──
    question = st.text_area(
        "What do you want to know?",
        placeholder="e.g. How is our overtime last week? Which department had the highest labor cost this month? Show me YTD hours by department.",
        height=90,
        key="ai_question",
    )

    run_btn = st.button("Generate Report", type="primary", use_container_width=False)

    if not run_btn:
        st.markdown("""
        <div style="background:#f8f9ff;border:1px dashed #c5cde8;border-radius:10px;
                    padding:28px;text-align:center;color:#888;margin-top:16px;">
            <div style="font-size:32px;margin-bottom:8px;">🤖</div>
            <div style="font-size:15px;font-weight:600;color:#555;">Ready to analyze your labor data</div>
            <div style="font-size:13px;margin-top:4px;">Type your question — include the time period you want (e.g. "last week", "this month", "last 30 days")</div>
        </div>
        """, unsafe_allow_html=True)
        return

    # ── Validate inputs ──
    if not question.strip():
        st.warning("Please enter a question before generating the report.")
        return

    # ── Parse date range from question ──
    today = date.today()
    q_lower = question.lower()

    if "yesterday" in q_lower:
        start_date = today - timedelta(days=1)
        end_date   = today - timedelta(days=1)
    elif "last week" in q_lower or "previous week" in q_lower:
        start_date = today - timedelta(days=today.weekday() + 7)
        end_date   = start_date + timedelta(days=6)
    elif "this week" in q_lower or "current week" in q_lower:
        start_date = today - timedelta(days=today.weekday())
        end_date   = today
    elif "last month" in q_lower or "previous month" in q_lower:
        first_this = today.replace(day=1)
        end_date   = first_this - timedelta(days=1)
        start_date = end_date.replace(day=1)
    elif "this month" in q_lower or "current month" in q_lower or "mtd" in q_lower or "month to date" in q_lower:
        start_date = today.replace(day=1)
        end_date   = today
    elif "last 7 days" in q_lower or "past 7 days" in q_lower:
        start_date = today - timedelta(days=6)
        end_date   = today
    elif "last 14 days" in q_lower or "past 14 days" in q_lower:
        start_date = today - timedelta(days=13)
        end_date   = today
    elif "last 30 days" in q_lower or "past 30 days" in q_lower:
        start_date = today - timedelta(days=29)
        end_date   = today
    elif "ytd" in q_lower or "year to date" in q_lower or "this year" in q_lower:
        start_date = today.replace(month=1, day=1)
        end_date   = today
    else:
        # Default: last 30 days
        start_date = today - timedelta(days=29)
        end_date   = today

    # ── Fetch data ──
    with st.spinner("Pulling data from your database..."):
        try:
            data = fetch_labor_data(hotel, start_date, end_date)
        except Exception as e:
            st.error(f"Database error: {e}")
            return

    totals = data["totals"]
    if totals.empty or float(totals.iloc[0].get("total_hours", 0)) == 0:
        st.warning(f"No labor data found for **{hotel}** between {start_date} and {end_date}. Make sure actual hours have been uploaded for this period.")
        return

    t = totals.iloc[0]
    total_hours    = float(t.get("total_hours", 0))
    total_ot_hours = float(t.get("total_ot_hours", 0))
    total_reg_pay  = float(t.get("total_reg_pay", 0))
    total_ot_pay   = float(t.get("total_ot_pay", 0))
    unique_emps    = int(t.get("unique_employees", 0))
    total_cost     = total_reg_pay + total_ot_pay
    ot_pct         = (total_ot_hours / total_hours * 100) if total_hours > 0 else 0

    # ── KPI Pills ──
    st.markdown(f"""
    <div style="display:flex;flex-wrap:wrap;gap:0;margin:16px 0 8px 0;">
        <div class="metric-pill">
            <div class="value">{total_hours:,.0f}</div>
            <div class="label">Total Hours</div>
        </div>
        <div class="metric-pill">
            <div class="value" style="color:#e53935;">{total_ot_hours:,.1f}</div>
            <div class="label">OT Hours ({ot_pct:.1f}%)</div>
        </div>
        <div class="metric-pill">
            <div class="value">${total_cost:,.0f}</div>
            <div class="label">Total Labor Cost</div>
        </div>
        <div class="metric-pill">
            <div class="value" style="color:#e53935;">${total_ot_pay:,.0f}</div>
            <div class="label">OT Pay</div>
        </div>
        <div class="metric-pill">
            <div class="value">{unique_emps}</div>
            <div class="label">Employees</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── AI Summary ──
    with st.spinner("Generating executive summary..."):
        try:
            prompt  = build_prompt(hotel, start_date, end_date, question, data)
            summary = call_groq(prompt)
        except Exception as e:
            st.error(f"AI error: {e}")
            return

    st.markdown(f"""
    <div class="ai-header">
        <span style="font-size:16px;font-weight:700;color:#1a1a2e;">Executive Summary</span>
        <span class="ai-badge">AI · Llama 3.3 70B</span>
    </div>
    <div class="ai-card">{summary}</div>
    """, unsafe_allow_html=True)

    # ── Charts ──
    st.markdown("---")
    st.markdown("#### Supporting Charts")
    render_charts(data)

    # ── Raw data expander ──
    with st.expander("View Raw Data"):
        st.markdown("**Department Breakdown**")
        st.dataframe(data["dept_df"], use_container_width=True)
        st.markdown("**Daily Trend**")
        st.dataframe(data["daily_df"], use_container_width=True)
        if not data["top_ot_df"].empty:
            st.markdown("**Top OT Employees**")
            st.dataframe(data["top_ot_df"], use_container_width=True)
