"""
aipilot.py — AI-powered labor intelligence page for LaborPilot
Uses ALL data in the system to answer questions accurately.
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from groq import Groq
from sqlalchemy import text
from db import ENGINE


# ── Raw SQL query helper ──────────────────────────────────────────────────────
def _q(sql: str, params: dict = {}) -> pd.DataFrame:
    with ENGINE.connect() as conn:
        return pd.read_sql_query(text(sql), conn, params=params)


# ── Pull comprehensive snapshot of ALL data for the hotel ────────────────────
def fetch_all_data(hotel: str) -> dict:
    p = {"hotel": hotel}

    # Overall totals (all time)
    totals = _q("""
        SELECT
            COALESCE(SUM(a.hours), 0)              AS total_hours,
            COALESCE(SUM(a.ot_hours), 0)           AS total_ot_hours,
            COALESCE(SUM(a.reg_pay), 0)            AS total_reg_pay,
            COALESCE(SUM(a.ot_pay), 0)             AS total_ot_pay,
            COUNT(DISTINCT a.emp_id)               AS unique_employees,
            MIN(a.date)                            AS earliest_date,
            MAX(a.date)                            AS latest_date
        FROM actual a
        WHERE a.hotel_name = :hotel
    """, p)

    # By department (all time)
    by_dept = _q("""
        SELECT
            pos.name                               AS position,
            COALESCE(SUM(a.hours), 0)              AS total_hours,
            COALESCE(SUM(a.ot_hours), 0)           AS ot_hours,
            COALESCE(SUM(a.reg_pay), 0)            AS reg_pay,
            COALESCE(SUM(a.ot_pay), 0)             AS ot_pay,
            COALESCE(SUM(a.reg_pay + a.ot_pay), 0) AS total_cost,
            COUNT(DISTINCT a.emp_id)               AS employee_count
        FROM actual a
        JOIN positions pos ON pos.id = a.position_id
        WHERE a.hotel_name = :hotel
        GROUP BY pos.name
        ORDER BY total_cost DESC
    """, p)

    # Monthly trend (all time)
    monthly = _q("""
        SELECT
            TO_CHAR(a.date, 'YYYY-MM')             AS month,
            COALESCE(SUM(a.hours), 0)              AS total_hours,
            COALESCE(SUM(a.ot_hours), 0)           AS ot_hours,
            COALESCE(SUM(a.reg_pay + a.ot_pay), 0) AS total_cost
        FROM actual a
        WHERE a.hotel_name = :hotel
        GROUP BY TO_CHAR(a.date, 'YYYY-MM')
        ORDER BY month
    """, p)

    # Top OT employees (all time)
    top_ot = _q("""
        SELECT
            e.name,
            e.department,
            COALESCE(SUM(a.ot_hours), 0)  AS ot_hours,
            COALESCE(SUM(a.ot_pay), 0)    AS ot_pay
        FROM actual a
        JOIN employee e
          ON e.id = a.emp_id AND e.hotel_name = :hotel
        WHERE a.hotel_name = :hotel
          AND a.ot_hours > 0
        GROUP BY e.name, e.department
        ORDER BY ot_hours DESC
        LIMIT 15
    """, p)

    # Employee roster
    employees = _q("""
        SELECT name, department, role, hourly_rate, emp_type
        FROM employee
        WHERE hotel_name = :hotel
        ORDER BY department, name
    """, p)

    # Schedule coverage (shift counts by type, all time)
    schedule_summary = _q("""
        SELECT
            shift_type,
            COUNT(*) AS shift_count,
            COUNT(DISTINCT emp_id) AS unique_employees
        FROM schedule
        WHERE hotel_name = :hotel
        GROUP BY shift_type
        ORDER BY shift_count DESC
    """, p)

    # Last 30 days daily trend
    last30 = _q("""
        SELECT
            a.date,
            COALESCE(SUM(a.hours), 0)              AS total_hours,
            COALESCE(SUM(a.ot_hours), 0)           AS ot_hours,
            COALESCE(SUM(a.reg_pay + a.ot_pay), 0) AS total_cost
        FROM actual a
        WHERE a.hotel_name = :hotel
          AND a.date >= (SELECT MAX(date) - INTERVAL '30 days' FROM actual WHERE hotel_name = :hotel)
        GROUP BY a.date
        ORDER BY a.date
    """, p)

    # OT risk snapshot (latest available)
    ot_risk = _q("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_name = 'ot_risk_all'
        LIMIT 1
    """, {})
    ot_risk_df = pd.DataFrame()
    if not ot_risk.empty:
        ot_risk_df = _q("""
            SELECT *
            FROM ot_risk_all
            WHERE hotel_name = :hotel
            ORDER BY week_start DESC
            LIMIT 30
        """, p)

    return {
        "totals":           totals,
        "by_dept":          by_dept,
        "monthly":          monthly,
        "top_ot":           top_ot,
        "employees":        employees,
        "schedule_summary": schedule_summary,
        "last30":           last30,
        "ot_risk":          ot_risk_df,
    }


# ── Build a rich system context for the AI ───────────────────────────────────
def build_context(hotel: str, data: dict) -> str:
    t = data["totals"]
    if t.empty:
        return "No labor data found in the system."

    row = t.iloc[0]
    total_hours    = float(row.get("total_hours", 0))
    total_ot       = float(row.get("total_ot_hours", 0))
    reg_pay        = float(row.get("total_reg_pay", 0))
    ot_pay         = float(row.get("total_ot_pay", 0))
    total_cost     = reg_pay + ot_pay
    unique_emps    = int(row.get("unique_employees", 0))
    earliest       = str(row.get("earliest_date", "N/A"))
    latest         = str(row.get("latest_date", "N/A"))
    ot_pct         = (total_ot / total_hours * 100) if total_hours > 0 else 0
    ot_cost_pct    = (ot_pay / total_cost * 100) if total_cost > 0 else 0

    dept_txt  = data["by_dept"].to_string(index=False)  if not data["by_dept"].empty  else "No data"
    month_txt = data["monthly"].to_string(index=False)   if not data["monthly"].empty  else "No data"
    ot_txt    = data["top_ot"].to_string(index=False)    if not data["top_ot"].empty   else "No overtime recorded"
    emp_txt   = data["employees"].to_string(index=False) if not data["employees"].empty else "No employees"
    sched_txt = data["schedule_summary"].to_string(index=False) if not data["schedule_summary"].empty else "No schedule data"
    risk_txt  = data["ot_risk"].to_string(index=False)  if not data["ot_risk"].empty  else "No OT risk data"
    last30_txt = data["last30"].to_string(index=False)  if not data["last30"].empty   else "No recent data"

    return f"""=== LABOR DATA SYSTEM CONTEXT FOR {hotel.upper()} ===
Data covers: {earliest} to {latest}

--- OVERALL TOTALS (ALL TIME) ---
Total Hours:        {total_hours:,.1f}
Overtime Hours:     {total_ot:,.1f}  ({ot_pct:.1f}% of total)
Regular Pay:        ${reg_pay:,.2f}
Overtime Pay:       ${ot_pay:,.2f}  ({ot_cost_pct:.1f}% of total cost)
Total Labor Cost:   ${total_cost:,.2f}
Unique Employees:   {unique_emps}

--- BY POSITION (ALL TIME) ---
{dept_txt}

--- MONTHLY TREND ---
{month_txt}

--- TOP OVERTIME EMPLOYEES (ALL TIME) ---
{ot_txt}

--- EMPLOYEE ROSTER ---
{emp_txt}

--- SCHEDULE SUMMARY ---
{sched_txt}

--- LAST 30 DAYS DAILY TREND ---
{last30_txt}

--- OT RISK DATA ---
{risk_txt}
"""


# ── Call Groq ─────────────────────────────────────────────────────────────────
def call_groq(system_ctx: str, question: str) -> str:
    api_key = os.environ.get("GROQ_API_KEY", "")
    if not api_key:
        return "⚠️ GROQ_API_KEY is not configured. Please add it in the Secrets panel."

    client = Groq(api_key=api_key)

    system_prompt = f"""You are an elite hotel labor analytics advisor delivering board-level executive briefings.
Your tone is confident, concise, and insight-driven — like a Chief People Officer presenting to the CEO.
Use plain English. No jargon. Lead with the most important insight. Be direct and specific with numbers.

You have full access to the following live labor data from the hotel's management system:

{system_ctx}

Answer the user's question using ONLY this real data. Never invent or estimate numbers not present above.
Structure your response as:
1. Direct answer to the question (2-3 sentences, bold key numbers using **bold**).
2. Supporting context or trend (2-3 sentences).
3. Bullet list of 2-3 specific, actionable takeaways.
4. One bold "CEO Recommendation" — a single decisive action.
Keep the total response under 350 words."""

    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": question},
        ],
        temperature=0.3,
        max_tokens=800,
    )
    return response.choices[0].message.content.strip()


# ── Auto-generate relevant charts based on available data ────────────────────
def render_charts(data: dict):
    by_dept  = data["by_dept"]
    monthly  = data["monthly"]
    top_ot   = data["top_ot"]
    last30   = data["last30"]

    has_dept    = not by_dept.empty
    has_monthly = not monthly.empty
    has_ot      = not top_ot.empty
    has_last30  = not last30.empty

    if not any([has_dept, has_monthly, has_ot, has_last30]):
        st.info("No chart data available.")
        return

    col1, col2 = st.columns(2)

    with col1:
        if has_dept:
            fig = px.bar(
                by_dept, x="position", y="total_cost",
                color="ot_pay",
                color_continuous_scale=["#4CAF50", "#FF5722"],
                labels={"total_cost": "Total Cost ($)", "position": "Position", "ot_pay": "OT Pay"},
                title="Total Labor Cost by Position",
                template="plotly_white",
            )
            fig.update_layout(title_font_size=14, margin=dict(t=40, b=60),
                              xaxis_tickangle=-35, coloraxis_colorbar=dict(title="OT Pay"))
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        if has_dept:
            fig2 = px.bar(
                by_dept, x="position",
                y=["reg_pay", "ot_pay"],
                barmode="stack",
                labels={"value": "Pay ($)", "position": "Position", "variable": "Type"},
                title="Regular vs Overtime Pay by Position",
                color_discrete_map={"reg_pay": "#2196F3", "ot_pay": "#FF5722"},
                template="plotly_white",
            )
            fig2.update_layout(title_font_size=14, margin=dict(t=40, b=60),
                               xaxis_tickangle=-35,
                               legend=dict(title="", orientation="h", y=1.08))
            fig2.for_each_trace(lambda t: t.update(
                name="Regular Pay" if t.name == "reg_pay" else "OT Pay"
            ))
            st.plotly_chart(fig2, use_container_width=True)

    col3, col4 = st.columns(2)

    with col3:
        if has_monthly:
            fig3 = go.Figure()
            fig3.add_trace(go.Bar(
                x=monthly["month"], y=monthly["total_cost"],
                name="Total Cost", marker_color="#3D52A0", opacity=0.85,
            ))
            fig3.add_trace(go.Scatter(
                x=monthly["month"], y=monthly["ot_hours"],
                name="OT Hours", yaxis="y2",
                mode="lines+markers",
                line=dict(color="#FF5722", width=2),
            ))
            fig3.update_layout(
                title="Monthly Cost & OT Hours Trend",
                template="plotly_white",
                title_font_size=14,
                margin=dict(t=40, b=40),
                legend=dict(orientation="h", y=1.08),
                yaxis=dict(title="Total Cost ($)"),
                yaxis2=dict(title="OT Hours", overlaying="y", side="right"),
                xaxis_tickangle=-35,
            )
            st.plotly_chart(fig3, use_container_width=True)

    with col4:
        if has_ot:
            fig4 = px.bar(
                top_ot.head(10), x="ot_hours", y="name",
                orientation="h",
                color="ot_pay",
                color_continuous_scale=["#FFF176", "#FF5722"],
                labels={"ot_hours": "OT Hours", "name": "Employee", "ot_pay": "OT Pay ($)"},
                title="Top Overtime Employees (All Time)",
                template="plotly_white",
            )
            fig4.update_layout(
                title_font_size=14, margin=dict(t=40, b=20),
                yaxis=dict(autorange="reversed"),
            )
            st.plotly_chart(fig4, use_container_width=True)

    if has_last30:
        fig5 = go.Figure()
        fig5.add_trace(go.Scatter(
            x=last30["date"], y=last30["total_hours"],
            name="Total Hours", mode="lines+markers",
            line=dict(color="#2196F3", width=2),
        ))
        fig5.add_trace(go.Scatter(
            x=last30["date"], y=last30["ot_hours"],
            name="OT Hours", mode="lines+markers",
            line=dict(color="#FF5722", width=2, dash="dash"),
        ))
        fig5.update_layout(
            title="Last 30 Days — Daily Hours",
            template="plotly_white",
            title_font_size=14,
            margin=dict(t=40, b=20),
            legend=dict(orientation="h", y=1.08),
            xaxis_title="Date", yaxis_title="Hours",
        )
        st.plotly_chart(fig5, use_container_width=True)


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
        line-height: 1.8;
        color: #1a1a2e;
        white-space: pre-wrap;
        box-shadow: 0 2px 12px rgba(61,82,160,0.08);
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
        padding: 8px 18px;
        margin: 4px;
        text-align: center;
        min-width: 100px;
    }
    .metric-pill .val { font-size: 20px; font-weight: 800; color: #3D52A0; }
    .metric-pill .val.red { color: #e53935; }
    .metric-pill .lbl { font-size: 11px; color: #666; margin-top: 2px; }
    </style>
    """, unsafe_allow_html=True)

    # Page header
    st.markdown("""
    <div style="display:flex;align-items:center;gap:12px;margin-bottom:4px;">
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:4px;">
            <div style="width:18px;height:18px;background:#3a3a3a;border-radius:4px;"></div>
            <div style="width:18px;height:18px;background:#e02020;border-radius:4px;"></div>
            <div style="width:18px;height:18px;background:#3a3a3a;border-radius:4px;"></div>
            <div style="width:18px;height:18px;background:#3a3a3a;border-radius:4px;"></div>
        </div>
        <div>
            <span style="font-size:22px;font-weight:800;color:#1a1a2e;letter-spacing:-0.5px;">AIPilot</span>
            <span style="font-size:13px;color:#888;margin-left:8px;">Labor Intelligence · All Data</span>
        </div>
    </div>
    <p style="color:#555;font-size:14px;margin-bottom:16px;">
        Ask anything. AIPilot reads your entire labor database to answer accurately.
    </p>
    """, unsafe_allow_html=True)

    # Load all data once (cached per session per hotel)
    cache_key = f"_aipilot_data_{hotel}"
    if cache_key not in st.session_state:
        with st.spinner("Loading system data..."):
            try:
                st.session_state[cache_key] = fetch_all_data(hotel)
            except Exception as e:
                st.error(f"Failed to load data: {e}")
                return

    data = st.session_state[cache_key]
    totals = data["totals"]

    if totals.empty or float(totals.iloc[0].get("total_hours", 0)) == 0:
        st.warning(f"No labor data found for **{hotel}**. Make sure actual hours have been uploaded first.")
        col_r, _ = st.columns([1, 4])
        with col_r:
            if st.button("Reload Data"):
                del st.session_state[cache_key]
                st.rerun()
        return

    row = totals.iloc[0]
    total_hours = float(row.get("total_hours", 0))
    total_ot    = float(row.get("total_ot_hours", 0))
    reg_pay     = float(row.get("total_reg_pay", 0))
    ot_pay      = float(row.get("total_ot_pay", 0))
    total_cost  = reg_pay + ot_pay
    unique_emps = int(row.get("unique_employees", 0))
    earliest    = str(row.get("earliest_date", ""))
    latest      = str(row.get("latest_date", ""))
    ot_pct      = (total_ot / total_hours * 100) if total_hours > 0 else 0

    # Data coverage banner
    st.markdown(f"""
    <div style="background:#f0f4ff;border:1px solid #d0d8f5;border-radius:8px;
                padding:8px 16px;margin-bottom:12px;font-size:12px;color:#555;">
        📊 <b>Data coverage:</b> {earliest} → {latest} &nbsp;|&nbsp;
        {unique_emps} employees &nbsp;|&nbsp; All historical records loaded
        &nbsp;&nbsp;
    </div>
    """, unsafe_allow_html=True)

    # Reload button (small, inline)
    if st.button("🔄 Reload Data", key="reload_data"):
        del st.session_state[cache_key]
        st.rerun()

    # KPI pills
    st.markdown(f"""
    <div style="display:flex;flex-wrap:wrap;gap:0;margin:8px 0 16px 0;">
        <div class="metric-pill">
            <div class="val">{total_hours:,.0f}</div>
            <div class="lbl">Total Hours</div>
        </div>
        <div class="metric-pill">
            <div class="val red">{total_ot:,.1f}</div>
            <div class="lbl">OT Hours ({ot_pct:.1f}%)</div>
        </div>
        <div class="metric-pill">
            <div class="val">${total_cost:,.0f}</div>
            <div class="lbl">Total Labor Cost</div>
        </div>
        <div class="metric-pill">
            <div class="val red">${ot_pay:,.0f}</div>
            <div class="lbl">OT Pay</div>
        </div>
        <div class="metric-pill">
            <div class="val">{unique_emps}</div>
            <div class="lbl">Employees</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Question input
    question = st.text_area(
        "What do you want to know?",
        placeholder=(
            "e.g. Which department has the highest overtime cost?\n"
            "     Who are the top 5 OT earners?\n"
            "     How has our total labor cost trended month over month?\n"
            "     Are we overstaffed on any shift type?"
        ),
        height=100,
        key="ai_question",
    )

    run_btn = st.button("Generate Report", type="primary")

    if not run_btn:
        st.markdown("""
        <div style="background:#f8f9ff;border:1px dashed #c5cde8;border-radius:10px;
                    padding:24px;text-align:center;color:#888;margin-top:12px;">
            <div style="font-size:28px;margin-bottom:6px;">🤖</div>
            <div style="font-size:14px;font-weight:600;color:#555;">Type your question above and click Generate Report</div>
            <div style="font-size:12px;margin-top:4px;">AIPilot has access to your full labor history — ask anything</div>
        </div>
        """, unsafe_allow_html=True)
        # Always show charts below even without a question
        st.markdown("---")
        st.markdown("#### System Overview Charts")
        render_charts(data)
        return

    if not question.strip():
        st.warning("Please enter a question.")
        return

    # Generate AI summary
    with st.spinner("Analyzing your labor data..."):
        try:
            ctx     = build_context(hotel, data)
            summary = call_groq(ctx, question.strip())
        except Exception as e:
            st.error(f"AI error: {e}")
            return

    st.markdown(f"""
    <div style="display:flex;align-items:center;gap:10px;margin-bottom:6px;">
        <span style="font-size:16px;font-weight:700;color:#1a1a2e;">Executive Summary</span>
        <span class="ai-badge">AI · Llama 3.3 70B</span>
    </div>
    <div class="ai-card">{summary}</div>
    """, unsafe_allow_html=True)

    # Charts
    st.markdown("---")
    st.markdown("#### Supporting Charts")
    render_charts(data)

    # Raw data expander
    with st.expander("View Raw Data Tables"):
        tabs = st.tabs(["By Position", "Monthly", "Top OT", "Employees", "Schedule"])
        with tabs[0]: st.dataframe(data["by_dept"],          use_container_width=True)
        with tabs[1]: st.dataframe(data["monthly"],          use_container_width=True)
        with tabs[2]: st.dataframe(data["top_ot"],           use_container_width=True)
        with tabs[3]: st.dataframe(data["employees"],        use_container_width=True)
        with tabs[4]: st.dataframe(data["schedule_summary"], use_container_width=True)
