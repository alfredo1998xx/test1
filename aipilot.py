"""
aipilot.py — AI-powered labor intelligence for LaborPilot
Full database awareness: employees, schedules, positions, costs, OT, mockups.
"""

import os
import re
import io
import base64
import calendar
from typing import Optional
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date, timedelta, datetime
from groq import Groq
from sqlalchemy import text
from db import ENGINE
from email_sender import send_email


# ─────────────────────────── Logo ───────────────────────────────────────────
def _logo_b64() -> str:
    path = os.path.join(os.path.dirname(__file__),
                        "attached_assets", "laborpilot_logo_nobg.png")
    if os.path.exists(path):
        with open(path, "rb") as f:
            return base64.b64encode(f.read()).decode()
    return ""


# ─────────────────────────── Date parser ─────────────────────────────────────
def parse_dates(q: str):
    q = q.lower()
    today = date.today()
    if "today" in q:
        return today, today
    if "yesterday" in q:
        d = today - timedelta(days=1); return d, d
    if "next week" in q:
        d = (7 - today.weekday()) % 7 or 7
        s = today + timedelta(days=d)
        return s, s + timedelta(days=6)
    if "this week" in q:
        s = today - timedelta(days=today.weekday()); return s, today
    if "last week" in q:
        s = today - timedelta(days=today.weekday() + 7)
        return s, s + timedelta(days=6)
    if "next month" in q:
        m = today.month % 12 + 1; y = today.year + (1 if today.month == 12 else 0)
        return date(y, m, 1), date(y, m, calendar.monthrange(y, m)[1])
    if "this month" in q:
        return today.replace(day=1), today
    if "last month" in q:
        f = today.replace(day=1); lp = f - timedelta(days=1)
        return lp.replace(day=1), lp
    if "last 7 days" in q or "past 7" in q:
        return today - timedelta(6), today
    if "last 14" in q or "two weeks" in q:
        return today - timedelta(13), today
    if "last 30" in q or "past 30" in q:
        return today - timedelta(29), today
    if "last 60" in q:
        return today - timedelta(59), today
    if "last 90" in q or "quarter" in q:
        return today - timedelta(89), today
    if "this year" in q or "ytd" in q:
        return today.replace(month=1, day=1), today
    months = {"january":1,"february":2,"march":3,"april":4,"may":5,"june":6,
              "july":7,"august":8,"september":9,"october":10,"november":11,"december":12}
    for mn, mv in months.items():
        if mn in q:
            y = today.year - (1 if mv > today.month else 0)
            return date(y, mv, 1), date(y, mv, calendar.monthrange(y, mv)[1])
    return today - timedelta(29), today


# ─────────────────────────── Intent detection ─────────────────────────────────
def detect_intent(q: str) -> list:
    q = q.lower()
    r = []
    if any(w in q for w in ["overtime","ot ","ot,","overti"]): r.append("overtime")
    if any(w in q for w in ["cost","pay","spend","budget","wage","dollar","labor cost"]): r.append("cost")
    if any(w in q for w in ["department","dept","housekeeping","front desk","food","f&b",
                              "engineering","guest service","maintenance","security","spa"]): r.append("department")
    if any(w in q for w in ["employee","staff","worker","who","top","highest","most","team","agent","name"]): r.append("employee")
    if any(w in q for w in ["trend","daily","day by day","over time","pattern"]): r.append("trend")
    if any(w in q for w in ["schedule","shift","roster","mockup","mock","next week","plan",
                              "coverage","assign","rotation","generate","create a","make a",
                              "table","week"]):  r.append("schedule_mockup")
    if any(w in q for w in ["room","occupancy","occ","occupied"]): r.append("rooms")
    if not r: r = ["overtime","cost","department","employee","trend"]
    return r


# ─────────────────────────── DB helpers ──────────────────────────────────────
def _q(sql: str, p: dict) -> pd.DataFrame:
    with ENGINE.connect() as c:
        return pd.read_sql_query(text(sql), c, params=p)

def fetch_employees(hotel: str) -> pd.DataFrame:
    return _q("SELECT id,name,department,role,hourly_rate,emp_type FROM employee WHERE hotel_name=:h ORDER BY department,name",
              {"h": hotel})

def fetch_positions(hotel: str) -> pd.DataFrame:
    return _q("""SELECT p.name AS position, d.name AS department
                 FROM positions p JOIN departments d ON d.id=p.department_id
                 WHERE p.hotel_name=:h ORDER BY d.name,p.name""", {"h": hotel})

def fetch_existing_schedule(hotel: str, s: date, e: date) -> pd.DataFrame:
    return _q("""SELECT emp.name,emp.department,emp.role,sc.day,sc.shift_type
                 FROM schedule sc JOIN employee emp ON emp.id=sc.emp_id AND emp.hotel_name=:h
                 WHERE sc.hotel_name=:h AND sc.day BETWEEN :s AND :e
                 ORDER BY sc.day,emp.department,emp.name""",
              {"h": hotel, "s": str(s), "e": str(e)})

def fetch_labor(hotel: str, s: date, e: date) -> dict:
    p = {"h": hotel, "s": str(s), "e": str(e)}
    base = "hotel_name=:h AND date BETWEEN :s AND :e"
    res = {}
    res["totals"] = _q(f"SELECT COALESCE(SUM(hours),0) total_hours,COALESCE(SUM(ot_hours),0) total_ot,"
                        f"COALESCE(SUM(reg_pay),0) reg_pay,COALESCE(SUM(ot_pay),0) ot_pay,"
                        f"COUNT(DISTINCT emp_id) unique_emp,COUNT(DISTINCT date) active_days "
                        f"FROM actual WHERE {base}", p)
    res["by_dept"] = _q(f"""SELECT e.department,
        COALESCE(SUM(a.hours),0) total_hours,COALESCE(SUM(a.ot_hours),0) ot_hours,
        COALESCE(SUM(a.reg_pay),0) reg_pay,COALESCE(SUM(a.ot_pay),0) ot_pay,
        COUNT(DISTINCT a.emp_id) employees
        FROM actual a JOIN employee e ON e.id=a.emp_id AND e.hotel_name=:h
        WHERE a.{base} GROUP BY e.department ORDER BY total_hours DESC""", p)
    res["top_emp"] = _q(f"""SELECT e.name,e.department,e.role,
        COALESCE(SUM(a.hours),0) total_hours,COALESCE(SUM(a.ot_hours),0) ot_hours,
        COALESCE(SUM(a.reg_pay+a.ot_pay),0) total_pay
        FROM actual a JOIN employee e ON e.id=a.emp_id AND e.hotel_name=:h
        WHERE a.{base} GROUP BY e.name,e.department,e.role
        ORDER BY ot_hours DESC,total_hours DESC LIMIT 20""", p)
    res["daily"] = _q(f"""SELECT date,COALESCE(SUM(hours),0) total_hours,
        COALESCE(SUM(ot_hours),0) ot_hours,COALESCE(SUM(reg_pay+ot_pay),0) total_cost
        FROM actual WHERE {base} GROUP BY date ORDER BY date""", p)
    return res


# ─────────────────────────── Build prompt ────────────────────────────────────
def build_prompt(hotel, start, end, question, data, intents, emps, positions, sched) -> str:
    days = (end - start).days + 1
    day_labels = [(start + timedelta(i)).strftime("%a %b %d") for i in range(days)]
    period = f"{start.strftime('%b %d')} – {end.strftime('%b %d, %Y')}"

    sections = []
    if not emps.empty:
        sections.append("FULL EMPLOYEE ROSTER:\n" + emps.to_string(index=False))
    if not positions.empty:
        sections.append("POSITIONS BY DEPT:\n" + positions.to_string(index=False))
    if not sched.empty:
        sections.append(f"EXISTING SCHEDULE ({period}):\n" + sched.to_string(index=False))
    else:
        sections.append(f"EXISTING SCHEDULE ({period}): None — create new mockup.")

    tot = data.get("totals", pd.DataFrame())
    if not tot.empty and float(tot.iloc[0].get("total_hours", 0)) > 0:
        r = tot.iloc[0]
        th = float(r["total_hours"]); ot = float(r["total_ot"]); rp = float(r["reg_pay"]); op = float(r["ot_pay"])
        sections.append(f"LABOR ACTUALS ({period}):\n"
                        f"- Hours: {th:,.1f}  OT: {ot:,.1f} ({ot/th*100:.1f}% of total)\n"
                        f"- Reg Pay: ${rp:,.2f}  OT Pay: ${op:,.2f}  Total: ${rp+op:,.2f}")

    bd = data.get("by_dept", pd.DataFrame())
    if not bd.empty: sections.append("DEPT BREAKDOWN:\n" + bd.to_string(index=False))
    te = data.get("top_emp", pd.DataFrame())
    if not te.empty: sections.append("EMPLOYEE DETAIL:\n" + te.to_string(index=False))
    dl = data.get("daily", pd.DataFrame())
    if not dl.empty and len(dl) <= 14: sections.append("DAILY:\n" + dl.to_string(index=False))

    data_block = "\n\n".join(sections)
    is_sched = "schedule_mockup" in intents

    # Detect dept filter
    dept_filter = ""
    ql = question.lower()
    for kw in ["housekeeping","front desk","food & beverage","f&b","engineering",
               "guest service","maintenance","security","spa","finance"]:
        if kw in ql: dept_filter = kw.title(); break

    if is_sched:
        col_csv = ",".join(day_labels)
        dept_note = f" Include ONLY the {dept_filter} department." if dept_filter else " Include all departments."
        sched_rule = f"""
CRITICAL — USER WANTS AN ACTUAL SCHEDULE TABLE WITH REAL EMPLOYEE NAMES.{dept_note}

YOUR RESPONSE MUST FOLLOW THIS EXACT FORMAT AND NOTHING ELSE:

One opening sentence here.

<<<TABLE_START>>>
Employee Name,Department,Role,{col_csv}
FirstName LastName,Department,Role,AM,PM,OFF,RDO,AM,PM,OFF
(one row per relevant employee — use ONLY real names from the FULL EMPLOYEE ROSTER)
<<<TABLE_END>>>

2-3 sentence staffing note for leadership.

RULES:
- Use only real names from the employee roster. Never invent names.
- Shift codes only inside table: AM | PM | MID | NT | OFF | RDO
- Every employee must appear (filtered by dept if requested).
- At least 2 days off per 7-day week per employee.
- Table must be valid CSV — no extra commas, same number of columns every row.
"""
    else:
        sched_rule = ""

    return f"""You are the most capable hotel labor analytics AI. You have FULL access to the live database below.

HOTEL: {hotel}
PERIOD: {period}
QUESTION: "{question}"
{sched_rule}

=== LIVE DATA ===
{data_block}

=== RULES (non-schedule) ===
- Use real employee names and real numbers from the data.
- Lead with the most important number/finding first.
- Give 2-3 numbered action items.
- End with one "Bottom Line" sentence.
- Under 250 words."""


# ─────────────────────────── Table parser ────────────────────────────────────
def extract_table(raw: str):
    m = re.search(r"<<<TABLE_START>>>(.*?)<<<TABLE_END>>>", raw, re.DOTALL)
    if not m:
        return None, raw
    csv_txt = m.group(1).strip()
    rest    = (raw[:m.start()] + "\n" + raw[m.end():]).strip()
    try:
        df = pd.read_csv(io.StringIO(csv_txt))
        df.columns = [c.strip() for c in df.columns]
        for col in df.select_dtypes("object").columns:
            df[col] = df[col].str.strip()
        return df, rest
    except Exception:
        return None, raw


# ─────────────────────────── Groq call ──────────────────────────────────────
def call_ai(prompt: str) -> str:
    key = os.environ.get("GROQ_API_KEY", "")
    if not key:
        return "GROQ_API_KEY not configured. Add it in Secrets."
    client = Groq(api_key=key)
    resp = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.35,
        max_tokens=1400,
    )
    return resp.choices[0].message.content.strip()


# ─────────────────────────── Charts ──────────────────────────────────────────
def render_charts(data: dict, intents: list):
    bd = data.get("by_dept", pd.DataFrame())
    dl = data.get("daily", pd.DataFrame())
    te = data.get("top_emp", pd.DataFrame())
    C1="#3D52A0"; C2="#2196F3"; OT="#FF5722"
    base = dict(template="plotly_white", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                margin=dict(t=40,b=30,l=10,r=10), title_font=dict(size=13,color="#3D52A0"),
                font=dict(color="#444",size=11))
    cols = st.columns(2); c = 0

    if not bd.empty:
        with cols[c%2]:
            fig = go.Figure()
            fig.add_trace(go.Bar(x=bd["department"],y=bd["total_hours"],name="Reg Hours",marker_color=C1,opacity=0.85))
            fig.add_trace(go.Bar(x=bd["department"],y=bd["ot_hours"],name="OT Hours",marker_color=OT,opacity=0.9))
            fig.update_layout(**base,title="Hours by Department",barmode="stack",
                              legend=dict(orientation="h",y=1.12),xaxis_tickangle=-25)
            st.plotly_chart(fig,use_container_width=True)
        c += 1
        if "cost" in intents:
            with cols[c%2]:
                fig2 = go.Figure()
                fig2.add_trace(go.Bar(x=bd["department"],y=bd["reg_pay"],name="Reg Pay",marker_color=C2,opacity=0.85))
                fig2.add_trace(go.Bar(x=bd["department"],y=bd["ot_pay"],name="OT Pay",marker_color=OT,opacity=0.9))
                fig2.update_layout(**base,title="Labor Cost by Dept",barmode="stack",
                                   legend=dict(orientation="h",y=1.12),xaxis_tickangle=-25)
                st.plotly_chart(fig2,use_container_width=True)
            c += 1

    if not dl.empty:
        with cols[c%2]:
            fig3 = go.Figure()
            fig3.add_trace(go.Scatter(x=dl["date"],y=dl["total_hours"],mode="lines+markers",name="Total Hrs",
                                      line=dict(color=C1,width=2.5),fill="tozeroy",fillcolor="rgba(61,82,160,0.07)"))
            fig3.add_trace(go.Scatter(x=dl["date"],y=dl["ot_hours"],mode="lines+markers",name="OT Hrs",
                                      line=dict(color=OT,width=2,dash="dot")))
            fig3.update_layout(**base,title="Daily Hours Trend",legend=dict(orientation="h",y=1.12))
            st.plotly_chart(fig3,use_container_width=True)
        c += 1

    ot_e = te[te["ot_hours"]>0].head(8) if not te.empty else pd.DataFrame()
    if not ot_e.empty:
        with cols[c%2]:
            fig4 = go.Figure(go.Bar(x=ot_e["ot_hours"],y=ot_e["name"],orientation="h",
                marker=dict(color=ot_e["ot_hours"],colorscale=[[0,C1],[1,OT]],showscale=False),
                text=ot_e["ot_hours"].apply(lambda v:f"{v:.1f}h"),textposition="inside"))
            fig4.update_layout(**base,title="Top OT Employees",yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig4,use_container_width=True)


# ─────────────────────────── Main render ─────────────────────────────────────
def render_aipilot(hotel: str):

    # ── Init session state keys ──
    for k, v in [("ai_q",""), ("ai_result",None), ("ai_data",None),
                  ("ai_intents",[]), ("ai_period",""), ("ai_sched_df",None),
                  ("ai_fill","")]:
        if k not in st.session_state:
            st.session_state[k] = v

    LOGO = _logo_b64()

    # ── CSS — remove Streamlit's blue top bar, clean layout ──
    st.markdown("""
    <style>
    /* Remove blue Streamlit header */
    header[data-testid="stHeader"] { background: #ffffff !important; box-shadow: 0 1px 3px rgba(0,0,0,0.06) !important; }
    header[data-testid="stHeader"] * { color: #444 !important; }
    div[data-testid="stToolbar"] { display:none !important; }

    .ai-hero {
        background: linear-gradient(135deg, #3D52A0 0%, #5C6FBF 55%, #8697C4 100%);
        border-radius: 14px; padding: 24px 30px 20px; margin-bottom: 18px;
        box-shadow: 0 4px 20px rgba(61,82,160,0.2);
    }
    .ai-logo-row { display:flex; align-items:center; gap:10px; margin-bottom:4px; }
    .ai-logo-row img { height:34px; filter:brightness(0) invert(1); }
    .ai-title { font-size:24px; font-weight:900; color:#fff; letter-spacing:-0.5px; }
    .ai-subtitle { color:rgba(255,255,255,0.75); font-size:12.5px; margin-top:2px; }
    .ai-live-badge {
        display:inline-flex; align-items:center; gap:5px;
        background:rgba(255,255,255,0.15); border:1px solid rgba(255,255,255,0.3);
        border-radius:20px; padding:3px 10px; font-size:11px; color:#fff; font-weight:600;
    }
    .ai-live-dot { width:6px;height:6px;border-radius:50%;background:#4CAF50;
        animation:aipulse 1.6s ease-in-out infinite;display:inline-block; }
    @keyframes aipulse{0%,100%{opacity:1;transform:scale(1)}50%{opacity:0.3;transform:scale(0.7)}}
    .ai-hero-row { display:flex;justify-content:space-between;align-items:flex-start; }
    .ai-hero-desc { margin-top:10px; color:rgba(255,255,255,0.62); font-size:12.5px; line-height:1.6; }

    .ai-sug-row { display:flex; flex-wrap:wrap; gap:6px; margin: 8px 0 12px 0; }
    .ai-response-card {
        background:#f8f9ff; border:1px solid #e0e4f0; border-left:4px solid #3D52A0;
        border-radius:10px; padding:18px 22px; margin:10px 0 12px 0;
        font-size:14px; line-height:1.8; color:#1a1a2e; white-space:pre-wrap;
        box-shadow:0 2px 10px rgba(61,82,160,0.07);
    }
    .ai-model-tag {
        display:inline-flex; align-items:center; gap:6px;
        background:linear-gradient(135deg,#3D52A0,#8697C4);
        border-radius:20px; padding:3px 12px; font-size:11px; color:#fff; font-weight:600; margin-bottom:8px;
    }
    .ai-metrics-row { display:flex;flex-wrap:wrap;gap:10px;margin:12px 0 4px 0; }
    .ai-metric { background:#f0f4ff;border:1px solid #d0d8f5;border-radius:10px;padding:10px 16px;min-width:105px;text-align:center; }
    .ai-metric .val { font-size:19px;font-weight:800;color:#3D52A0;line-height:1.2; }
    .ai-metric .val.red { color:#FF5722; }
    .ai-metric .val.teal { color:#2196F3; }
    .ai-metric .lbl { font-size:10px;color:#888;margin-top:2px; }
    .ai-period-pill {
        display:inline-flex;align-items:center;gap:4px;
        background:#eef2ff;border:1px solid #c7d2fe;border-radius:8px;
        padding:4px 12px;font-size:11px;color:#3D52A0;margin-bottom:10px;
    }
    .ai-tbl-hdr { color:#3D52A0;font-size:12px;font-weight:700;letter-spacing:0.5px;text-transform:uppercase;margin:16px 0 4px 0; }
    .ai-chart-hdr { color:#3D52A0;font-size:12px;font-weight:700;letter-spacing:0.5px;text-transform:uppercase;margin:18px 0 4px 0; }
    .ai-divider { height:1px;background:linear-gradient(90deg,#3D52A0,transparent);opacity:0.15;margin-bottom:10px; }
    .ai-email-box { background:#f0f4ff;border:1px solid #c7d2fe;border-radius:10px;padding:16px 20px;margin-top:18px; }
    .ai-empty { background:#f8f9ff;border:1px dashed #c7d2fe;border-radius:12px;padding:40px 24px;text-align:center;margin-top:6px; }
    .ai-empty-icon { font-size:38px;margin-bottom:10px; }
    .ai-empty-title { color:#3D52A0;font-size:14px;font-weight:700;margin-bottom:4px; }
    .ai-empty-sub { color:#888;font-size:12.5px; }
    /* Input field */
    div[data-testid="stTextInput"] > div > div > input {
        border:1.5px solid #c7d2fe !important; border-radius:10px !important;
        font-size:14px !important; padding:11px 15px !important;
    }
    div[data-testid="stTextInput"] > div > div > input:focus {
        border-color:#3D52A0 !important; box-shadow:0 0 0 3px rgba(61,82,160,0.09) !important;
    }
    /* Suggestion buttons styling */
    div[data-testid="stHorizontalBlock"] div[data-testid="stButton"] button {
        border-radius: 20px !important; font-size:11.5px !important;
        padding: 4px 12px !important; height: auto !important;
    }
    </style>
    """, unsafe_allow_html=True)

    # ── Hero ──
    logo_img = f'<img src="data:image/png;base64,{LOGO}" />' if LOGO else ""
    st.markdown(f"""
    <div class="ai-hero">
      <div class="ai-hero-row">
        <div>
          <div class="ai-logo-row">{logo_img}<div class="ai-title">AIPilot</div></div>
          <div class="ai-subtitle">Labor Intelligence for {hotel}</div>
        </div>
        <div class="ai-live-badge"><span class="ai-live-dot"></span> LIVE DATA</div>
      </div>
      <div class="ai-hero-desc">
        Ask anything — schedules, OT risk, costs, staff mockups, trends, reports.<br>
        I read your full live database and respond with real names and real numbers.
      </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Suggestion chips ──
    SUGS = [
        "Create a full schedule mockup for next week",
        "Who had the most OT this month?",
        "Highest labor cost department this month",
        "OT risk employees this week",
        "Total labor cost summary last month",
    ]

    # Pre-fill text input if suggestion was clicked last rerun
    if st.session_state.ai_fill:
        st.session_state["_ai_q_input"] = st.session_state.ai_fill
        st.session_state.ai_fill = ""

    sug_cols = st.columns(len(SUGS))
    for i, sug in enumerate(SUGS):
        with sug_cols[i]:
            short = (sug[:22] + "…") if len(sug) > 22 else sug
            if st.button(short, key=f"sug_{i}", help=sug, use_container_width=True):
                st.session_state.ai_fill = sug
                st.rerun()

    # ── Question input + Ask button ──
    col_input, col_btn = st.columns([6, 1])
    with col_input:
        question = st.text_input(
            "",
            value=st.session_state.get("_ai_q_input", ""),
            placeholder='e.g. "Create a mockup schedule for housekeeping next week" · "Who had the most OT?"',
            label_visibility="collapsed",
            key="_ai_q_input",
        )
    with col_btn:
        ask = st.button("Ask", type="primary", use_container_width=True)

    # ── Decide whether to run AI ──
    run_ai = ask and question.strip()

    # If new question → clear cached result
    if run_ai and question.strip() != st.session_state.ai_q:
        st.session_state.ai_result   = None
        st.session_state.ai_sched_df = None
        st.session_state.ai_data     = None

    # Show empty state if nothing asked yet
    if not question.strip() and st.session_state.ai_result is None:
        st.markdown("""
        <div class="ai-empty">
          <div class="ai-empty-icon">🤖</div>
          <div class="ai-empty-title">What do you want to know?</div>
          <div class="ai-empty-sub">
            Click a suggestion above or type a question.<br>
            I can read every table in your database and answer anything.
          </div>
        </div>
        """, unsafe_allow_html=True)
        return

    # ── Run AI (only when asked and question changed) ──
    if run_ai and st.session_state.ai_result is None:
        start_date, end_date = parse_dates(question)
        intents = detect_intent(question)
        days    = (end_date - start_date).days + 1
        period  = f"{start_date.strftime('%b %d')} – {end_date.strftime('%b %d, %Y')}"

        with st.spinner("Reading your database…"):
            try:
                data     = fetch_labor(hotel, start_date, end_date)
                emps     = fetch_employees(hotel)
                pos      = fetch_positions(hotel)
                ex_sched = fetch_existing_schedule(hotel, start_date, end_date)
            except Exception as e:
                st.error(f"Database error: {e}"); return

        with st.spinner("Generating answer…"):
            try:
                prompt = build_prompt(hotel, start_date, end_date, question,
                                      data, intents, emps, pos, ex_sched)
                raw    = call_ai(prompt)
            except Exception as e:
                st.error(f"AI error: {e}"); return

        sched_df, summary = extract_table(raw)

        # Cache everything
        st.session_state.ai_q        = question
        st.session_state.ai_result   = summary
        st.session_state.ai_data     = data
        st.session_state.ai_intents  = intents
        st.session_state.ai_period   = period
        st.session_state.ai_sched_df = sched_df
        st.session_state.ai_start    = start_date
        st.session_state.ai_end      = end_date

    # ── Display cached result ──
    if st.session_state.ai_result is None:
        return

    summary     = st.session_state.ai_result
    data        = st.session_state.ai_data
    intents     = st.session_state.ai_intents
    period      = st.session_state.ai_period
    sched_df    = st.session_state.ai_sched_df
    start_date  = st.session_state.get("ai_start")
    end_date    = st.session_state.get("ai_end")

    if start_date and end_date:
        days = (end_date - start_date).days + 1
        st.markdown(f'<div class="ai-period-pill">◈ Period: {period} · {days} day{"s" if days!=1 else ""}</div>',
                    unsafe_allow_html=True)

    # KPI pills
    tot = data.get("totals", pd.DataFrame()) if data else pd.DataFrame()
    has_act = not tot.empty and float(tot.iloc[0].get("total_hours", 0)) > 0
    if has_act:
        r = tot.iloc[0]
        th=float(r["total_hours"]); ot=float(r["total_ot"]); rp=float(r["reg_pay"]); op=float(r["ot_pay"])
        ue=int(r["unique_emp"]); otp=(ot/th*100) if th>0 else 0
        st.markdown(f"""<div class="ai-metrics-row">
            <div class="ai-metric"><div class="val">{th:,.0f}</div><div class="lbl">Total Hours</div></div>
            <div class="ai-metric"><div class="val red">{ot:,.1f}</div><div class="lbl">OT Hours ({otp:.1f}%)</div></div>
            <div class="ai-metric"><div class="val teal">${rp+op:,.0f}</div><div class="lbl">Labor Cost</div></div>
            <div class="ai-metric"><div class="val red">${op:,.0f}</div><div class="lbl">OT Pay</div></div>
            <div class="ai-metric"><div class="val">{ue}</div><div class="lbl">Employees</div></div>
        </div>""", unsafe_allow_html=True)

    # AI response
    st.markdown('<div class="ai-model-tag">✦ Llama 3.3 · 70B</div>', unsafe_allow_html=True)
    if summary.strip():
        st.markdown(f'<div class="ai-response-card">{summary}</div>', unsafe_allow_html=True)

    # Schedule table
    if sched_df is not None:
        st.markdown('<div class="ai-tbl-hdr">📅 Generated Schedule</div><div class="ai-divider"></div>',
                    unsafe_allow_html=True)
        SHIFT_COLORS = {
            "AM":  "background-color:#dbeafe;color:#1e40af;font-weight:600",
            "PM":  "background-color:#dcfce7;color:#166534;font-weight:600",
            "MID": "background-color:#fef9c3;color:#854d0e;font-weight:600",
            "OFF": "background-color:#f3f4f6;color:#6b7280",
            "RDO": "background-color:#fee2e2;color:#991b1b;font-weight:600",
            "NT":  "background-color:#ede9fe;color:#5b21b6;font-weight:600",
        }
        fixed_cols = {"Employee Name","Department","Role","Name"}
        shift_cols = [c for c in sched_df.columns if c not in fixed_cols]

        def style_cell(val):
            return SHIFT_COLORS.get(str(val).strip().upper(), "")

        styled = sched_df.style.applymap(style_cell, subset=shift_cols) if shift_cols else sched_df.style
        st.dataframe(styled, use_container_width=True, height=min(50 + 36*len(sched_df), 600))

        csv_bytes = sched_df.to_csv(index=False).encode()
        st.download_button("⬇ Download Schedule (.csv)", data=csv_bytes,
                           file_name=f"schedule_{start_date}_{end_date}.csv",
                           mime="text/csv", key="dl_sched")

    # Employee roster expander (schedule requests)
    if "schedule_mockup" in intents:
        try:
            emps_now = fetch_employees(hotel)
            if not emps_now.empty:
                with st.expander("Employee Roster Used", expanded=False):
                    st.dataframe(emps_now, use_container_width=True)
        except Exception:
            pass

    # Charts (non-schedule)
    if has_act and "schedule_mockup" not in intents:
        st.markdown('<div class="ai-chart-hdr">Supporting Charts</div><div class="ai-divider"></div>',
                    unsafe_allow_html=True)
        render_charts(data, intents)

    # ── Email ──
    st.markdown('<div class="ai-email-box">', unsafe_allow_html=True)
    st.markdown("**📧 Email This Report**")
    email_input = st.text_input(
        "Recipient(s) — separate with commas",
        placeholder="you@example.com, manager@hotel.com",
        key="ai_email_to",
    )
    if st.button("Send Report via Email", key="ai_send_email"):
        if not email_input.strip():
            st.warning("Enter at least one email address.")
        else:
            recips = [e.strip() for e in email_input.split(",") if e.strip()]
            with st.spinner("Sending…"):
                try:
                    _send_report(recips, hotel, st.session_state.ai_q, summary, period, data, sched_df)
                    st.success(f"✅ Report sent to: {', '.join(recips)}")
                except Exception as e:
                    st.error(f"Email failed: {e}")
    st.markdown("</div>", unsafe_allow_html=True)


# ─────────────────────────── Email builder ────────────────────────────────────
def _send_report(recips, hotel, question, summary, period, data, sched_df: Optional[pd.DataFrame]):
    tot = data.get("totals", pd.DataFrame())
    bd  = data.get("by_dept", pd.DataFrame())
    te  = data.get("top_emp", pd.DataFrame())
    r   = tot.iloc[0] if not tot.empty else {}
    th  = float(r.get("total_hours", 0)); ot = float(r.get("total_ot", 0))
    rp  = float(r.get("reg_pay", 0));    op  = float(r.get("ot_pay", 0))

    lines = [
        f"AIPilot Labor Report — {hotel}",
        f"Period: {period}",
        "=" * 60, "",
        f"QUESTION: {question}", "",
        "AI SUMMARY:", summary, "",
    ]
    if sched_df is not None:
        lines += ["SCHEDULE MOCKUP:", sched_df.to_string(index=False), ""]
    if th > 0:
        lines += ["KEY METRICS:",
                  f"  Total Hours : {th:,.1f}",
                  f"  OT Hours    : {ot:,.1f}",
                  f"  Total Cost  : ${rp+op:,.2f}",
                  f"  OT Pay      : ${op:,.2f}", ""]
    if not bd.empty:
        lines += ["DEPT BREAKDOWN:", bd.to_string(index=False), ""]
    if not te.empty:
        lines += ["TOP EMPLOYEES (OT):", te.head(10).to_string(index=False), ""]
    lines.append(f"— LaborPilot AIPilot · {date.today().strftime('%B %d, %Y')}")

    send_email(recips, f"AIPilot Report — {hotel} | {period}", "\n".join(lines))
