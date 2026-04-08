"""
aipilot.py — AIPilot: full-app AI intelligence for LaborPilot
Modern light design · reads all 20+ tables · schedule/cost/OT/rooms/reports
"""

import os, re, io, base64, calendar
from typing import Optional
from datetime import date, timedelta
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from groq import Groq
from sqlalchemy import text
from db import ENGINE
from email_sender import send_email

# ─── palette ────────────────────────────────────────────────────────────────
PRIMARY   = "#3D52A0"
LIGHT_BG  = "#f5f6fa"
CARD_BG   = "#ffffff"
BORDER    = "#e2e8f0"
TXT       = "#1e293b"
TXT2      = "#64748b"
RED       = "#ef4444"
GREEN     = "#22c55e"
BLUE      = "#3b82f6"
AMBER     = "#f59e0b"

# ─── logo ────────────────────────────────────────────────────────────────────
def _logo() -> str:
    p = os.path.join(os.path.dirname(__file__), "attached_assets", "laborpilot_logo_nobg.png")
    if os.path.exists(p):
        with open(p, "rb") as f: return base64.b64encode(f.read()).decode()
    return ""

# ─── date parser ─────────────────────────────────────────────────────────────
def parse_dates(q: str):
    q = q.lower(); today = date.today()
    if "today"     in q: return today, today
    if "yesterday" in q: d=today-timedelta(1); return d,d
    if "next week" in q:
        d=(7-today.weekday())%7 or 7; s=today+timedelta(d); return s,s+timedelta(6)
    if "this week" in q: return today-timedelta(today.weekday()),today
    if "last week" in q:
        s=today-timedelta(today.weekday()+7); return s,s+timedelta(6)
    if "next month" in q:
        m=today.month%12+1; y=today.year+(1 if today.month==12 else 0)
        return date(y,m,1),date(y,m,calendar.monthrange(y,m)[1])
    if "this month" in q: return today.replace(day=1),today
    if "last month" in q:
        f=today.replace(day=1); lp=f-timedelta(1); return lp.replace(day=1),lp
    for n,d in [("7",6),("14",13),("30",29),("60",59),("90",89)]:
        if f"last {n}" in q or f"past {n}" in q: return today-timedelta(d),today
    if "quarter" in q: return today-timedelta(89),today
    if "this year" in q or "ytd" in q: return today.replace(month=1,day=1),today
    months={"january":1,"february":2,"march":3,"april":4,"may":5,"june":6,
            "july":7,"august":8,"september":9,"october":10,"november":11,"december":12}
    for mn,mv in months.items():
        if mn in q:
            y=today.year-(1 if mv>today.month else 0)
            return date(y,mv,1),date(y,mv,calendar.monthrange(y,mv)[1])
    return today-timedelta(29),today

# ─── intent detection ─────────────────────────────────────────────────────────
def detect(q: str) -> list:
    q=q.lower(); r=[]
    if any(w in q for w in ["overtime","ot ","ot,","overti","risk"]): r.append("overtime")
    if any(w in q for w in ["cost","pay","spend","budget","wage","dollar","labor cost","payroll"]): r.append("cost")
    if any(w in q for w in ["department","dept","housekeeping","front desk","food","f&b","engineering",
                              "guest service","maintenance","security","spa","banquet"]): r.append("department")
    if any(w in q for w in ["employee","staff","worker","who","top","highest","most","team","agent","name","person"]): r.append("employee")
    if any(w in q for w in ["trend","daily","day by day","over time","pattern","history"]): r.append("trend")
    if any(w in q for w in ["schedule","shift","roster","mockup","mock","next week","plan","coverage",
                              "assign","rotation","generate","create","make a","build","table","week"]): r.append("schedule")
    if any(w in q for w in ["room","occupancy","occ","occupied","adr","revpar","pickup","forecast","otb"]): r.append("rooms")
    if any(w in q for w in ["position","role","title","job","labor standard","standard"]): r.append("positions")
    if any(w in q for w in ["report","summary","breakdown","analysis","overview"]): r.append("report")
    if not r: r=["overtime","cost","department","employee"]
    return r

# ─── DB helpers ──────────────────────────────────────────────────────────────
def Q(sql,p={}):
    with ENGINE.connect() as c: return pd.read_sql_query(text(sql),c,params=p)

def employees(hotel):
    return Q("SELECT id,name,department,role,hourly_rate,emp_type FROM employee WHERE hotel_name=:h ORDER BY department,name",{"h":hotel})

def positions(hotel):
    return Q("SELECT p.name position,p.department_id,d.name department FROM positions p JOIN departments d ON d.id=p.department_id WHERE p.hotel_name=:h ORDER BY d.name,p.name",{"h":hotel})

def schedule_range(hotel,s,e):
    return Q("SELECT emp.name,emp.department,emp.role,sc.day,sc.shift_type FROM schedule sc JOIN employee emp ON emp.id=sc.emp_id AND emp.hotel_name=:h WHERE sc.hotel_name=:h AND sc.day BETWEEN :s AND :e ORDER BY sc.day,emp.department,emp.name",{"h":hotel,"s":str(s),"e":str(e)})

def labor(hotel,s,e):
    p={"h":hotel,"s":str(s),"e":str(e)}; base="hotel_name=:h AND date BETWEEN :s AND :e"
    out={}
    out["totals"]=Q(f"SELECT COALESCE(SUM(hours),0) total_hours,COALESCE(SUM(ot_hours),0) total_ot,COALESCE(SUM(reg_pay),0) reg_pay,COALESCE(SUM(ot_pay),0) ot_pay,COUNT(DISTINCT emp_id) unique_emp,COUNT(DISTINCT date) active_days FROM actual WHERE {base}",p)
    out["by_dept"]=Q(f"SELECT e.department,COALESCE(SUM(a.hours),0) total_hours,COALESCE(SUM(a.ot_hours),0) ot_hours,COALESCE(SUM(a.reg_pay),0) reg_pay,COALESCE(SUM(a.ot_pay),0) ot_pay,COUNT(DISTINCT a.emp_id) employees FROM actual a JOIN employee e ON e.id=a.emp_id AND e.hotel_name=:h WHERE a.{base} GROUP BY e.department ORDER BY total_hours DESC",p)
    out["top_emp"]=Q(f"SELECT e.name,e.department,e.role,COALESCE(SUM(a.hours),0) total_hours,COALESCE(SUM(a.ot_hours),0) ot_hours,COALESCE(SUM(a.reg_pay+a.ot_pay),0) total_pay FROM actual a JOIN employee e ON e.id=a.emp_id AND e.hotel_name=:h WHERE a.{base} GROUP BY e.name,e.department,e.role ORDER BY ot_hours DESC,total_hours DESC LIMIT 20",p)
    out["daily"]=Q(f"SELECT date,COALESCE(SUM(hours),0) total_hours,COALESCE(SUM(ot_hours),0) ot_hours,COALESCE(SUM(reg_pay+ot_pay),0) total_cost FROM actual WHERE {base} GROUP BY date ORDER BY date",p)
    out["ot_risk"]=Q(f"SELECT e.name,e.department,e.hourly_rate,COALESCE(SUM(a.hours),0) hrs_to_date,COALESCE(SUM(a.ot_hours),0) ot_hrs FROM actual a JOIN employee e ON e.id=a.emp_id AND e.hotel_name=:h WHERE a.{base} GROUP BY e.name,e.department,e.hourly_rate HAVING COALESCE(SUM(a.hours),0)>=35 ORDER BY hrs_to_date DESC",p)
    return out

def rooms_data(hotel,s,e):
    try:
        p={"h":hotel,"s":str(s),"e":str(e)}
        r=Q("SELECT date,rooms_sold,rooms_avail,occ_pct,adr,revpar FROM room_kpis WHERE hotel_name=:h AND date BETWEEN :s AND :e ORDER BY date",p)
        return r
    except Exception: return pd.DataFrame()

def labor_standards(hotel):
    try: return Q("SELECT * FROM labor_standards WHERE hotel_name=:h",{"h":hotel})
    except Exception: return pd.DataFrame()

def planning(hotel,s,e):
    try: return Q("SELECT * FROM planning_summary WHERE hotel_name=:h AND date BETWEEN :s AND :e ORDER BY date",{"h":hotel,"s":str(s),"e":str(e)})
    except Exception: return pd.DataFrame()

# ─── prompt builder ───────────────────────────────────────────────────────────
def build_prompt(hotel,start,end,question,data,intents,emps,pos,sched,rooms_df,standards_df) -> str:
    days=(end-start).days+1
    day_labels=[(start+timedelta(i)).strftime("%a %b %d") for i in range(days)]
    period=f"{start.strftime('%b %d')} – {end.strftime('%b %d, %Y')}"
    q_low=question.lower()

    secs=[]
    if not emps.empty:   secs.append("EMPLOYEE ROSTER:\n"+emps.to_string(index=False))
    if not pos.empty:    secs.append("POSITIONS/DEPTS:\n"+pos.to_string(index=False))
    if not sched.empty:  secs.append(f"EXISTING SCHEDULE ({period}):\n"+sched.to_string(index=False))
    else:                secs.append(f"EXISTING SCHEDULE ({period}): None — create new mockup if asked.")

    tot=data.get("totals",pd.DataFrame())
    if not tot.empty and float(tot.iloc[0].get("total_hours",0))>0:
        r=tot.iloc[0]; th=float(r["total_hours"]); ot=float(r["total_ot"]); rp=float(r["reg_pay"]); op=float(r["ot_pay"])
        secs.append(f"LABOR ACTUALS ({period}):\n- Hours:{th:,.1f}  OT:{ot:,.1f}({ot/th*100:.1f}%)\n- Reg Pay:${rp:,.2f}  OT Pay:${op:,.2f}  Total:${rp+op:,.2f}")

    bd=data.get("by_dept",pd.DataFrame())
    if not bd.empty: secs.append("BY DEPARTMENT:\n"+bd.to_string(index=False))
    te=data.get("top_emp",pd.DataFrame())
    if not te.empty: secs.append("EMPLOYEE DETAIL:\n"+te.to_string(index=False))
    dl=data.get("daily",pd.DataFrame())
    if not dl.empty and len(dl)<=14: secs.append("DAILY BREAKDOWN:\n"+dl.to_string(index=False))
    ri=data.get("ot_risk",pd.DataFrame())
    if not ri.empty: secs.append("OT RISK (employees ≥35h):\n"+ri.to_string(index=False))
    if not rooms_df.empty: secs.append(f"ROOM KPIs ({period}):\n"+rooms_df.to_string(index=False))
    if not standards_df.empty: secs.append("LABOR STANDARDS:\n"+standards_df.to_string(index=False))

    block="\n\n".join(secs)

    # Detect dept filter
    dept=""
    for kw in ["housekeeping","front desk","food & beverage","f&b","engineering",
               "guest service","maintenance","security","spa","banquet","finance"]:
        if kw in q_low: dept=kw.title(); break

    is_sched="schedule" in intents
    if is_sched:
        col_csv=",".join(day_labels)
        dnote=f" Include ONLY {dept} dept." if dept else " Include ALL departments."
        sched_block=f"""
CRITICAL: USER WANTS AN ACTUAL SCHEDULE TABLE WITH REAL NAMES FROM THE ROSTER.{dnote}

RESPOND IN EXACTLY THIS FORMAT:

Opening sentence (plain text).

<<<TABLE_START>>>
Employee Name,Department,Role,{col_csv}
[Use real employee names from the EMPLOYEE ROSTER above — one row per relevant employee]
<<<TABLE_END>>>

2-3 sentence staffing leadership note.

RULES FOR THE TABLE:
- Only real names from the EMPLOYEE ROSTER. Never invent names.
- Shift codes: AM | PM | MID | NT | OFF | RDO only.
- Minimum 2 days off per 7-day period per person.
- Valid CSV — same number of columns every row. No quotes unless name has comma.
"""
    else:
        sched_block=""

    return f"""You are LaborPilot's AI — the smartest hotel labor intelligence system available.
You have COMPLETE ACCESS to the hotel's live database for {hotel}.
You answer EXACTLY what is asked using real names, real numbers from the data below.

HOTEL: {hotel}
PERIOD: {period}
QUESTION: "{question}"
{sched_block}
=== LIVE DATABASE ===
{block}

=== RESPONSE RULES (non-schedule) ===
- Use real names and numbers from the data above.
- First sentence: the single most important number or finding.
- 2-3 numbered bullet points with specifics.
- End: one "Bottom Line:" sentence — what to act on now.
- Max 220 words. No markdown headers. Plain conversational language.
- If the data is empty for the period, say so honestly and suggest checking a different date range."""

# ─── table parser ─────────────────────────────────────────────────────────────
def extract_table(raw):
    m=re.search(r"<<<TABLE_START>>>(.*?)<<<TABLE_END>>>",raw,re.DOTALL)
    if not m: return None,raw
    rest=(raw[:m.start()]+"\n"+raw[m.end():]).strip()
    try:
        df=pd.read_csv(io.StringIO(m.group(1).strip()))
        df.columns=[c.strip() for c in df.columns]
        for c in df.select_dtypes("object").columns: df[c]=df[c].str.strip()
        return df,rest
    except: return None,raw

# ─── AI call ──────────────────────────────────────────────────────────────────
def call_ai(prompt):
    key=os.environ.get("GROQ_API_KEY","")
    if not key: return "GROQ_API_KEY not configured."
    cl=Groq(api_key=key)
    r=cl.chat.completions.create(model="llama-3.3-70b-versatile",
        messages=[{"role":"user","content":prompt}],temperature=0.3,max_tokens=1500)
    return r.choices[0].message.content.strip()

# ─── charts ───────────────────────────────────────────────────────────────────
def render_charts(data,intents):
    bd=data.get("by_dept",pd.DataFrame()); dl=data.get("daily",pd.DataFrame())
    te=data.get("top_emp",pd.DataFrame()); ri=data.get("ot_risk",pd.DataFrame())
    base=dict(template="plotly_white",paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
              margin=dict(t=36,b=24,l=8,r=8),title_font=dict(size=13,color=PRIMARY),font=dict(color=TXT2,size=11))
    cols=st.columns(2); c=0

    if not bd.empty:
        with cols[c%2]:
            f=go.Figure()
            f.add_trace(go.Bar(x=bd["department"],y=bd["total_hours"],name="Reg Hours",marker_color=PRIMARY,opacity=0.8))
            f.add_trace(go.Bar(x=bd["department"],y=bd["ot_hours"],name="OT Hours",marker_color=RED,opacity=0.85))
            f.update_layout(**base,title="Hours by Department",barmode="stack",
                            legend=dict(orientation="h",y=1.1,x=0),xaxis_tickangle=-30)
            st.plotly_chart(f,use_container_width=True); c+=1
        if "cost" in intents:
            with cols[c%2]:
                f2=go.Figure()
                f2.add_trace(go.Bar(x=bd["department"],y=bd["reg_pay"],name="Reg Pay",marker_color=BLUE,opacity=0.8))
                f2.add_trace(go.Bar(x=bd["department"],y=bd["ot_pay"],name="OT Pay",marker_color=RED,opacity=0.85))
                f2.update_layout(**base,title="Cost by Department",barmode="stack",
                                 legend=dict(orientation="h",y=1.1,x=0),xaxis_tickangle=-30)
                st.plotly_chart(f2,use_container_width=True); c+=1

    if not dl.empty:
        with cols[c%2]:
            f3=go.Figure()
            f3.add_trace(go.Scatter(x=dl["date"],y=dl["total_hours"],name="Hours",
                                    line=dict(color=PRIMARY,width=2.5),fill="tozeroy",fillcolor="rgba(61,82,160,0.06)"))
            f3.add_trace(go.Scatter(x=dl["date"],y=dl["ot_hours"],name="OT",
                                    line=dict(color=RED,width=2,dash="dot")))
            f3.update_layout(**base,title="Daily Hours Trend",legend=dict(orientation="h",y=1.1))
            st.plotly_chart(f3,use_container_width=True); c+=1

    ot_e=ri if not ri.empty else (te[te["ot_hours"]>0].head(8) if not te.empty else pd.DataFrame())
    if not ot_e.empty and "ot_hours" in ot_e.columns:
        with cols[c%2]:
            ot_e=ot_e.head(8).sort_values("ot_hours")
            f4=go.Figure(go.Bar(x=ot_e["ot_hours"],y=ot_e["name"],orientation="h",
                marker=dict(color=ot_e["ot_hours"],colorscale=[[0,PRIMARY],[1,RED]],showscale=False),
                text=ot_e["ot_hours"].apply(lambda v:f"{v:.1f}h"),textposition="inside"))
            f4.update_layout(**base,title="OT Risk by Employee",yaxis=dict(autorange="reversed"))
            st.plotly_chart(f4,use_container_width=True); c+=1

# ─── email ────────────────────────────────────────────────────────────────────
def do_email(recips,hotel,question,summary,period,data,sched_df):
    tot=data.get("totals",pd.DataFrame()); bd=data.get("by_dept",pd.DataFrame())
    te=data.get("top_emp",pd.DataFrame()); r=tot.iloc[0] if not tot.empty else {}
    th=float(r.get("total_hours",0)); ot=float(r.get("total_ot",0))
    rp=float(r.get("reg_pay",0)); op=float(r.get("ot_pay",0))
    lines=[f"AIPilot Labor Report — {hotel}",f"Period: {period}","="*60,"",
           f"QUESTION: {question}","","AI SUMMARY:",summary,""]
    if sched_df is not None: lines+=["SCHEDULE:",sched_df.to_string(index=False),""]
    if th>0: lines+=["METRICS:",f"  Hours:{th:,.1f}  OT:{ot:,.1f}  Cost:${rp+op:,.2f}  OT Pay:${op:,.2f}",""]
    if not bd.empty: lines+=["BY DEPT:",bd.to_string(index=False),""]
    if not te.empty: lines+=["TOP EMPLOYEES:",te.head(10).to_string(index=False),""]
    lines.append(f"— LaborPilot AIPilot · {date.today().strftime('%B %d, %Y')}")
    send_email(recips,f"AIPilot Report — {hotel} | {period}","\n".join(lines))

# ═══════════════════════════ MAIN RENDER ═════════════════════════════════════
def render_aipilot(hotel: str):

    # ── session init ──
    for k,v in [("ai_q",""),("ai_result",None),("ai_data",None),
                 ("ai_intents",[]),("ai_period",""),("ai_sched",None),
                 ("ai_start",None),("ai_end",None),("ai_fill","")]:
        if k not in st.session_state: st.session_state[k]=v

    LOGO=_logo()

    # ── pre-fill suggestion ──
    if st.session_state.ai_fill:
        st.session_state["_q"]=st.session_state.ai_fill
        st.session_state.ai_fill=""

    # ════════════════════════ CSS ═════════════════════════════════════════════
    st.markdown(f"""
    <style>
    /* Page background */
    .stApp, .main .block-container {{ background:{LIGHT_BG} !important; padding-top:0 !important; }}
    .block-container {{ max-width:900px !important; padding:0 2rem 4rem 2rem !important; }}

    /* Hide Streamlit chrome */
    header[data-testid="stHeader"] {{
        background:#ffffff !important;
        border-bottom:1px solid {BORDER} !important;
        box-shadow:0 1px 4px rgba(0,0,0,0.04) !important;
    }}
    div[data-testid="stToolbar"], #MainMenu {{ display:none !important; }}
    footer {{ display:none !important; }}

    /* ── HERO CARD ── */
    .ai-hero {{
        background:#ffffff;
        border:1px solid {BORDER};
        border-radius:16px;
        padding:28px 32px 24px;
        margin:20px 0 18px 0;
        box-shadow:0 1px 8px rgba(0,0,0,0.05);
        display:flex; align-items:center; justify-content:space-between;
    }}
    .ai-hero-left {{ display:flex; align-items:center; gap:14px; }}
    .ai-hero-logo {{ height:42px; width:auto; }}
    .ai-hero-text .title {{
        font-size:24px; font-weight:800; color:{TXT};
        letter-spacing:-0.5px; line-height:1.1;
    }}
    .ai-hero-text .sub {{
        font-size:12.5px; color:{TXT2}; margin-top:3px;
    }}
    .ai-hero-right {{ text-align:right; }}
    .ai-badge {{
        display:inline-flex; align-items:center; gap:5px;
        background:#f0f4ff; border:1px solid #c7d2fe;
        border-radius:20px; padding:4px 12px;
        font-size:11px; color:{PRIMARY}; font-weight:600;
    }}
    .ai-badge-dot {{
        width:6px; height:6px; border-radius:50%; background:#22c55e;
        display:inline-block; animation:pulse 1.8s ease-in-out infinite;
    }}
    @keyframes pulse{{0%,100%{{opacity:1;transform:scale(1)}}50%{{opacity:0.3;transform:scale(0.65)}}}}
    .ai-hotel-name {{
        font-size:11px; color:{TXT2}; margin-top:5px;
        white-space:nowrap; overflow:hidden; text-overflow:ellipsis; max-width:200px;
    }}

    /* ── SEARCH AREA ── */
    .ai-search-label {{
        font-size:13px; font-weight:600; color:{TXT2};
        text-transform:uppercase; letter-spacing:0.6px;
        margin-bottom:8px;
    }}
    div[data-testid="stTextInput"] > div > div > input {{
        background:#ffffff !important;
        border:1.5px solid {BORDER} !important;
        border-radius:12px !important;
        font-size:15px !important;
        padding:13px 18px !important;
        color:{TXT} !important;
        box-shadow:0 1px 4px rgba(0,0,0,0.04) !important;
        transition:border-color 0.15s, box-shadow 0.15s !important;
    }}
    div[data-testid="stTextInput"] > div > div > input:focus {{
        border-color:{PRIMARY} !important;
        box-shadow:0 0 0 3px rgba(61,82,160,0.08) !important;
    }}
    div[data-testid="stTextInput"] > div > div > input::placeholder {{
        color:#94a3b8 !important;
    }}

    /* ── SUGGESTION CHIPS ── */
    .sug-wrap {{ display:flex; flex-wrap:wrap; gap:7px; margin:10px 0 6px 0; }}
    div[data-testid="stButton"] button[kind="secondary"] {{
        background:#ffffff !important;
        border:1px solid {BORDER} !important;
        border-radius:20px !important;
        color:{TXT2} !important;
        font-size:12px !important;
        padding:5px 14px !important;
        box-shadow:0 1px 3px rgba(0,0,0,0.05) !important;
        transition:all 0.15s !important;
        height:auto !important;
    }}
    div[data-testid="stButton"] button[kind="secondary"]:hover {{
        border-color:{PRIMARY} !important;
        color:{PRIMARY} !important;
        box-shadow:0 2px 8px rgba(61,82,160,0.12) !important;
    }}

    /* ── RESULT CARD ── */
    .ai-card {{
        background:#ffffff;
        border:1px solid {BORDER};
        border-radius:14px;
        padding:22px 26px;
        margin:14px 0 12px 0;
        box-shadow:0 2px 10px rgba(0,0,0,0.05);
    }}
    .ai-card-header {{
        display:flex; align-items:center; justify-content:space-between;
        margin-bottom:14px;
    }}
    .ai-model-pill {{
        display:inline-flex; align-items:center; gap:5px;
        background:#f0f4ff; border:1px solid #c7d2fe;
        border-radius:20px; padding:3px 11px;
        font-size:11px; color:{PRIMARY}; font-weight:600;
    }}
    .ai-response {{
        font-size:14.5px; line-height:1.85; color:{TXT};
        white-space:pre-wrap;
    }}

    /* ── KPI PILLS ── */
    .kpi-row {{ display:flex; flex-wrap:wrap; gap:9px; margin:12px 0; }}
    .kpi-pill {{
        background:#ffffff; border:1px solid {BORDER};
        border-radius:10px; padding:10px 16px; min-width:100px; text-align:center;
        box-shadow:0 1px 3px rgba(0,0,0,0.04);
    }}
    .kpi-val {{ font-size:18px; font-weight:800; color:{PRIMARY}; line-height:1.2; }}
    .kpi-val.r {{ color:{RED}; }}
    .kpi-val.b {{ color:{BLUE}; }}
    .kpi-val.g {{ color:#16a34a; }}
    .kpi-lbl {{ font-size:10px; color:{TXT2}; margin-top:2px; }}

    /* ── PERIOD PILL ── */
    .period-pill {{
        display:inline-flex; align-items:center; gap:5px;
        background:#f8faff; border:1px solid #dde4f7;
        border-radius:8px; padding:4px 12px;
        font-size:11px; color:{PRIMARY}; margin-bottom:10px; font-weight:500;
    }}

    /* ── SECTION HEADER ── */
    .sec-hdr {{
        font-size:11.5px; font-weight:700; color:{TXT2};
        text-transform:uppercase; letter-spacing:0.6px;
        margin:20px 0 6px 0; padding-bottom:6px;
        border-bottom:1px solid {BORDER};
    }}

    /* ── SCHEDULE TABLE ── */
    .sched-legend {{ display:flex; flex-wrap:wrap; gap:8px; margin:8px 0 12px 0; }}
    .sched-chip {{
        display:inline-flex; align-items:center; gap:5px;
        border-radius:6px; padding:3px 10px; font-size:11px; font-weight:600;
    }}

    /* ── EMAIL BOX ── */
    .email-box {{
        background:#f8faff; border:1px solid #dde4f7;
        border-radius:12px; padding:18px 22px; margin-top:20px;
    }}
    .email-title {{ font-size:13px; font-weight:700; color:{PRIMARY}; margin-bottom:12px; }}

    /* ── EMPTY STATE ── */
    .empty-wrap {{
        background:#ffffff; border:1px solid {BORDER};
        border-radius:16px; padding:52px 32px;
        text-align:center; margin:10px 0;
        box-shadow:0 1px 6px rgba(0,0,0,0.04);
    }}
    .empty-icon {{ font-size:44px; margin-bottom:14px; }}
    .empty-title {{ font-size:17px; font-weight:700; color:{TXT}; margin-bottom:6px; }}
    .empty-sub {{ font-size:13px; color:{TXT2}; line-height:1.6; }}
    </style>
    """, unsafe_allow_html=True)

    # ════════════════════════ HERO ═══════════════════════════════════════════
    logo_img=(f'<img src="data:image/png;base64,{LOGO}" class="ai-hero-logo" />' if LOGO
              else '<span style="font-size:28px">🤖</span>')
    st.markdown(f"""
    <div class="ai-hero">
      <div class="ai-hero-left">
        {logo_img}
        <div class="ai-hero-text">
          <div class="title">AIPilot</div>
          <div class="sub">Full-app labor intelligence · reads every table</div>
        </div>
      </div>
      <div class="ai-hero-right">
        <div class="ai-badge"><span class="ai-badge-dot"></span> LIVE DATA</div>
        <div class="ai-hotel-name">{hotel}</div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    # ════════════════════════ SUGGESTIONS ════════════════════════════════════
    SUGS=[
        "Create a schedule mockup for all departments next week",
        "Who had the most OT this month?",
        "Top labor cost department — last 30 days",
        "Show OT risk employees this week",
        "Full labor cost summary this month",
    ]
    sug_cols=st.columns(len(SUGS))
    for i,s in enumerate(SUGS):
        with sug_cols[i]:
            short=(s[:20]+"…") if len(s)>20 else s
            if st.button(short,key=f"sug_{i}",help=s,use_container_width=True):
                st.session_state.ai_fill=s; st.rerun()

    # ════════════════════════ SEARCH BAR ═════════════════════════════════════
    st.markdown('<div class="ai-search-label">Ask AIPilot anything</div>', unsafe_allow_html=True)
    col_q,col_btn=st.columns([7,1])
    with col_q:
        question=st.text_input("",
            value=st.session_state.get("_q",""),
            placeholder='e.g. "Make a schedule mockup for housekeeping next week" · "Who had OT last month?"',
            label_visibility="collapsed", key="_q")
    with col_btn:
        ask=st.button("Ask →", type="primary", use_container_width=True)

    # ════════════════════════ LOGIC ═══════════════════════════════════════════
    run=ask and question.strip()

    # Clear cache if new question
    if run and question.strip()!=st.session_state.ai_q:
        st.session_state.ai_result=None; st.session_state.ai_sched=None; st.session_state.ai_data=None

    # Empty state
    if not question.strip() and st.session_state.ai_result is None:
        st.markdown("""
        <div class="empty-wrap">
          <div class="empty-icon">🤖</div>
          <div class="empty-title">What do you want to know?</div>
          <div class="empty-sub">
            Click a suggestion above or type your question and press <strong>Ask →</strong><br>
            I have access to your schedules, costs, OT data, room stats, employee roster — everything.
          </div>
        </div>""", unsafe_allow_html=True)
        return

    # Run AI
    if run and st.session_state.ai_result is None:
        s_date,e_date=parse_dates(question)
        intents=detect(question)

        with st.spinner("Reading database…"):
            try:
                data=labor(hotel,s_date,e_date)
                emps=employees(hotel); pos=positions(hotel)
                sched=schedule_range(hotel,s_date,e_date)
                rooms_df=rooms_data(hotel,s_date,e_date)
                std_df=labor_standards(hotel)
            except Exception as e: st.error(f"DB error: {e}"); return

        with st.spinner("Generating answer…"):
            try:
                prompt=build_prompt(hotel,s_date,e_date,question,data,intents,emps,pos,sched,rooms_df,std_df)
                raw=call_ai(prompt)
            except Exception as e: st.error(f"AI error: {e}"); return

        sched_df,summary=extract_table(raw)
        period=f"{s_date.strftime('%b %d')} – {e_date.strftime('%b %d, %Y')}"

        st.session_state.ai_q=question; st.session_state.ai_result=summary
        st.session_state.ai_data=data;  st.session_state.ai_intents=intents
        st.session_state.ai_period=period; st.session_state.ai_sched=sched_df
        st.session_state.ai_start=s_date; st.session_state.ai_end=e_date

    # ════════════════════════ DISPLAY ════════════════════════════════════════
    if st.session_state.ai_result is None: return

    summary   = st.session_state.ai_result
    data      = st.session_state.ai_data
    intents   = st.session_state.ai_intents
    period    = st.session_state.ai_period
    sched_df  = st.session_state.ai_sched
    s_date    = st.session_state.ai_start
    e_date    = st.session_state.ai_end

    # Period pill
    days=(e_date-s_date).days+1 if s_date and e_date else 0
    st.markdown(f'<div class="period-pill">📅 {period} &nbsp;·&nbsp; {days} day{"s" if days!=1 else ""}</div>',
                unsafe_allow_html=True)

    # KPI pills
    tot=data.get("totals",pd.DataFrame()) if data else pd.DataFrame()
    has_act=not tot.empty and float(tot.iloc[0].get("total_hours",0))>0
    if has_act:
        r=tot.iloc[0]
        th=float(r["total_hours"]); ot=float(r["total_ot"])
        rp=float(r["reg_pay"]); op=float(r["ot_pay"]); ue=int(r["unique_emp"])
        otp=ot/th*100 if th>0 else 0
        st.markdown(f"""<div class="kpi-row">
          <div class="kpi-pill"><div class="kpi-val">{th:,.0f}</div><div class="kpi-lbl">Total Hours</div></div>
          <div class="kpi-pill"><div class="kpi-val r">{ot:,.1f}</div><div class="kpi-lbl">OT Hrs ({otp:.1f}%)</div></div>
          <div class="kpi-pill"><div class="kpi-val b">${rp+op:,.0f}</div><div class="kpi-lbl">Labor Cost</div></div>
          <div class="kpi-pill"><div class="kpi-val r">${op:,.0f}</div><div class="kpi-lbl">OT Pay</div></div>
          <div class="kpi-pill"><div class="kpi-val g">{ue}</div><div class="kpi-lbl">Employees</div></div>
        </div>""", unsafe_allow_html=True)

    # AI response card
    st.markdown(f"""
    <div class="ai-card">
      <div class="ai-card-header">
        <div class="ai-model-pill">✦ Llama 3.3 · 70B</div>
      </div>
      <div class="ai-response">{summary}</div>
    </div>""", unsafe_allow_html=True)

    # ── Schedule table ──
    if sched_df is not None:
        st.markdown('<div class="sec-hdr">Generated Schedule</div>', unsafe_allow_html=True)
        # Legend
        st.markdown("""<div class="sched-legend">
          <span class="sched-chip" style="background:#dbeafe;color:#1e40af">🌅 AM — Morning</span>
          <span class="sched-chip" style="background:#dcfce7;color:#15803d">🌆 PM — Afternoon</span>
          <span class="sched-chip" style="background:#fef9c3;color:#92400e">🕛 MID — Mid-shift</span>
          <span class="sched-chip" style="background:#ede9fe;color:#5b21b6">🌙 NT — Night</span>
          <span class="sched-chip" style="background:#f3f4f6;color:#6b7280">☐ OFF</span>
          <span class="sched-chip" style="background:#fee2e2;color:#991b1b">RDO</span>
        </div>""", unsafe_allow_html=True)

        SHIFT_CLR={
            "AM":"background-color:#dbeafe;color:#1e40af;font-weight:600",
            "PM":"background-color:#dcfce7;color:#15803d;font-weight:600",
            "MID":"background-color:#fef9c3;color:#92400e;font-weight:600",
            "OFF":"background-color:#f3f4f6;color:#9ca3af",
            "RDO":"background-color:#fee2e2;color:#991b1b;font-weight:600",
            "NT":"background-color:#ede9fe;color:#5b21b6;font-weight:600",
        }
        fixed={"Employee Name","Department","Role","Name"}
        shift_cols=[c for c in sched_df.columns if c not in fixed]
        styled=sched_df.style.applymap(lambda v:SHIFT_CLR.get(str(v).strip().upper(),""),subset=shift_cols) if shift_cols else sched_df.style
        st.dataframe(styled,use_container_width=True,height=min(56+36*len(sched_df),620))
        st.download_button("⬇ Download Schedule (.csv)",
            data=sched_df.to_csv(index=False).encode(),
            file_name=f"schedule_{s_date}_{e_date}.csv",mime="text/csv",key="dl_sched")

    # ── Charts ──
    if has_act and "schedule" not in intents:
        st.markdown('<div class="sec-hdr">Supporting Charts</div>', unsafe_allow_html=True)
        render_charts(data,intents)

    # ── Roster expander for schedule requests ──
    if "schedule" in intents:
        try:
            emps_now=employees(hotel)
            if not emps_now.empty:
                with st.expander("Employee Roster Used", expanded=False):
                    st.dataframe(emps_now,use_container_width=True)
        except: pass

    # ── EMAIL ──
    st.markdown('<div class="email-box"><div class="email-title">📧 Email This Report</div>',
                unsafe_allow_html=True)
    ec1,ec2=st.columns([4,1])
    with ec1:
        email_to=st.text_input("Recipients (comma-separated)",
            placeholder="you@example.com, manager@hotel.com",
            label_visibility="visible", key="ai_email_to")
    with ec2:
        st.markdown("<div style='height:28px'></div>",unsafe_allow_html=True)
        send_btn=st.button("Send", key="ai_send_email", type="primary", use_container_width=True)
    if send_btn:
        if not email_to.strip():
            st.warning("Enter at least one email address.")
        else:
            recips=[e.strip() for e in email_to.split(",") if e.strip()]
            with st.spinner("Sending…"):
                try:
                    do_email(recips,hotel,st.session_state.ai_q,summary,period,data,sched_df)
                    st.success(f"✅ Sent to: {', '.join(recips)}")
                except Exception as e:
                    st.error(f"Failed: {e}")
    st.markdown("</div>",unsafe_allow_html=True)
