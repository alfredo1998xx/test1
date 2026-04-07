# ============================================================
# report_logic.py  — Pure OT Risk logic (no Streamlit)
# Used by scheduler.py to generate OT Risk data
# ============================================================

import pandas as pd
import db
from sqlalchemy import or_
from datetime import datetime
from collections import defaultdict

from db import (
    Actual, Department, Position, Employee, Schedule,
    RoomForecast, RoomActual, RoomOTBPickup
)


# ------------------------------------------------------------
# Utility: Convert shift "09:00-17:00" → hours (7.5)
# ------------------------------------------------------------
def parse_shift_to_hours(shift_str):
    try:
        start, end = shift_str.split("-")
        start_dt = pd.to_datetime(start, format="%H:%M")
        end_dt = pd.to_datetime(end, format="%H:%M")
        hours = (end_dt - start_dt).total_seconds() / 3600
        if hours < 0:
            hours += 24
        return max(0, hours - 0.5)  # subtract 30-min break
    except:
        return 0


# ------------------------------------------------------------
# MAIN FUNCTION — run_ot_risk_report()
# ------------------------------------------------------------
def run_ot_risk_report(
    session,
    week_start,
    week_end,
    sched_week_start,
    sched_week_end,
    dept,
    pos,
):
    """
    Pure OT Risk logic.
    Returns a pandas DataFrame with full OT risk metrics.
    """

    # ============================================================
    # QUERY ACTUAL HOURS
    # ============================================================
    q = (
        session.query(
            Actual.emp_id.label("Number"),
            Actual.date.label("Business Date"),
            (Actual.hours + Actual.ot_hours).label("Hours"),
            Position.name.label("Position"),
            Department.name.label("Department")
        )
        .join(Position, Actual.position_id == Position.id)
        .join(Department, Position.department_id == Department.id)
        .filter(Actual.date.between(week_start, week_end))
        .filter(or_(Actual.hours != 0, Actual.ot_hours != 0))
    )

    if dept and dept != "(All)":
        q = q.filter(Department.name == dept)
    if pos and pos != "(All)":
        q = q.filter(Position.name == pos)

    raw = pd.DataFrame(
        q.all(),
        columns=["Number", "Business Date", "Hours", "Position", "Department"]
    )

    if raw.empty:
        return None

    # Normalize employee ID
    raw["Number"] = (
        pd.to_numeric(raw["Number"], errors="coerce")
        .fillna(0).astype(int).astype(str).str.zfill(5)
    )

    # ============================================================
    # MERGE EMPLOYEE NAMES
    # ============================================================
    employees = session.query(Employee).all()

    emp_df = pd.DataFrame([
        {
            "id": e.id,
            "name": e.name,
            "hourly_rate": getattr(e, "hourly_rate", 0)
        }
        for e in employees
    ])

    parts = emp_df["name"].astype(str).str.extract(
        r"^\s*(?P<Last_Name>[^,]+),\s*(?P<First_Name>[^\d]+?)\s+(?P<ID>\d+)"
    )

    emp_df["ID"] = parts["ID"].fillna("").astype(str).str.zfill(5)
    emp_df["First Name"] = parts["First_Name"].str.strip()
    emp_df["Last Name"] = parts["Last_Name"].str.strip()
    emp_df["match_ID"] = emp_df["ID"].astype(str).str.lstrip("0")

    raw["match_ID"] = raw["Number"].astype(str).str.lstrip("0")

    merged = raw.merge(
        emp_df[["match_ID", "First Name", "Last Name", "hourly_rate"]],
        on="match_ID",
        how="left"
    )

    # ============================================================
    # GROUP TOTAL HOURS + DAYS WORKED
    # ============================================================
    agg = merged.groupby(["Number", "First Name", "Last Name"]).agg(
        total_hours=("Hours", "sum"),
        days_worked=("Business Date", pd.Series.nunique)
    ).reset_index()

    # ============================================================
    # MERGE HOURLY RATE INTO AGG
    # ============================================================
    agg = agg.merge(
        emp_df[["ID", "hourly_rate"]],
        left_on="Number",
        right_on="ID",
        how="left"
    )

    agg["hourly_rate"] = agg["hourly_rate"].fillna(0)

    # ============================================================
    # QUERY SCHEDULED SHIFTS (Mon → Sun)
    # ============================================================
    sched_rows = (
        session.query(
            Employee.name,
            Schedule.day,
            Schedule.shift_type
        )
        .join(Employee, Employee.id == Schedule.emp_id)
        .filter(Schedule.day.between(sched_week_start, sched_week_end))
        .all()
    )

    sched_df = pd.DataFrame(sched_rows, columns=["name", "day", "shift_type"])

    if not sched_df.empty:
        sched_df["shift_type"] = (
            sched_df["shift_type"].fillna("").astype(str).str.upper().str.strip()
        )

        # Remove OFF days
        sched_df = sched_df[sched_df["shift_type"] != "OFF"]

        sched_df["Number"] = sched_df["name"].str.extract(r"(\d+)$")[0].str.zfill(5)
        sched_df["day"] = pd.to_datetime(sched_df["day"])

        valid_numbers = set(agg["Number"].unique())
        sched_df = sched_df[sched_df["Number"].isin(valid_numbers)]

        merged["Business Date"] = pd.to_datetime(merged["Business Date"])
        last_worked = (
            merged.groupby("Number")["Business Date"]
            .max()
            .reset_index()
            .rename(columns={"Business Date": "last_worked"})
        )

        sched_df = sched_df.merge(last_worked, on="Number", how="left")

        sched_df["last_worked"] = sched_df["last_worked"].fillna(
            pd.to_datetime(week_start) - pd.Timedelta(days=1)
        )

        sched_df["after_work"] = sched_df["day"] > sched_df["last_worked"]

        sched_counts = (
            sched_df.groupby("Number")["day"]
            .nunique().reset_index(name="Days Scheduled")
        )

        sched_future = sched_df[sched_df["after_work"]].copy()
        days_remaining = (
            sched_future.groupby("Number")["day"]
            .nunique().reset_index(name="Days Remaining")
        )

        sched_future["shift_hours"] = sched_future["shift_type"].apply(parse_shift_to_hours)
        future_hours = (
            sched_future.groupby("Number")["shift_hours"]
            .sum().reset_index(name="Future Scheduled Hrs")
        )

        agg = agg.merge(sched_counts, on="Number", how="left")
        agg = agg.merge(days_remaining, on="Number", how="left")
        agg = agg.merge(future_hours, on="Number", how="left")

    else:
        agg["Days Scheduled"] = 0
        agg["Days Remaining"] = 0
        agg["Future Scheduled Hrs"] = 0

    # ============================================================
    # FINAL METRICS
    # ============================================================
    agg["Days Scheduled"] = agg["Days Scheduled"].fillna(0).astype(int)
    agg["Days Remaining"] = agg["Days Remaining"].fillna(0).astype(int)
    agg["Future Scheduled Hrs"] = agg["Future Scheduled Hrs"].fillna(0)

    agg["Total Hrs Worked + Schedule"] = (
        agg["total_hours"] + agg["Future Scheduled Hrs"]
    ).round(2)

    def classify_ot_risk(row):
        total = row["Total Hrs Worked + Schedule"]
        remaining = row["Days Remaining"]
        if total <= 40:
            return "No Risk"
        elif remaining > 0:
            return "At Risk"
        else:
            return "OT"

    agg["OT Risk"] = agg.apply(classify_ot_risk, axis=1)

    def estimate_risk_percent(row):
        if row["OT Risk"] == "No Risk":
            return "0%"
        if row["Days Remaining"] == 0:
            return "100%"
        elif row["Days Remaining"] == 1:
            return "80%"
        elif row["Days Remaining"] == 2:
            return "60%"
        elif row["Days Remaining"] == 3:
            return "40%"
        return "20%"

    agg["OT Risk %"] = agg.apply(estimate_risk_percent, axis=1)

    agg["Projected OT"] = agg["Total Hrs Worked + Schedule"].apply(
        lambda h: max(round(h - 40, 2), 0)
    )

    agg["OT Cost"] = (agg["Projected OT"] * agg["hourly_rate"] * 1.5).round(2)

    return agg


# ------------------------------------------------------------
# FORMATTER — Match Streamlit EXACT export layout
# ------------------------------------------------------------
def prepare_ot_risk_export(df):
    """
    Takes the OT Risk DF produced by run_ot_risk_report()
    and returns EXACTLY the same export_df used in Streamlit.
    """

    df = df.rename(columns={
        "days_worked": "Days Worked",
        "Total Hrs Worked + Schedule": "Total"
    })

    export_df = df[[
        "Number",
        "First Name",
        "Last Name",
        "Days Worked",
        "Days Scheduled",
        "Days Remaining",
        "Total",
        "OT Risk",
        "OT Risk %",
        "Projected OT",
        "OT Cost"
    ]].copy()

    export_df = export_df.drop_duplicates(subset=["Number"]).reset_index(drop=True)

    return export_df

def _pull_week_kpi_totals(session, Model, label, week_start, week_end, dept=None):
    """
    Helper function:
    Summaries for a KPI model between week_start and week_end.
    Returns a DataFrame: KPI | <label>
    """

    q = session.query(Model).filter(
        Model.date.between(week_start, week_end)
    )

    # Optional department filter
    if dept and dept != "(All)":
        q = q.filter(Model.department == dept)

    rows = q.all()

    data = defaultdict(float)
    for r in rows:
        data[r.kpi] += r.value

    if not data:
        return pd.DataFrame(columns=["KPI", label])

    return pd.DataFrame([(k, v) for k, v in data.items()], columns=["KPI", label])


# ------------------------------------------------------------
# MAIN FUNCTION — run_forecast_variance_report()
# ------------------------------------------------------------
def run_forecast_variance_report(
    session,
    week_start,
    week_end,
    dept=None
):
    """
    Pure Forecast Variance logic.
    Returns a merged DataFrame with full KPI comparisons.
    """

    df_forecast = _pull_week_kpi_totals(
        session, RoomForecast, "Forecast", week_start, week_end, dept
    )
    df_actual = _pull_week_kpi_totals(
        session, RoomActual, "Actual", week_start, week_end, dept
    )
    df_otb = _pull_week_kpi_totals(
        session, RoomOTBPickup, "OTB + Pickup", week_start, week_end, dept
    )

    # If NO data in any model → return None
    if df_forecast.empty and df_actual.empty and df_otb.empty:
        return None

    merged = (
        df_forecast
        .merge(df_actual, on="KPI", how="outer")
        .merge(df_otb, on="KPI", how="outer")
        .fillna(0)
    )

    merged = merged[["KPI", "Actual", "Forecast", "OTB + Pickup"]]

    # Variances (numeric only here — Streamlit adds arrows)
    merged["Δ Actual - Forecast"] = merged["Actual"] - merged["Forecast"]
    merged["Δ OTB - Forecast"] = merged["OTB + Pickup"] - merged["Forecast"]

    return merged


# ------------------------------------------------------------
# FORMATTER — prepare_forecast_variance_export()
# ------------------------------------------------------------
def prepare_forecast_variance_export(df):
    """
    Returns a cleaned export DataFrame suitable for Excel/PDF output.
    No arrows — scheduler will output numeric values only.
    """

    export_df = df.copy()

    # Numeric formatting (no arrows)
    for col in ["Actual", "Forecast", "OTB + Pickup",
                "Δ Actual - Forecast", "Δ OTB - Forecast"]:
        export_df[col] = export_df[col].astype(float).round(2)

    return export_df

# ------------------------------------------------------------
# PRODUCTIVITY INDEX — BACKEND VERSION (for Scheduler)
# ------------------------------------------------------------
def run_productivity_index_report(
    session,
    week_start,
    week_end,
    dept,
    pos
):
    import pandas as pd

    # ---------- Load base tables ----------
    from db import Actual, Position, Department, LaborStandard, RoomActual

    dept_df = pd.read_sql(session.query(Department).statement, session.bind)
    pos_df  = pd.read_sql(session.query(Position).statement, session.bind)
    std_df  = pd.read_sql(session.query(LaborStandard).statement, session.bind)
    ah_df   = pd.read_sql(session.query(Actual).statement, session.bind)
    kpi_df  = pd.read_sql(session.query(RoomActual).statement, session.bind)

    dept_df = dept_df.rename(columns={"id": "dept_id", "name": "dept"})
    pos_df  = pos_df.rename(columns={"id": "position_id", "name": "position"})

    # ---------- Filter standards ----------
    std_df = std_df.merge(
        pos_df[["position_id", "position", "department_id"]],
        on="position_id",
        how="left"
    ).merge(
        dept_df[["dept_id", "dept"]],
        left_on="department_id",
        right_on="dept_id",
        how="left"
    )

    std_df = std_df[std_df["dept"] == dept]

    if pos and pos != "All Positions":
        std_df = std_df[std_df["position"] == pos]

    # ---------- Actual hours ----------
    ah_df = ah_df[
        (ah_df["source"].isin(["manual", "contract"])) &
        (ah_df["date"].between(week_start, week_end))
    ]

    ah_df["total_hours"] = ah_df[["hours", "ot_hours"]].sum(axis=1)

    hours_summary = (
        ah_df.groupby("position_id")["total_hours"]
        .sum()
        .reset_index()
        .rename(columns={"total_hours": "actual_hours"})
    )

    hours_summary = hours_summary.merge(
        pos_df[["position_id", "position"]],
        on="position_id",
        how="left"
    )

    # ---------- KPI Outputs ----------
    kpi_df = kpi_df[kpi_df["date"].between(week_start, week_end)]

    kpi_summary = (
        kpi_df.groupby("kpi")["value"]
        .sum()
        .reset_index()
        .rename(columns={"value": "output"})
    )

    # ---------- Final Rows ----------
    rows = []

    target_positions = (
        [pos] if pos and pos != "All Positions"
        else sorted(std_df["position"].dropna().unique().tolist())
    )

    for position in target_positions:

        pos_std = std_df[std_df["position"] == position].copy()
        if pos_std.empty:
            continue

        actual_hours = hours_summary.loc[
            hours_summary["position"] == position, "actual_hours"
        ].sum()

        tmp = pos_std.merge(
            kpi_summary.rename(columns={"kpi": "metric"}),
            on="metric",
            how="left"
        )
        tmp["output"] = tmp["output"].fillna(0.0)

        tmp["std_hrs_per_unit"] = tmp["standard"].apply(
            lambda s: (8.0 / s) if (s not in [None, "", 0]) else None
        )

        total_output = float(tmp["output"].sum())

        if total_output > 0 and tmp["std_hrs_per_unit"].notna().any():
            tmp["weighted"] = tmp["output"] * tmp["std_hrs_per_unit"].fillna(0.0)
            std_weighted = tmp["weighted"].sum() / total_output
        else:
            std_weighted = None

        productivity = (actual_hours / total_output) if total_output > 0 else 0.0

        if std_weighted is not None:
            variance = std_weighted - productivity
            arrow = "▲" if variance > 0 else ("▼" if variance < 0 else "")
            variance_str = f"{round(variance, 2)} {arrow}"
        else:
            variance_str = ""

        rows.append({
            "Position": position,
            "Output": round(total_output, 2),
            "Hours": round(float(actual_hours or 0), 2),
            "Productivity (hrs/unit)": round(productivity, 2),
            "Standard (hrs/unit)": round(std_weighted, 2) if std_weighted is not None else "",
            "Variance": variance_str
        })

    final_df = pd.DataFrame(rows)

    if final_df.empty:
        return None

    # ---------- TOTAL ROW ----------
    total_output = final_df["Output"].sum()
    total_hours = final_df["Hours"].sum()
    weighted_productivity = (total_hours / total_output) if total_output else 0.0

    merged_all = std_df.merge(
        kpi_summary.rename(columns={"kpi": "metric"}),
        on="metric",
        how="left"
    )

    merged_all["output"] = merged_all["output"].fillna(0.0)
    merged_all["std_hrs_per_unit"] = merged_all["standard"].apply(
        lambda s: (8.0 / s) if s not in [None, "", 0] else None
    )

    if total_output and merged_all["std_hrs_per_unit"].notna().any():
        merged_all["weighted"] = merged_all["output"] * merged_all["std_hrs_per_unit"].fillna(0.0)
        total_weighted_std = merged_all["weighted"].sum() / total_output
    else:
        total_weighted_std = None

    variance_total = (total_weighted_std - weighted_productivity) if total_weighted_std is not None else None
    arrow_total = "▲" if (variance_total is not None and variance_total > 0) else ("▼" if (variance_total is not None and variance_total < 0) else "")

    total_row = {
        "Position": "TOTAL",
        "Output": round(total_output, 2),
        "Hours": round(total_hours, 2),
        "Productivity (hrs/unit)": round(weighted_productivity, 2),
        "Standard (hrs/unit)": round(total_weighted_std, 2) if total_weighted_std is not None else "",
        "Variance": f"{round(variance_total, 2)} {arrow_total}" if variance_total is not None else ""
    }

    final_df = pd.concat([final_df, pd.DataFrame([total_row])], ignore_index=True)

    return final_df

def run_labor_variance_report(session, week_start, week_end, dept=None, pos=None):

      import pandas as pd

      # -----------------------------
      # LOAD BASE DATA
      # -----------------------------
      emp_df           = pd.read_sql(session.query(db.Employee).statement, session.bind)
      schedule_df      = pd.read_sql(session.query(db.Schedule).statement, session.bind)
      actual_df        = pd.read_sql(session.query(db.Actual).statement, session.bind)
      room_actual_df   = pd.read_sql(session.query(db.RoomActual).statement, session.bind)
      std_df           = pd.read_sql(session.query(db.LaborStandard).statement, session.bind)
      pos_df_raw       = pd.read_sql(session.query(db.Position).statement, session.bind)
      dept_df_raw      = pd.read_sql(session.query(db.Department).statement, session.bind)

      pos_df = pos_df_raw.rename(
            columns={"id": "position_id", "name": "position", "department_id": "dept_id"}
      )

      dept_df = dept_df_raw.rename(
            columns={"id": "dept_id", "name": "department"}
      )

      pos_df = pos_df.merge(dept_df, on="dept_id", how="left")

      # -----------------------------
      # FILTER EMPLOYEES
      # -----------------------------
      emp_filtered = emp_df.copy()

      if dept and dept != "All":
            emp_filtered = emp_filtered[emp_filtered["department"] == dept]

      if pos and pos != "All":
            emp_filtered = emp_filtered[emp_filtered["role"] == pos]

      pos_names = emp_filtered["role"].dropna().unique()
      pos_match = pos_df[pos_df["position"].isin(pos_names)]

      if pos_match.empty:
            return pd.DataFrame()

      # -----------------------------
      # ACTUAL HOURS SUMMARY
      # -----------------------------
      ah_df = actual_df[
            (actual_df["source"].isin(["manual", "contract"])) &
            (actual_df["date"].between(week_start, week_end))
      ].copy()

      ah_df["total_hours"] = ah_df[["hours", "ot_hours"]].sum(axis=1)

      actual_summary = (
            ah_df.groupby("position_id")["total_hours"]
            .sum()
            .reset_index()
            .rename(columns={"total_hours": "actual_hours"})
      )

      actual_summary = actual_summary.merge(
            pos_df[["position_id", "position"]],
            on="position_id",
            how="left"
      )

      results = []
      seen_positions = set()

      # -----------------------------
      # MAIN POSITION LOOP
      # -----------------------------
      for _, row in pos_match.iterrows():

            pos_name   = row["position"]
            pos_id     = row["position_id"]
            dept_name  = row["department"]

            if pos_name in seen_positions and dept:
                  continue

            if dept:
                  seen_positions.add(pos_name)

            # -----------------------------
            # EMPLOYEE IDS ✅ FIXED FOR ALL vs NONE
            # -----------------------------
            if not dept or dept == "All":
                  emp_ids = emp_df[
                        (emp_df["role"] == pos_name) &
                        (emp_df["department"] == dept_name)
                  ]["id"].tolist()
                  dept_value = dept_name
            else:
                  emp_ids = emp_df[
                        (emp_df["department"] == dept) &
                        (emp_df["role"] == pos_name)
                  ]["id"].tolist()
                  dept_value = dept

            # -----------------------------
            # SCHEDULED HOURS
            # -----------------------------
            sched_rows = schedule_df[
                  (schedule_df["emp_id"].isin(emp_ids)) &
                  (schedule_df["day"] >= week_start) &
                  (schedule_df["day"] <= week_end)
            ]

            sched_hours = sched_rows["shift_type"].apply(
                  lambda x: 0 if str(x).strip().upper() == "OFF" else (
                        pd.to_datetime(str(x).split("-")[1]) -
                        pd.to_datetime(str(x).split("-")[0])
                  ).seconds / 3600
            ).sum()

            # -----------------------------
            # ACTUAL HOURS
            # -----------------------------
            actual_hours = actual_summary.loc[
                  actual_summary["position"] == pos_name, "actual_hours"
            ].sum()

            # -----------------------------
            # PROJECTED HOURS
            # -----------------------------
            std_pos = std_df[std_df["position_id"] == pos_id]
            proj_hours_total = 0

            for _, std_row in std_pos.iterrows():
                  metric   = std_row["metric"]
                  standard = std_row["standard"]

                  if not standard or standard == 0:
                        continue

                  actual_output = room_actual_df[
                        (room_actual_df["kpi"] == metric) &
                        (room_actual_df["date"] >= week_start) &
                        (room_actual_df["date"] <= week_end)
                  ]["value"].sum()

                  proj_hours_total += (actual_output / standard) * 8

            projected_hours = proj_hours_total
            variance        = actual_hours - projected_hours
            variance_pct    = (variance / projected_hours * 100) if projected_hours else 0

            results.append({
                  "Department": dept_value,
                  "Position": pos_name,
                  "Scheduled Hours": round(sched_hours, 1),
                  "Actual Hours": round(actual_hours, 1),
                  "Projected Hours": round(projected_hours, 1),
                  "Variance": round(variance, 1),
                  "Variance %": f"{'▲' if variance > 0 else '▼' if variance < 0 else ''} {abs(variance_pct):.2f}%" if projected_hours else "–"
            })

      report_df = pd.DataFrame(results)

      if report_df.empty:
            return report_df

      # -----------------------------
      # TOTAL ROW ✅ FIXED FOR NONE
      # -----------------------------
      total_row = {
            "Department": dept if dept else "All Departments",
            "Position": "TOTAL",
            "Scheduled Hours": round(report_df["Scheduled Hours"].sum(), 1),
            "Actual Hours": round(report_df["Actual Hours"].sum(), 1),
            "Projected Hours": round(report_df["Projected Hours"].sum(), 1),
            "Variance": round(report_df["Variance"].sum(), 1)
      }

      if total_row["Projected Hours"]:
            total_var_pct = (total_row["Variance"] / total_row["Projected Hours"]) * 100
            total_row["Variance %"] = f"{'▲' if total_var_pct > 0 else '▼' if total_var_pct < 0 else ''} {abs(total_var_pct):.2f}%"
      else:
            total_row["Variance %"] = "–"

      report_df.loc[len(report_df)] = total_row

      return report_df

# ============================================================
# Expose functions for scheduler import
# ============================================================
__all__ = [
    "run_ot_risk_report",
    "prepare_ot_risk_export",
    "run_forecast_variance_report",
    "prepare_forecast_variance_export",
    "run_productivity_index_report",
    "run_labor_variance_report",   # ✅ ADDED
]