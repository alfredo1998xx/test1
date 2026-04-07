# ============================================================
# scheduler.py — Background Scheduled Task Runner
# Runs OT Risk + Forecast Variance → Excel/PDF → emails results
# ============================================================

import os
import pandas as pd
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta, MO
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from db import ScheduledTask
from email_sender import send_email
from report_logic import (
    run_ot_risk_report,
    prepare_ot_risk_export,
    run_forecast_variance_report,
    prepare_forecast_variance_export,
    run_productivity_index_report,
    run_labor_variance_report      # ✅ ADDED
)

from scheduler_report_exports import (
    export_ot_risk_excel,
    export_ot_risk_pdf,
    export_forecast_variance_excel,
    export_forecast_variance_pdf,
    export_productivity_index_excel,
    export_productivity_index_pdf,
    export_labor_variance_excel,    # ✅ ADDED
    export_labor_variance_pdf       # ✅ ADDED
)

# ------------------------------------------------------------
# CONFIG — PATHS
# ------------------------------------------------------------
DB_PATH = r"C:\Users\jperez\OneDrive - Highgate\Desktop\Labor tool with stremlit and fast api v1\Labor tool with stremlit and fast api v1\labor tool to FastAPI\hotel_labor.db"
OUTPUT_DIR = r"C:\Users\jperez\LaborSchedulerTemp"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ------------------------------------------------------------
# DATABASE SESSION
# ------------------------------------------------------------
engine = create_engine(f"sqlite:///{DB_PATH}", echo=False)
Session = sessionmaker(bind=engine)
session = Session()


# ------------------------------------------------------------
# DATE-MODE → RANGE CALCULATIONS
# ------------------------------------------------------------
def get_date_ranges(date_mode):
    today = date.today()

    if date_mode == "Yesterday":
        start = today - timedelta(days=1)
        end   = start

    elif date_mode == "Current Week":
        start = today + relativedelta(weekday=MO(-1))
        end   = start + timedelta(days=6)

    elif date_mode == "Last Week":
        this_monday = today + relativedelta(weekday=MO(-1))
        start = this_monday - timedelta(days=7)
        end   = this_monday - timedelta(days=1)

    elif date_mode == "MTD":
        start = date(today.year, today.month, 1)
        end   = today

    else:
        start = today
        end   = today

    sched_start = start + relativedelta(weekday=MO(-1))
    sched_end   = sched_start + timedelta(days=6)

    return start, end, sched_start, sched_end


# ------------------------------------------------------------
# MAIN SCHEDULER LOOP
# ------------------------------------------------------------
def run_scheduled_jobs():

    print("\n======================================")
    print("Running scheduled jobs:", datetime.now())
    print("======================================\n")

    tasks = session.query(ScheduledTask).all()

    for t in tasks:
        print(f"Checking task {t.id}: {t.task_type} for hotel {t.hotel_name}")
        print("⏭ FORCE RUN FOR TESTING\n")

        # --------------------------------------------------------
        # Compute date ranges
        # --------------------------------------------------------
        week_start, week_end, sched_week_start, sched_week_end = get_date_ranges(t.date_mode)

        # --------------------------------------------------------
        # Shared metadata for all reports
        # --------------------------------------------------------
        metadata = {
            "hotel": t.hotel_name,
            "dept": t.department or "(All)",
            "pos": t.position or "(All)",
            "sched_range": f"{sched_week_start:%m-%d-%Y} to {sched_week_end:%m-%d-%Y}",
            "actual_range": f"{week_start:%m-%d-%Y} to {week_end:%m-%d-%Y}",
            "created_str": f"Created on {datetime.now():%m/%d/%Y %I:%M %p}"
        }

        # --------------------------------------------------------
        # =============== RUN THE CORRECT REPORT ===============
        # --------------------------------------------------------

        # =====================
        # OT RISK
        # =====================
        if t.task_type == "OT Risk":
            print("▶ Running OT Risk")

            df = run_ot_risk_report(
                session=session,
                week_start=week_start,
                week_end=week_end,
                sched_week_start=sched_week_start,
                sched_week_end=sched_week_end,
                dept=t.department,
                pos=t.position
            )

            if df is None or df.empty:
                print("⚠ No OT Risk data. Skipping.\n")
                continue

            export_df = prepare_ot_risk_export(df.copy())

            excel_path = os.path.join(
                OUTPUT_DIR, f"ot_risk_{t.id}_{datetime.now():%Y%m%d}.xlsx"
            )
            pdf_path = os.path.join(
                OUTPUT_DIR, f"ot_risk_{t.id}_{datetime.now():%Y%m%d}.pdf"
            )

            excel_file = export_ot_risk_excel(export_df.copy(), excel_path, metadata)
            pdf_file   = export_ot_risk_pdf(export_df.copy(), pdf_path, metadata)

            dept_label = t.department if t.department else "All Departments"

            subject = f"{t.hotel_name} | OT Risk Report | {dept_label} | {week_start:%m-%d-%Y} to {week_end:%m-%d-%Y}"
            body = (
                f"Hello,\n\n"
                f"Attached is the OT Risk Report for:\n"
                f"Hotel: {t.hotel_name}\n"
                f"Department: {dept_label}\n"
                f"Actuals: {week_start:%m-%d-%Y} → {week_end:%m-%d-%Y}\n"
                f"Schedule Week: {sched_week_start:%m-%d-%Y} → {sched_week_end:%m-%d-%Y}\n\n"
                f"Regards,\nLaborPilot Scheduler"
            )

        # =====================
        # FORECAST VARIANCE
        # =====================
        elif t.task_type == "Forecast Variance":
            print("▶ Running Forecast Variance")

            df = run_forecast_variance_report(
                session=session,
                week_start=week_start,
                week_end=week_end,
                dept=t.department
            )

            if df is None or df.empty:
                print("⚠ No Forecast Variance data. Skipping.\n")
                continue

            export_df = prepare_forecast_variance_export(df.copy())

            excel_path = os.path.join(
                OUTPUT_DIR, f"forecast_variance_{t.id}_{datetime.now():%Y%m%d}.xlsx"
            )
            pdf_path = os.path.join(
                OUTPUT_DIR, f"forecast_variance_{t.id}_{datetime.now():%Y%m%d}.pdf"
            )

            excel_file = export_forecast_variance_excel(export_df.copy(), excel_path, metadata)
            pdf_file   = export_forecast_variance_pdf(export_df.copy(), pdf_path, metadata)

            subject = f"Forecast Variance Report — {t.hotel_name}"
            body = (
                f"Hello,\n\n"
                f"Attached is the Forecast Variance Report for:\n"
                f"Week: {week_start:%m-%d-%Y} → {week_end:%m-%d-%Y}\n\n"
                f"Task ID: {t.id}\n"
                f"Hotel: {t.hotel_name}\n\n"
                f"Regards,\nLaborPilot Scheduler"
            )

        # --------------------------------------------------------
        # =============== RUN THE CORRECT REPORT ===============
        # --------------------------------------------------------

        # =====================
        # OT RISK
        # =====================
        if t.task_type == "OT Risk":
            print("▶ Running OT Risk")

            df = run_ot_risk_report(
                session=session,
                week_start=week_start,
                week_end=week_end,
                sched_week_start=sched_week_start,
                sched_week_end=sched_week_end,
                dept=t.department,
                pos=t.position
            )

            if df is None or df.empty:
                print("⚠ No OT Risk data. Skipping.\n")
                continue

            export_df = prepare_ot_risk_export(df.copy())

            excel_path = os.path.join(
                OUTPUT_DIR, f"ot_risk_{t.id}_{datetime.now():%Y%m%d}.xlsx"
            )
            pdf_path = os.path.join(
                OUTPUT_DIR, f"ot_risk_{t.id}_{datetime.now():%Y%m%d}.pdf"
            )

            excel_file = export_ot_risk_excel(export_df.copy(), excel_path, metadata)
            pdf_file   = export_ot_risk_pdf(export_df.copy(), pdf_path, metadata)

            dept_label = t.department if t.department else "All Departments"

            subject = f"{t.hotel_name} → OT Risk Report | {dept_label} | {week_start:%m-%d-%Y} to {week_end:%m-%d-%Y}"
            body = (
                f"Hello,\n\n"
                f"Attached is the OT Risk Report for:\n"
                f"Hotel: {t.hotel_name}\n"
                f"Department: {dept_label}\n"
                f"Actuals: {week_start:%m-%d-%Y} → {week_end:%m-%d-%Y}\n"
                f"Schedule Week: {sched_week_start:%m-%d-%Y} → {sched_week_end:%m-%d-%Y}\n\n"
                f"Regards,\nLaborPilot Scheduler"
            )


        # =====================
        # FORECAST VARIANCE
        # =====================
        elif t.task_type == "Forecast Variance":
            print("▶ Running Forecast Variance")

            df = run_forecast_variance_report(
                session=session,
                week_start=week_start,
                week_end=week_end,
                dept=t.department
            )

            if df is None or df.empty:
                print("⚠ No Forecast Variance data. Skipping.\n")
                continue

            export_df = prepare_forecast_variance_export(df.copy())

            excel_path = os.path.join(
                OUTPUT_DIR, f"forecast_variance_{t.id}_{datetime.now():%Y%m%d}.xlsx"
            )
            pdf_path = os.path.join(
                OUTPUT_DIR, f"forecast_variance_{t.id}_{datetime.now():%Y%m%d}.pdf"
            )

            excel_file = export_forecast_variance_excel(export_df.copy(), excel_path, metadata)
            pdf_file   = export_forecast_variance_pdf(export_df.copy(), pdf_path, metadata)

            dept_label = t.department if t.department else "All Departments"

            subject = f"{t.hotel_name} → Forecast Variance Report | {dept_label} | {week_start:%m-%d-%Y} to {week_end:%m-%d-%Y}"
            body = (
                f"Hello,\n\n"
                f"Attached is the Forecast Variance Report for:\n"
                f"Hotel: {t.hotel_name}\n"
                f"Department: {dept_label}\n"
                f"Week: {week_start:%m-%d-%Y} → {week_end:%m-%d-%Y}\n\n"
                f"Regards,\nLaborPilot Scheduler"
            )


        # =====================
        # PRODUCTIVITY INDEX
        # =====================
        elif t.task_type == "Productivity Index":

            print("▶ Running Productivity Index")

            week_start, week_end, _, _ = get_date_ranges(t.date_mode)

            df = run_productivity_index_report(
                session=session,
                week_start=week_start,
                week_end=week_end,
                dept=t.department,
                pos=t.position
            )

            if df is None or df.empty:
                print("⚠ No Productivity Index data. Skipping.\n")
                continue

            metadata = {
                "hotel": t.hotel_name,
                "dept": t.department or "(All)",
                "pos": t.position or "(All)",
                "period": f"{week_start:%m-%d-%Y} to {week_end:%m-%d-%Y}",
                "created_str": f"Created on {datetime.now():%m/%d/%Y %I:%M %p}"
            }

            excel_path = os.path.join(
                OUTPUT_DIR,
                f"productivity_{t.id}_{datetime.now():%Y%m%d}.xlsx"
            )

            pdf_path = os.path.join(
                OUTPUT_DIR,
                f"productivity_{t.id}_{datetime.now():%Y%m%d}.pdf"
            )

            excel_file = export_productivity_index_excel(df.copy(), excel_path, metadata)
            pdf_file   = export_productivity_index_pdf(df.copy(), pdf_path, metadata)

            dept_label = t.department if t.department else "All Departments"
            pos_label  = t.position if t.position else "All Positions"

            subject = f"{t.hotel_name} → Productivity Index Report | {dept_label} | {week_start:%m-%d-%Y} to {week_end:%m-%d-%Y}"
            body = (
                f"Hello,\n\n"
                f"Attached is the Productivity Index Report for:\n"
                f"Hotel: {t.hotel_name}\n"
                f"Department: {dept_label}\n"
                f"Position: {pos_label}\n"
                f"Period: {week_start:%m-%d-%Y} → {week_end:%m-%d-%Y}\n\n"
                f"Regards,\nLaborPilot Scheduler"
            )

        # =====================
        # LABOR VARIANCE
        # =====================
        elif t.task_type == "Labor Variance":

            print("▶ Running Labor Variance")

            df = run_labor_variance_report(
                session=session,
                week_start=week_start,
                week_end=week_end,
                dept=t.department,
                pos=t.position
            )

            if df is None or df.empty:
                print("⚠ No Labor Variance data. Skipping.\n")
                continue

            metadata = {
                "hotel": t.hotel_name,
                "dept": t.department or "(All)",
                "pos": t.position or "(All)",
                "period": f"{week_start:%m-%d-%Y} to {week_end:%m-%d-%Y}",
                "created_str": f"Created on {datetime.now():%m/%d/%Y %I:%M %p}"
            }

            excel_path = os.path.join(
                OUTPUT_DIR,
                f"labor_variance_{t.id}_{datetime.now():%Y%m%d}.xlsx"
            )

            pdf_path = os.path.join(
                OUTPUT_DIR,
                f"labor_variance_{t.id}_{datetime.now():%Y%m%d}.pdf"
            )

            excel_file = export_labor_variance_excel(df.copy(), excel_path, metadata)
            pdf_file   = export_labor_variance_pdf(df.copy(), pdf_path, metadata)

            recipients = [e.strip() for e in t.emails.split(",") if e.strip()]

            dept_label = t.department if t.department else "All Departments"

            subject = f"{t.hotel_name} → Labor Variance Report | {dept_label} | {week_start:%m-%d-%Y} to {week_end:%m-%d-%Y}"
            body = (
                f"Hello,\n\n"
                f"Attached is the Labor Variance Report for:\n"
                f"Hotel: {t.hotel_name}\n"
                f"Department: {dept_label}\n"
                f"Week: {week_start:%m-%d-%Y} → {week_end:%m-%d-%Y}\n\n"
                f"Regards,\nLaborPilot Scheduler"
            )


        else:
            print(f"⚠ Unsupported task type: {t.task_type}\n")
            continue

        # --------------------------------------------------------
        # SEND EMAIL
        # --------------------------------------------------------
        recipients = [e.strip() for e in t.emails.split(",") if e.strip()]

        send_email(
            recipients=recipients,
            subject=subject,
            body=body,
            attachments=[excel_file, pdf_file]
        )

        print("✅ Task completed!\n")

    print("All scheduled tasks processed.\n")


# ------------------------------------------------------------
# RUN SCHEDULER
# ------------------------------------------------------------
if __name__ == "__main__":
    run_scheduled_jobs()