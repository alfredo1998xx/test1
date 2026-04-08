import os
import tempfile
import pandas as pd
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta, MO
from sqlalchemy.orm import sessionmaker
from database import engine as ENGINE
from db import ScheduledTask
from email_sender import send_email
from report_logic import (
    run_ot_risk_report,
    prepare_ot_risk_export,
    run_forecast_variance_report,
    prepare_forecast_variance_export,
    run_productivity_index_report,
    run_labor_variance_report,
)
from scheduler_report_exports import (
    export_ot_risk_excel,
    export_ot_risk_pdf,
    export_forecast_variance_excel,
    export_forecast_variance_pdf,
    export_productivity_index_excel,
    export_productivity_index_pdf,
    export_labor_variance_excel,
    export_labor_variance_pdf,
)

OUTPUT_DIR = os.path.join(tempfile.gettempdir(), "labor_scheduler")
os.makedirs(OUTPUT_DIR, exist_ok=True)

Session = sessionmaker(bind=ENGINE)


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


def _is_task_due(task) -> bool:
    """Return True if this task should fire right now based on run_time + frequency."""
    now = datetime.now()
    try:
        hour, minute = map(int, task.run_time.split(":"))
    except Exception:
        return False

    if now.hour != hour or now.minute != minute:
        return False

    freq = (task.frequency or "Daily").strip().lower()
    if freq == "daily":
        return True

    created = task.created_at or now
    if freq == "weekly":
        return now.weekday() == created.weekday()

    if freq == "bi-weekly":
        weeks_since = (now.date() - created.date()).days // 7
        return now.weekday() == created.weekday() and weeks_since % 2 == 0

    return True


def run_single_task(task, session=None):
    """
    Run one ScheduledTask immediately — used by both the cron loop and Send Now.
    Pass an existing session or leave None to create a fresh one.
    """
    close_session = False
    if session is None:
        session = Session()
        close_session = True

    try:
        week_start, week_end, sched_week_start, sched_week_end = get_date_ranges(task.date_mode)

        now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        stamp   = f"{task.id}_{now_str}"

        metadata_base = {
            "hotel":       task.hotel_name,
            "dept":        task.department or "(All)",
            "pos":         task.position   or "(All)",
            "sched_range": f"{sched_week_start:%m-%d-%Y} to {sched_week_end:%m-%d-%Y}",
            "actual_range":f"{week_start:%m-%d-%Y} to {week_end:%m-%d-%Y}",
            "created_str": f"Created on {datetime.now():%m/%d/%Y %I:%M %p}",
        }

        dept_label = task.department or "All Departments"
        pos_label  = task.position   or "All Positions"
        date_range = f"{week_start:%m-%d-%Y} to {week_end:%m-%d-%Y}"

        excel_file = None
        pdf_file   = None
        subject    = ""
        body       = ""

        if task.task_type == "OT Risk":
            df = run_ot_risk_report(
                session=session,
                week_start=week_start, week_end=week_end,
                sched_week_start=sched_week_start, sched_week_end=sched_week_end,
                dept=task.department, pos=task.position,
            )
            if df is None or df.empty:
                return False, "No OT Risk data available for the selected period."

            export_df  = prepare_ot_risk_export(df.copy())
            excel_path = os.path.join(OUTPUT_DIR, f"ot_risk_{stamp}.xlsx")
            pdf_path   = os.path.join(OUTPUT_DIR, f"ot_risk_{stamp}.pdf")
            excel_file = export_ot_risk_excel(export_df.copy(), excel_path, metadata_base)
            pdf_file   = export_ot_risk_pdf(export_df.copy(), pdf_path, metadata_base)

            subject = f"{task.hotel_name} | OT Risk Report | {dept_label} | {date_range}"
            body    = (
                f"Hello,\n\nAttached is the OT Risk Report for:\n"
                f"Hotel: {task.hotel_name}\nDepartment: {dept_label}\n"
                f"Actuals: {date_range}\n"
                f"Schedule Week: {sched_week_start:%m-%d-%Y} → {sched_week_end:%m-%d-%Y}\n\n"
                f"Regards,\nLaborPilot"
            )

        elif task.task_type == "Forecast Variance":
            df = run_forecast_variance_report(
                session=session,
                week_start=week_start, week_end=week_end,
                dept=task.department,
            )
            if df is None or df.empty:
                return False, "No Forecast Variance data available for the selected period."

            export_df  = prepare_forecast_variance_export(df.copy())
            excel_path = os.path.join(OUTPUT_DIR, f"forecast_variance_{stamp}.xlsx")
            pdf_path   = os.path.join(OUTPUT_DIR, f"forecast_variance_{stamp}.pdf")
            excel_file = export_forecast_variance_excel(export_df.copy(), excel_path, metadata_base)
            pdf_file   = export_forecast_variance_pdf(export_df.copy(), pdf_path, metadata_base)

            subject = f"{task.hotel_name} | Forecast Variance Report | {dept_label} | {date_range}"
            body    = (
                f"Hello,\n\nAttached is the Forecast Variance Report for:\n"
                f"Hotel: {task.hotel_name}\nDepartment: {dept_label}\n"
                f"Week: {date_range}\n\nRegards,\nLaborPilot"
            )

        elif task.task_type == "Productivity Index":
            df = run_productivity_index_report(
                session=session,
                week_start=week_start, week_end=week_end,
                dept=task.department, pos=task.position,
            )
            if df is None or df.empty:
                return False, "No Productivity Index data available for the selected period."

            metadata_pi = {**metadata_base, "period": date_range}
            excel_path  = os.path.join(OUTPUT_DIR, f"productivity_{stamp}.xlsx")
            pdf_path    = os.path.join(OUTPUT_DIR, f"productivity_{stamp}.pdf")
            excel_file  = export_productivity_index_excel(df.copy(), excel_path, metadata_pi)
            pdf_file    = export_productivity_index_pdf(df.copy(), pdf_path, metadata_pi)

            subject = f"{task.hotel_name} | Productivity Index Report | {dept_label} | {date_range}"
            body    = (
                f"Hello,\n\nAttached is the Productivity Index Report for:\n"
                f"Hotel: {task.hotel_name}\nDepartment: {dept_label}\n"
                f"Position: {pos_label}\nPeriod: {date_range}\n\nRegards,\nLaborPilot"
            )

        elif task.task_type == "Labor Variance":
            df = run_labor_variance_report(
                session=session,
                week_start=week_start, week_end=week_end,
                dept=task.department, pos=task.position,
            )
            if df is None or df.empty:
                return False, "No Labor Variance data available for the selected period."

            metadata_lv = {**metadata_base, "period": date_range}
            excel_path  = os.path.join(OUTPUT_DIR, f"labor_variance_{stamp}.xlsx")
            pdf_path    = os.path.join(OUTPUT_DIR, f"labor_variance_{stamp}.pdf")
            excel_file  = export_labor_variance_excel(df.copy(), excel_path, metadata_lv)
            pdf_file    = export_labor_variance_pdf(df.copy(), pdf_path, metadata_lv)

            subject = f"{task.hotel_name} | Labor Variance Report | {dept_label} | {date_range}"
            body    = (
                f"Hello,\n\nAttached is the Labor Variance Report for:\n"
                f"Hotel: {task.hotel_name}\nDepartment: {dept_label}\n"
                f"Week: {date_range}\n\nRegards,\nLaborPilot"
            )

        else:
            return False, f"Unsupported task type: {task.task_type}"

        recipients = [e.strip() for e in task.emails.split(",") if e.strip()]
        if not recipients:
            return False, "No valid email recipients configured."

        send_email(
            recipients=recipients,
            subject=subject,
            body=body,
            attachments=[f for f in [excel_file, pdf_file] if f],
        )
        return True, f"Report sent to {', '.join(recipients)}"

    except Exception as e:
        import traceback
        traceback.print_exc()
        return False, str(e)
    finally:
        if close_session:
            session.close()


def run_scheduled_jobs():
    """Check all tasks and run those that are due right now."""
    print(f"\n[Scheduler] Checking at {datetime.now():%Y-%m-%d %H:%M}")
    session = Session()
    try:
        tasks = session.query(ScheduledTask).all()
        for task in tasks:
            if _is_task_due(task):
                print(f"[Scheduler] Running task {task.id}: {task.task_type} for {task.hotel_name}")
                ok, msg = run_single_task(task, session=session)
                print(f"[Scheduler] {'✅' if ok else '❌'} {msg}")
            else:
                print(f"[Scheduler] Skipping task {task.id} (not due yet)")
    finally:
        session.close()


if __name__ == "__main__":
    run_scheduled_jobs()
