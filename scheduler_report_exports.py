# =============================================================
# scheduler_report_exports.py — Excel & PDF Export Functions
# Reproduces the EXACT same formatting used inside Streamlit UI
# =============================================================

import pandas as pd
import io
from datetime import datetime
from reportlab.platypus import (
    SimpleDocTemplate, Table, TableStyle,
    Paragraph, Spacer
)
from reportlab.lib.pagesizes import landscape, letter
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors



# -------------------------------------------------------------
# EXCEL EXPORT  — identical to UI formatting
# -------------------------------------------------------------
def export_ot_risk_excel(df, file_path, metadata):
    with pd.ExcelWriter(file_path, engine="xlsxwriter") as writer:
        df.to_excel(
            writer,
            sheet_name="OT Risk Report",
            startrow=7,
            startcol=1,
            index=False,
            header=False
        )

        workbook = writer.book
        worksheet = writer.sheets["OT Risk Report"]

        # ===== Solid background =====
        solid_bg_fmt = workbook.add_format({'bg_color': '#FFFFFF', 'pattern': 1})
        worksheet.set_column(0, 51, None, solid_bg_fmt)
        for r in range(0, 2000):
            worksheet.set_row(r, None, solid_bg_fmt)

        worksheet.set_column(0, 0, 2)   # gutter

        # ===== TOP METADATA =====
        title_fmt = workbook.add_format({'bold': True, 'font_size': 14})
        worksheet.write("B1", "OT Risk Report", title_fmt)

        # Borderless formats
        label_fmt = workbook.add_format({'bold': True, 'border': 0, 'bg_color': '#FFFFFF'})
        value_fmt = workbook.add_format({'border': 0, 'bg_color': '#FFFFFF'})

        # Top-right text
        hotel_fmt = workbook.add_format({
            'bold': True, 'align': 'right', 'border': 0, 'bg_color': '#FFFFFF'
        })
        created_fmt = workbook.add_format({
            'align': 'right', 'border': 0, 'bg_color': '#FFFFFF'
        })

        # Rightmost column → right align
        right_col = 1 + len(df.columns) - 1
        worksheet.write(0, right_col, metadata["hotel"], hotel_fmt)
        worksheet.write(1, right_col, metadata["created_str"], created_fmt)

        # LEFT metadata block
        worksheet.write("B3", "Department:", label_fmt)
        worksheet.write("C3", metadata["dept"], value_fmt)

        worksheet.write("B4", "Position:", label_fmt)
        worksheet.write("C4", metadata["pos"], value_fmt)

        worksheet.write("B5", "Week (Schedule):", label_fmt)
        worksheet.write("C5", metadata["sched_range"], value_fmt)

        worksheet.write("B6", "Actuals Range:", label_fmt)
        worksheet.write("C6", metadata["actual_range"], value_fmt)

        # ===== TABLE FORMATS =====
        border_fmt = {'border': 1, 'border_color': '#A6A6A6'}

        header_fmt = workbook.add_format({
            **border_fmt,
            'bold': True,
            'bg_color': '#16365C',
            'font_color': '#FFFFFF',
            'align': 'center',
            'pattern': 1
        })

        default_fmt = workbook.add_format({**border_fmt, 'align': 'center'})
        first3_fmt = workbook.add_format({**border_fmt, 'bold': True, 'align': 'center'})
        last2_fmt = workbook.add_format({**border_fmt, 'bg_color': '#FEF6F0', 'align': 'center', 'pattern': 1})

        green_fmt = workbook.add_format({**border_fmt, 'font_color': '#008000', 'align': 'center'})
        red_fmt = workbook.add_format({**border_fmt, 'font_color': '#FF0000', 'align': 'center'})

        dollar_norm = workbook.add_format({
            **border_fmt,
            'num_format': '$#,##0.00',
            'align': 'center'
        })
        dollar_red = workbook.add_format({
            **border_fmt,
            'num_format': '$#,##0.00',
            'font_color': '#FF0000',
            'align': 'center'
        })

        # ===== HEADER ROW =====
        header_row = 7
        for col_num, col_name in enumerate(df.columns):
            worksheet.write(header_row, 1 + col_num, col_name, header_fmt)

        # ===== DATA ROWS =====
        data_start_row = 8
        for r_idx, row in df.iterrows():
            for c_idx, col_name in enumerate(df.columns):
                val = row[col_name]

                if c_idx <= 2:
                    fmt = first3_fmt
                elif c_idx >= len(df.columns) - 2:
                    fmt = last2_fmt
                elif col_name == "OT Risk" and val == "OT":
                    fmt = red_fmt
                elif col_name == "OT Risk %" and val == "100%":
                    fmt = red_fmt
                elif col_name == "OT Risk %" and val == "0%":
                    fmt = green_fmt
                elif col_name == "OT Cost":
                    try:
                        fmt = dollar_red if float(val) > 0 else dollar_norm
                    except:
                        fmt = default_fmt
                else:
                    fmt = default_fmt

                worksheet.write(data_start_row + r_idx, 1 + c_idx, val, fmt)

        # ===== AUTO-EXPAND ALL COLUMNS =====
        for c_idx, col_name in enumerate(df.columns):
            series_as_str = df[col_name].astype(str)
            longest = max(series_as_str.map(len).max(), len(col_name)) + 4  # padding
            worksheet.set_column(1 + c_idx, 1 + c_idx, longest)

        # ===== SUMMARY BLOCK =====
        sum_col = 1 + (len(df.columns) - 2)

        summary_header = workbook.add_format({
            'bold': True,
            'bg_color': '#16365C',
            'font_color': '#FFFFFF',
            'border': 1,
            'align': 'center',
            'pattern': 1,
            'border_color': '#A6A6A6'
        })

        worksheet.merge_range(3, sum_col, 3, sum_col + 1, "Total", summary_header)
        worksheet.write(4, sum_col, "Projected OT", default_fmt)
        worksheet.write(4, sum_col + 1, df["Projected OT"].sum(), dollar_norm)
        worksheet.write(5, sum_col, "OT Cost", default_fmt)
        worksheet.write(5, sum_col + 1, df["OT Cost"].sum(), dollar_norm)

        # ==== FOOTER ====
        footer_row = data_start_row + len(df) + 2
        worksheet.write(
            footer_row,
            1,
            "Confidential | 2025 Labor Pilot",
            workbook.add_format({'align': 'left'})
        )

    return file_path


# -------------------------------------------------------------
# PDF EXPORT — identical to UI formatting (with header/footer)
# -------------------------------------------------------------
def export_ot_risk_pdf(df, file_path, metadata):
    styles = getSampleStyleSheet()
    elements = []

    # ---------------------------------------------------------
    # DOC SETUP
    # ---------------------------------------------------------
    doc = SimpleDocTemplate(
        file_path,
        pagesize=landscape(letter),
        leftMargin=28,
        rightMargin=28,
        topMargin=60,     # More room for header
        bottomMargin=40   # More room for footer
    )

    # ---------------------------------------------------------
    # TITLE
    # ---------------------------------------------------------
    elements.append(Paragraph("<b>OT Risk Report</b>", styles["Heading2"]))
    elements.append(Spacer(1, 8))

    # ---------------------------------------------------------
    # LEFT METADATA BLOCK
    # ---------------------------------------------------------
    left_meta = [
        Paragraph(f"<b>Department:</b> {metadata['dept']}", styles["Normal"]),
        Paragraph(f"<b>Position:</b> {metadata['pos']}", styles["Normal"]),
        Paragraph(f"<b>Week (Schedule):</b> {metadata['sched_range']}", styles["Normal"]),
        Paragraph(f"<b>Actuals Range:</b> {metadata['actual_range']}", styles["Normal"]),
    ]

    # ---------------------------------------------------------
    # SUMMARY BLOCK (RIGHT)
    # ---------------------------------------------------------
    summary_data = [
        ["Total", ""],
        ["Projected OT", f"{df['Projected OT'].sum():.2f}"],
        ["OT Cost", f"${df['OT Cost'].sum():,.2f}"]
    ]

    summary_table = Table(summary_data, colWidths=[100, 80])
    summary_table.setStyle(TableStyle([
        ("SPAN", (0, 0), (1, 0)),
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#16365C")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#A6A6A6")),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))

    # ---------------------------------------------------------
    # TWO COLUMN LAYOUT
    # ---------------------------------------------------------
    layout = Table([[left_meta, summary_table]], colWidths=[None, 180])
    layout.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
    ]))

    elements.append(layout)
    elements.append(Spacer(1, 16))

    # ---------------------------------------------------------
    # MAIN DATA TABLE
    # ---------------------------------------------------------
    pdf_data = [df.columns.tolist()] + df.values.tolist()

    table = Table(pdf_data)
    table_style = TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#16365C")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#A6A6A6")),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ])

    # Shade last 2 columns (#FEF6F0)
    last2_start = len(df.columns) - 2
    table_style.add(
        "BACKGROUND",
        (last2_start, 1),
        (len(df.columns) - 1, len(pdf_data) - 1),
        colors.HexColor("#FEF6F0")
    )

    table.setStyle(table_style)
    elements.append(table)

    # ---------------------------------------------------------
    # HEADER + FOOTER DRAW FUNCTIONS
    # ---------------------------------------------------------
    def draw_header(canvas, doc):
        canvas.saveState()

        # Hotel name
        canvas.setFont("Helvetica-Bold", 10)
        canvas.drawRightString(
            doc.pagesize[0] - doc.rightMargin,
            doc.pagesize[1] - 20,
            metadata["hotel"]
        )

        # Created line
        canvas.setFont("Helvetica", 8)
        canvas.drawRightString(
            doc.pagesize[0] - doc.rightMargin,
            doc.pagesize[1] - 34,
            metadata["created_str"]
        )

        canvas.restoreState()

    def draw_footer(canvas, doc):
        canvas.saveState()

        # Footer left text
        canvas.setFont("Helvetica", 8)
        canvas.drawString(
            doc.leftMargin,
            doc.bottomMargin - 20,
            "Confidential | 2025 Labor Pilot"
        )

        # Page number right
        canvas.drawRightString(
            doc.pagesize[0] - doc.rightMargin,
            doc.bottomMargin - 20,
            f"Page | {canvas.getPageNumber()}"
        )

        canvas.restoreState()

    # ---------------------------------------------------------
    # BUILD DOCUMENT WITH HEADER/FOOTER
    # ---------------------------------------------------------
    doc.build(
        elements,
        onFirstPage=lambda c, d: (draw_header(c, d), draw_footer(c, d)),
        onLaterPages=lambda c, d: (draw_header(c, d), draw_footer(c, d))
    )

    return file_path
# -------------------------------------------------------------
# EXCEL EXPORT — Forecast Variance (matches UI formatting)
# -------------------------------------------------------------
def export_forecast_variance_excel(df, file_path, metadata):
    with pd.ExcelWriter(file_path, engine="xlsxwriter") as writer:

        df.to_excel(
            writer,
            sheet_name="Forecast Variance",
            startrow=6,
            startcol=1,
            index=False,
            header=False
        )

        workbook = writer.book
        ws = writer.sheets["Forecast Variance"]

        # ===== WHITE BACKGROUND =====
        solid_bg_fmt = workbook.add_format({'bg_color': '#FFFFFF', 'pattern': 1})
        ws.set_column(0, 51, None, solid_bg_fmt)
        for r in range(0, 2000):
            ws.set_row(r, None, solid_bg_fmt)

        ws.set_column(0, 0, 2)  # gutter

        # ===== METADATA TOP ROWS =====
        title_fmt = workbook.add_format({'bold': True, 'font_size': 14})
        ws.write("B1", "Forecast Variance Report", title_fmt)

        label_fmt = workbook.add_format({'bold': True, 'border': 0, 'bg_color': '#FFFFFF'})
        value_fmt = workbook.add_format({'border': 0, 'bg_color': '#FFFFFF'})

        # RIGHT metadata
        hotel_fmt = workbook.add_format({'bold': True, 'align': 'right', 'border': 0, 'bg_color': '#FFFFFF'})
        created_fmt = workbook.add_format({'align': 'right', 'border': 0, 'bg_color': '#FFFFFF'})

        right_col = 1 + len(df.columns) - 1
        ws.write(0, right_col, metadata["hotel"], hotel_fmt)
        ws.write(1, right_col, metadata["created_str"], created_fmt)

        # LEFT metadata
        ws.write("B3", "Week:", label_fmt)
        ws.write("C3", metadata["actual_range"], value_fmt)

        if metadata.get("dept"):
            ws.write("B4", "Department:", label_fmt)
            ws.write("C4", metadata["dept"], value_fmt)

        # ===== TABLE FORMATS =====
        border_fmt = {"border": 1, "border_color": "#A6A6A6"}

        header_fmt = workbook.add_format({
            **border_fmt,
            "bold": True,
            "bg_color": "#16365C",
            "font_color": "#FFFFFF",
            "align": "center",
            "pattern": 1
        })

        kpi_fmt = workbook.add_format({
            **border_fmt, "bold": True, "align": "center"
        })

        num_fmt = workbook.add_format({
            **border_fmt,
            "num_format": "#,##0",
            "align": "center"
        })

        variance_fmt = workbook.add_format({
            **border_fmt,
            "bg_color": "#FEF6F0",
            "num_format": "#,##0",
            "align": "center",
            "pattern": 1
        })

        # ===== HEADER ROW =====
        header_row = 6
        for c_idx, col in enumerate(df.columns):
            ws.write(header_row, 1 + c_idx, col, header_fmt)

        # ===== DATA ROWS =====
        data_row = 7
        for r_idx, row in df.iterrows():
            for c_idx, col in enumerate(df.columns):
                val = row[col]

                if col == "KPI":
                    fmt = kpi_fmt
                elif "Δ" in col:
                    fmt = variance_fmt
                else:
                    fmt = num_fmt

                ws.write(data_row + r_idx, 1 + c_idx, val, fmt)

        # ===== AUTO WIDTHS =====
        for c_idx, col in enumerate(df.columns):
            max_len = max(len(str(col)), df[col].astype(str).str.len().max(), 12) + 2
            ws.set_column(1 + c_idx, 1 + c_idx, max_len)

        # ===== FOOTER =====
        footer_row = data_row + len(df) + 2
        ws.write(
            footer_row,
            1,
            "Confidential | 2025 Labor Pilot",
            workbook.add_format({'align': 'left'})
        )

    return file_path
# -------------------------------------------------------------
# PDF EXPORT — Forecast Variance (identical to OT Risk format)
# -------------------------------------------------------------
def export_forecast_variance_pdf(df, file_path, metadata):

    styles = getSampleStyleSheet()
    elements = []

    # ---------------------------------------------------------
    # DOC SETUP  ✅ MATCHES OT RISK
    # ---------------------------------------------------------
    doc = SimpleDocTemplate(
        file_path,
        pagesize=landscape(letter),
        leftMargin=28,
        rightMargin=28,
        topMargin=60,     # ✅ SAME AS OT RISK
        bottomMargin=40
    )

    # ---------------------------------------------------------
    # TITLE  ✅ SAME PLACEMENT AS OT RISK
    # ---------------------------------------------------------
    elements.append(Paragraph("<b>Forecast Variance Report</b>", styles["Heading2"]))
    elements.append(Spacer(1, 8))

    # ---------------------------------------------------------
    # LEFT METADATA BLOCK  ✅ MATCH STRUCTURE
    # ---------------------------------------------------------
    left_meta = [
        Paragraph(f"<b>Week:</b> {metadata['actual_range']}", styles["Normal"]),
        Paragraph(f"<b>Department:</b> {metadata.get('dept', '(All)')}", styles["Normal"]),
    ]

    meta_table = Table([[left_meta]], colWidths=[None])
    meta_table.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
    ]))

    elements.append(meta_table)
    elements.append(Spacer(1, 16))

    # ---------------------------------------------------------
    # MAIN DATA TABLE
    # ---------------------------------------------------------
    pdf_data = [df.columns.tolist()] + df.values.tolist()

    table = Table(pdf_data)
    table_style = TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#16365C")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#A6A6A6")),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ])

    # ---------------------------------------------------------
    # SHADE VARIANCE COLUMNS (#FEF6F0)
    # ---------------------------------------------------------
    for c_idx, col in enumerate(df.columns):
        if "Δ" in col:
            table_style.add(
                "BACKGROUND",
                (c_idx, 1),
                (c_idx, len(pdf_data) - 1),
                colors.HexColor("#FEF6F0")
            )

    table.setStyle(table_style)
    elements.append(table)

    # ---------------------------------------------------------
    # HEADER + FOOTER DRAW FUNCTIONS ✅ MATCH OT RISK
    # ---------------------------------------------------------
    def draw_header(canvas, doc):
        canvas.saveState()

        # Hotel name (TOP RIGHT)
        canvas.setFont("Helvetica-Bold", 10)
        canvas.drawRightString(
            doc.pagesize[0] - doc.rightMargin,
            doc.pagesize[1] - 20,
            metadata["hotel"]
        )

        # Created line (UNDER HOTEL)
        canvas.setFont("Helvetica", 8)
        canvas.drawRightString(
            doc.pagesize[0] - doc.rightMargin,
            doc.pagesize[1] - 34,
            metadata["created_str"]
        )

        canvas.restoreState()

    def draw_footer(canvas, doc):
        canvas.saveState()

        # Footer left
        canvas.setFont("Helvetica", 8)
        canvas.drawString(
            doc.leftMargin,
            doc.bottomMargin - 20,
            "Confidential | 2025 Labor Pilot"
        )

        # Page number right
        canvas.drawRightString(
            doc.pagesize[0] - doc.rightMargin,
            doc.bottomMargin - 20,
            f"Page | {canvas.getPageNumber()}"
        )

        canvas.restoreState()

    # ---------------------------------------------------------
    # BUILD DOCUMENT ✅ MATCH OT RISK
    # ---------------------------------------------------------
    doc.build(
        elements,
        onFirstPage=lambda c, d: (draw_header(c, d), draw_footer(c, d)),
        onLaterPages=lambda c, d: (draw_header(c, d), draw_footer(c, d))
    )

    return file_path
# -------------------------------------------------------------
# EXCEL EXPORT — Productivity Index (Scheduler)
# -------------------------------------------------------------
def export_productivity_index_excel(df, file_path, metadata):

    with pd.ExcelWriter(file_path, engine="xlsxwriter") as writer:
        df.to_excel(
            writer,
            sheet_name="Productivity Index",
            startrow=6,
            startcol=1,
            index=False,
            header=False
        )

        workbook = writer.book
        ws = writer.sheets["Productivity Index"]

        # ===== Background =====
        solid_bg_fmt = workbook.add_format({"bg_color": "#FFFFFF", "pattern": 1})
        ws.set_column(0, 51, None, solid_bg_fmt)
        for r in range(0, 2000):
            ws.set_row(r, None, solid_bg_fmt)
        ws.set_column(0, 0, 2)

        # ===== Title =====
        title_fmt = workbook.add_format({"bold": True, "font_size": 14})
        ws.write("B1", "Productivity Index Report", title_fmt)

        # ===== RIGHT metadata =====
        hotel_fmt   = workbook.add_format({'bold': True, 'align': 'right', 'border': 0, 'bg_color': '#FFFFFF'})
        created_fmt = workbook.add_format({'align': 'right', 'border': 0, 'bg_color': '#FFFFFF'})

        right_col = 1 + len(df.columns) - 1
        ws.write(0, right_col, metadata["hotel"], hotel_fmt)
        ws.write(1, right_col, metadata["created_str"], created_fmt)

        # ===== LEFT metadata =====
        label_fmt = workbook.add_format({'bold': True, 'border': 0, 'bg_color': '#FFFFFF'})
        value_fmt = workbook.add_format({'border': 0, 'bg_color': '#FFFFFF'})

        ws.write("B3", "Department:", label_fmt)
        ws.write("C3", metadata["dept"], value_fmt)

        ws.write("B4", "Position:", label_fmt)
        ws.write("C4", metadata["pos"], value_fmt)

        ws.write("B5", "Period:", label_fmt)
        ws.write("C5", metadata["period"], value_fmt)

        # ===== Table formats =====
        border_fmt = {"border": 1, "border_color": "#A6A6A6"}

        header_fmt = workbook.add_format({
            **border_fmt,
            "bold": True,
            "font_color": "white",
            "bg_color": "#16365C",
            "align": "center",
            "pattern": 1
        })

        data_fmt = workbook.add_format({
            **border_fmt,
            "align": "center"
        })

        variance_fmt = workbook.add_format({
            **border_fmt,
            "align": "center",
            "bg_color": "#FEF6F0",
            "pattern": 1
        })

        total_fmt = workbook.add_format({
            **border_fmt,
            "bg_color": "#DCE6F1",
            "bold": True,
            "align": "center",
            "pattern": 1
        })

        # ===== Header Row =====
        header_row = 6
        header_col = 1
        for i, col in enumerate(df.columns):
            ws.write(header_row, header_col + i, col, header_fmt)

        # ===== Data Rows =====
        data_start_row = 7
        variance_col_idx = list(df.columns).index("Variance")

        for ridx, row in df[:-1].iterrows():
            for cidx, val in enumerate(row):
                fmt = variance_fmt if cidx == variance_col_idx else data_fmt
                ws.write(data_start_row + ridx, header_col + cidx, val, fmt)

        # ===== TOTAL ROW =====
        total_row_idx = len(df) + 6
        for cidx, val in enumerate(df.iloc[-1]):
            ws.write(total_row_idx, header_col + cidx, val, total_fmt)

        # ===== Footer =====
        ws.write(
            total_row_idx + 2,
            1,
            "Confidential | 2025 Labor Pilot",
            workbook.add_format({"align": "left"})
        )

        # ===== Auto width =====
        for i, col in enumerate(df.columns):
            w = max(len(str(col)), df[col].astype(str).str.len().max())
            ws.set_column(1 + i, 1 + i, max(12, min(30, (w or 10) + 2)))

    return file_path
# -------------------------------------------------------------
# PDF EXPORT — Productivity Index (Scheduler)
# -------------------------------------------------------------
def export_productivity_index_pdf(df, file_path, metadata):

    styles = getSampleStyleSheet()
    elements = []

    doc = SimpleDocTemplate(
        file_path,
        pagesize=landscape(letter),
        leftMargin=28,
        rightMargin=28,
        topMargin=24,
        bottomMargin=24
    )

    # ---------- TITLE ----------
    elements.append(Paragraph("<b>Productivity Index Report</b>", styles["Heading2"]))
    elements.append(Spacer(1, 6))

    # ---------- LEFT METADATA ----------
    elements.append(Paragraph(f"<b>Department:</b> {metadata['dept']}", styles["Normal"]))
    elements.append(Paragraph(f"<b>Position:</b> {metadata['pos']}", styles["Normal"]))
    elements.append(Paragraph(f"<b>Period:</b> {metadata['period']}", styles["Normal"]))
    elements.append(Spacer(1, 12))

    # ---------- TABLE ----------
    pdf_data = [df.columns.tolist()] + df.values.tolist()
    table = Table(pdf_data, repeatRows=1)

    table_style = TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#16365C")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#A6A6A6")),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ])

    variance_col = pdf_data[0].index("Variance")

    for r in range(1, len(pdf_data)):
        row = pdf_data[r]

        if row[0] == "TOTAL":
            table_style.add(
                "BACKGROUND",
                (0, r), (-1, r),
                colors.HexColor("#DCE6F1")
            )
        else:
            table_style.add(
                "BACKGROUND",
                (variance_col, r),
                (variance_col, r),
                colors.HexColor("#FEF6F0")
            )

    table.setStyle(table_style)
    elements.append(table)

    # ---------- HEADER / FOOTER ----------
    def draw_header(canvas, doc):
        canvas.saveState()

        canvas.setFont("Helvetica-Bold", 10)
        canvas.drawRightString(
            doc.pagesize[0] - doc.rightMargin,
            doc.pagesize[1] - 40,
            metadata["hotel"]
        )

        canvas.setFont("Helvetica", 8)
        canvas.drawRightString(
            doc.pagesize[0] - doc.rightMargin,
            doc.pagesize[1] - 54,
            metadata["created_str"]
        )

        canvas.restoreState()

    def draw_footer(canvas, doc):
        canvas.saveState()

        canvas.setFont("Helvetica", 8)
        canvas.drawString(
            doc.leftMargin,
            doc.bottomMargin - 12,
            "Confidential | 2025 Labor Pilot"
        )

        canvas.drawRightString(
            doc.pagesize[0] - doc.rightMargin,
            doc.bottomMargin - 12,
            f"Page | {canvas.getPageNumber()}"
        )

        canvas.restoreState()

    doc.build(
        elements,
        onFirstPage=lambda c, d: (draw_header(c, d), draw_footer(c, d)),
        onLaterPages=lambda c, d: (draw_header(c, d), draw_footer(c, d))
    )

    return file_path


def export_labor_variance_excel(df, output_path, metadata):

      import pandas as pd
      from datetime import datetime

      with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:

            df.to_excel(
                  writer,
                  sheet_name="Labor Variance",
                  index=False,
                  startrow=6,
                  startcol=1,
                  header=False
            )

            workbook  = writer.book
            worksheet = writer.sheets["Labor Variance"]

            # ───── Sheet Background ─────
            solid_bg_fmt = workbook.add_format({'bg_color': '#FFFFFF', 'pattern': 1})
            worksheet.set_column(0, 60, None, solid_bg_fmt)
            for r in range(0, 2000):
                  worksheet.set_row(r, None, solid_bg_fmt)

            worksheet.set_column(0, 0, 2)

            # ───── Formats ─────
            title_fmt  = workbook.add_format({'bold': True, 'font_size': 14})

            borderless_label_fmt = workbook.add_format({
                  'bold': True, 'border': 0, 'bg_color': '#FFFFFF'
            })

            borderless_value_fmt = workbook.add_format({
                  'border': 0, 'bg_color': '#FFFFFF'
            })

            header_fmt = workbook.add_format({
                  'bold': True,
                  'bg_color': '#16365C',
                  'font_color': '#FFFFFF',
                  'border': 1,
                  'border_color': '#A6A6A6',
                  'align': 'center',
                  'pattern': 1
            })

            dept_fmt = workbook.add_format({
                  'bold': True,
                  'align': 'center',
                  'border': 1,
                  'border_color': '#A6A6A6'
            })

            pos_fmt = workbook.add_format({
                  'bold': True,
                  'align': 'center',
                  'border': 1,
                  'border_color': '#A6A6A6'
            })

            default_fmt = workbook.add_format({
                  'border': 1,
                  'border_color': '#A6A6A6',
                  'num_format': '#,##0.00',
                  'align': 'center'
            })

            variance_fmt = workbook.add_format({
                  'bg_color': '#FEF6F0',
                  'border': 1,
                  'border_color': '#A6A6A6',
                  'num_format': '#,##0.00',
                  'align': 'center',
                  'pattern': 1
            })

            total_fmt = workbook.add_format({
                  'bg_color': '#DCE6F1',
                  'bold': True,
                  'border': 1,
                  'border_color': '#A6A6A6',
                  'num_format': '#,##0.00',
                  'align': 'center',
                  'pattern': 1
            })

            total_text_fmt = workbook.add_format({
                  'bg_color': '#DCE6F1',
                  'bold': True,
                  'border': 1,
                  'border_color': '#A6A6A6',
                  'align': 'center',
                  'pattern': 1
            })

            total_varpct_fmt = workbook.add_format({
                  'bg_color': '#DCE6F1',
                  'bold': True,
                  'border': 1,
                  'border_color': '#A6A6A6',
                  'align': 'center',
                  'pattern': 1
            })

            # ───── Title + Metadata ─────
            worksheet.write("B1", "Labor Variance Report", title_fmt)

            worksheet.write("B3", "Department:", borderless_label_fmt)
            worksheet.write("C3", metadata.get("dept"), borderless_value_fmt)

            worksheet.write("B4", "Position:", borderless_label_fmt)
            worksheet.write("C4", metadata.get("pos"), borderless_value_fmt)

            worksheet.write("B5", "Week:", borderless_label_fmt)
            worksheet.write("C5", metadata.get("period"), borderless_value_fmt)

            # ───── Header Row ─────
            header_row = 6
            header_col = 1
            for col_idx, col in enumerate(df.columns):
                  worksheet.write(header_row, header_col + col_idx, col, header_fmt)

            # ───── Data Rows ─────
            for row_idx, row in df.iterrows():
                  is_total = row["Position"] == "TOTAL"
                  for col_idx, col in enumerate(df.columns):
                        val = row[col]

                        if col == "Department":
                              fmt = total_text_fmt if is_total else dept_fmt
                        elif col == "Position":
                              fmt = total_text_fmt if is_total else pos_fmt
                        elif col == "Variance %":
                              fmt = total_varpct_fmt if is_total else variance_fmt
                        elif "Variance" in col:
                              fmt = total_fmt if is_total else variance_fmt
                        else:
                              fmt = total_fmt if is_total else default_fmt

                        worksheet.write(7 + row_idx, 1 + col_idx, val, fmt)

            # ───── Footer ─────
            rightmost_col = 1 + len(df.columns) - 1
            worksheet.write(0, rightmost_col, metadata.get("hotel"),
                  workbook.add_format({'bold': True, 'align': 'right', 'border': 0})
            )

            worksheet.write(1, rightmost_col, metadata.get("created_str"),
                  workbook.add_format({'align': 'right', 'border': 0})
            )

            bottom_row = 7 + len(df) + 2
            worksheet.write(bottom_row, 1, "Confidential | 2025 Labor Pilot",
                  workbook.add_format({'align': 'left', 'border': 0})
            )

            # ───── AUTO-EXPAND COLUMN WIDTHS (NO SHRINKING) ─────
            for i, col in enumerate(df.columns):
                  series = df[col].astype(str)
                  max_len = max(
                        series.map(len).max(),
                        len(col)
                  ) + 3

                  worksheet.set_column(
                        1 + i,        # start col
                        1 + i,        # end col
                        max(16, min(max_len, 60))  # safe readable bounds
                  )

      return output_path

# -------------------------------------------------------------
# PDF EXPORT — Labor Variance (Scheduler) ✅ SHRUNK PAGE FIX
# -------------------------------------------------------------
def export_labor_variance_pdf(df, file_path, metadata):

      from reportlab.lib.pagesizes import landscape, letter
      from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
      from reportlab.lib import colors
      from reportlab.lib.styles import getSampleStyleSheet

      styles = getSampleStyleSheet()
      elements = []

      # ✅ TIGHTER PAGE SCALE
      doc = SimpleDocTemplate(
            file_path,
            pagesize=landscape(letter),
            leftMargin=48,
            rightMargin=48,
            topMargin=28,
            bottomMargin=28
      )

      # ---------- TITLE ----------
      elements.append(Paragraph("<b>Labor Variance Report</b>", styles["Heading2"]))
      elements.append(Spacer(1, 6))

      # ---------- LEFT METADATA ----------
      elements.append(Paragraph(f"<b>Department:</b> {metadata['dept']}", styles["Normal"]))
      elements.append(Paragraph(f"<b>Position:</b> {metadata['pos']}", styles["Normal"]))
      elements.append(Paragraph(f"<b>Week:</b> {metadata['period']}", styles["Normal"]))
      elements.append(Spacer(1, 10))

      # ---------- TABLE DATA ----------
      pdf_data = [df.columns.tolist()] + df.values.tolist()

      # ✅ SHRUNK COLUMN WIDTHS (THIS IS THE MAIN FIX)
      column_widths = [
            140,   # Department
            130,   # Position
            90,   # Scheduled Hours
            90,   # Actual Hours
            90,   # Projected Hours
            90,    # Variance
            90    # Variance %
      ]

      table = Table(pdf_data, colWidths=column_widths, repeatRows=1)

      table_style = TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#16365C")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("ALIGN", (0, 0), (-1, -1), "CENTER"),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#A6A6A6")),

            # ✅ SMALLER FONT = SHRUNK VISUAL SCALE
            ("FONTSIZE", (0, 0), (-1, -1), 8),

            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
      ])

      # ✅ FIND VARIANCE & VARIANCE %
      variance_col = pdf_data[0].index("Variance")
      var_pct_col  = pdf_data[0].index("Variance %")

      for r in range(1, len(pdf_data)):
            row = pdf_data[r]

            # TOTAL ROW
            if row[1] == "TOTAL":
                  table_style.add(
                        "BACKGROUND",
                        (0, r), (-1, r),
                        colors.HexColor("#DCE6F1")
                  )
                  table_style.add(
                        "FONTNAME",
                        (0, r), (-1, r),
                        "Helvetica-Bold"
                  )

            # ✅ SHADE BOTH VARIANCE + VARIANCE %
            else:
                  table_style.add(
                        "BACKGROUND",
                        (variance_col, r),
                        (variance_col, r),
                        colors.HexColor("#FEF6F0")
                  )
                  table_style.add(
                        "BACKGROUND",
                        (var_pct_col, r),
                        (var_pct_col, r),
                        colors.HexColor("#FEF6F0")
                  )

      table.setStyle(table_style)
      elements.append(table)

      # ---------- HEADER / FOOTER (UNCHANGED STYLE) ----------
      def draw_header(canvas, doc):
            canvas.saveState()

            canvas.setFont("Helvetica-Bold", 10)
            canvas.drawRightString(
                  doc.pagesize[0] - doc.rightMargin,
                  doc.pagesize[1] - 36,
                  metadata["hotel"]
            )

            canvas.setFont("Helvetica", 8)
            canvas.drawRightString(
                  doc.pagesize[0] - doc.rightMargin,
                  doc.pagesize[1] - 50,
                  metadata["created_str"]
            )

            canvas.restoreState()

      def draw_footer(canvas, doc):
            canvas.saveState()

            canvas.setFont("Helvetica", 8)
            canvas.drawString(
                  doc.leftMargin,
                  doc.bottomMargin - 10,
                  "Confidential | 2025 Labor Pilot"
            )

            canvas.drawRightString(
                  doc.pagesize[0] - doc.rightMargin,
                  doc.bottomMargin - 10,
                  f"Page | {canvas.getPageNumber()}"
            )

            canvas.restoreState()

      doc.build(
            elements,
            onFirstPage=lambda c, d: (draw_header(c, d), draw_footer(c, d)),
            onLaterPages=lambda c, d: (draw_header(c, d), draw_footer(c, d))
      )

      return file_path

__all__ = [
    "export_ot_risk_excel",
    "export_ot_risk_pdf",
    "export_forecast_variance_excel",
    "export_forecast_variance_pdf",
    "export_productivity_index_excel",
    "export_productivity_index_pdf",
    "export_labor_variance_excel",   # ✅ ADDED
    "export_labor_variance_pdf",     # ✅ ADDED
]