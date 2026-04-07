# ============================================================
# email_sender.py  — Gmail App Password Email Utility
# Sends emails with Excel + PDF attachments for scheduler
# ============================================================

import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import os
import traceback


# ------------------------------------------------------------
# CONFIGURATION — Change only if needed
# ------------------------------------------------------------
EMAIL_SENDER = "alfredo1998x@gmail.com"
EMAIL_PASSWORD = "iqry ajnp zvuo yeuq"   # Gmail App Password (safe to store)


# ------------------------------------------------------------
# send_email()
# Sends an email with multiple attachments
# ------------------------------------------------------------
def send_email(recipients, subject, body, attachments=None):
    """
    recipients: list of email strings
    subject: string
    body: string
    attachments: list of file paths
    """
    if isinstance(recipients, str):
        recipients = [recipients]

    print("\n📧 Preparing email...")
    print("Recipients:", recipients)
    print("Attachments:", attachments)

    message = MIMEMultipart()
    message["From"] = EMAIL_SENDER
    message["To"] = ", ".join(recipients)
    message["Subject"] = subject

    # Email body
    message.attach(MIMEText(body, "plain"))

    # --------------------------------------------------------
    # Attach files (Excel + PDF)
    # --------------------------------------------------------
    if attachments:
        for file_path in attachments:
            if not os.path.isfile(file_path):
                print(f"⚠️ Skipping missing attachment: {file_path}")
                continue

            filename = os.path.basename(file_path)

            try:
                with open(file_path, "rb") as attachment:
                    part = MIMEBase("application", "octet-stream")
                    part.set_payload(attachment.read())

                encoders.encode_base64(part)
                part.add_header(
                    "Content-Disposition",
                    f"attachment; filename= {filename}",
                )
                message.attach(part)
                print(f"📎 Attached file: {filename}")

            except Exception as e:
                print(f"❌ Error attaching {filename}: {e}")
                traceback.print_exc()

    # --------------------------------------------------------
    # Connect to Gmail SMTP
    # --------------------------------------------------------
    try:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as smtp:
            smtp.login(EMAIL_SENDER, EMAIL_PASSWORD)
            smtp.sendmail(EMAIL_SENDER, recipients, message.as_string())

        print("✅ Email sent successfully!")

    except Exception as e:
        print("❌ Email FAILED to send!")
        traceback.print_exc()
