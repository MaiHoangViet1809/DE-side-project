import smtplib, ssl
import traceback
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from os.path import basename
from pathlib import Path

from cores.utils.configs import FrameworkConfigs as CFG


def send_email(send_to: str, msg_body: str, subject: str, cc_to: str = None, bcc_to: str = None, files: list = None):
    """
    Helper function to send an email via SMTP.

    Args:
        send_to (str): Recipient email addresses, separated by semicolons.
        msg_body (str): Email body content or path to a text file with the body content.
        subject (str): Subject of the email.
        cc_to (str, optional): CC email addresses, separated by semicolons. Defaults to None.
        bcc_to (str, optional): BCC email addresses, separated by semicolons. Defaults to None.
        files (list, optional): List of file paths to attach to the email. Defaults to None.

    Returns:
        None

    Examples:
        ```python
        send_email(
            send_to="recipient@example.com",
            msg_body="This is a test email.",
            subject="Test Email",
            cc_to="cc@example.com",
            bcc_to="bcc@example.com",
            files=["/path/to/file.txt"]
        )
        ```
    """

    smtp_server = CFG.Email.HOST_EMAIL_IP  # "smtp.office365.com"
    port = int(CFG.Email.HOST_EMAIL_PORT)  # 25 / 587
    sender_email = CFG.Email.EMAIL_USER
    password = CFG.Email.EMAIL_PASSWORD

    try:
        # email body
        message = MIMEMultipart("alternative")
        message["Subject"] = subject
        message["From"] = sender_email
        message["To"] = send_to

        if cc_to:
            message["Cc"] = cc_to

        if bcc_to:
            message["Bcc"] = bcc_to

        # read msg body from file
        if len(msg_body) < 255 or msg_body.endswith(".txt"):
            if Path(msg_body).resolve().exists():
                msg_body = Path(msg_body).read_text()

        # Create the plain-text and HTML version of your message
        text = msg_body

        html = text if "</html>" in text else f"""\
        <html>
          <body>
            <p>
            {text}
            </p>
          </body>
        </html>
        """

        # Turn these into plain/html MIMEText objects
        part1 = MIMEText(text, "plain")
        part2 = MIMEText(html, "html", _charset=None)

        # Add HTML/plain-text parts to MIMEMultipart message
        # The email client will try to render the last part first
        message.attach(part1)
        message.attach(part2)

        # attach file to email:
        for f in files or []:
            with open(f, "rb") as fil:
                part = MIMEApplication(fil.read(), Name=basename(f))
            # After the file is closed
            part['Content-Disposition'] = f'attachment; filename="{basename(f)}"'
            message.attach(part)

        # Create a secure SSL context
        context = ssl.create_default_context()
        with smtplib.SMTP(host=smtp_server, port=port) as server:
            server.starttls(context=context)
            server.login(sender_email, password)
            server.send_message(msg=message,
                                # from_addr=sender_email,
                                )

    except Exception as e:
        print(f"[send_email] error: {e}")
        print(f"[send_email] traceback: {traceback.format_exc()}")
    finally:
        # print("[send_email] finally")
        ...


if __name__ == "__main__":
    print(CFG.Email.HOST_EMAIL_IP, CFG.Email.HOST_EMAIL_PORT, CFG.Email.EMAIL_USER, CFG.Email.EMAIL_CC_TO_DEFAULT, CFG.Email.ADMIN_DATA_ENGINEER)
    send_email(send_to=CFG.Email.EMAIL_CC_TO_DEFAULT, msg_body="test email", subject="TEST EMAIL AUTOMATION", cc_to=CFG.Email.ADMIN_DATA_ENGINEER)
