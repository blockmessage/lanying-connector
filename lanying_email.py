import smtplib
from email.mime.text import MIMEText
from email.header import Header
import os
import logging

def send_mail(email, subject, content):
    try:
        smtp_user = os.getenv("LANYING_CONNECTOR_SMTP_USER")
        smtp_pass = os.getenv("LANYING_CONNECTOR_SMTP_PASS")
        msg = MIMEText(content, "plain", "utf-8")
        msg['Subject'] = Header(subject, "utf-8")
        msg['From'] = smtp_user
        msg['To'] = email

        # 连接 SMTP 服务器并发送邮件
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.login(smtp_user, smtp_pass)
        server.send_message(msg)
        server.quit()
        return True
    except smtplib.SMTPException as e:
        logging.error(f"邮件发送失败: {e}")
        return False
