from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
import smtplib

def alert_notification1(EMAIL_FROM, EMAIL_TO, msg):
  ##AWS Config
  EMAIL_HOST = 'email-smtp.us-east-1.amazonaws.com'
  EMAIL_HOST_USER = 'AKIAJTFDDSQQC55IAXUA'
  EMAIL_HOST_PASSWORD = 'Aj/kTq8YAcqinnJvXxlurqFywVoM9QbTw1tyThFMKfwa'
  EMAIL_PORT = 587
  s = smtplib.SMTP(EMAIL_HOST, EMAIL_PORT)
  s.starttls()
  s.login(EMAIL_HOST_USER, EMAIL_HOST_PASSWORD)
  s.sendmail(EMAIL_FROM, EMAIL_TO , msg.as_string())
  s.quit()


EMAIL_FROM = 'Pranay.Appani@Viacomcontractor.com'
EMAIL_TO = ['Pranay.Appani@Viacomcontractor.com','Honey.shahi@Viacomcontractor.com']
msg = MIMEMultipart('alternative')


msg['Subject'] = "Test Subject"
body = "Test Body"

msg.attach(MIMEText(body, 'plain'))

alert_notification1(EMAIL_FROM,EMAIL_TO, msg)