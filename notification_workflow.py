#!/usr/bin/python
# coding: utf-8
import datetime
import psycopg2 as pg
import pandas as pd
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
import smtplib
import emailhtmltemplate
import emailhtmltemplate2

VIEW_NOTIFICATION_REASON_MAP = {
    'scenario1': 'Not activated',
    'scenario2': 'Not used'
}
EMAIL_HOST = 'email-smtp.us-east-1.amazonaws.com'
EMAIL_HOST_USER = 'AKIAJTFDDSQQC55IAXUA'
EMAIL_HOST_PASSWORD = 'Aj/kTq8YAcqinnJvXxlurqFywVoM9QbTw1tyThFMKfwa'
EMAIL_PORT = 587

class Notify(object):
    def __init__(self, host, database, user, password, port=5432):
        self.pg_connection = pg.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port)
        self.email_server = smtplib.SMTP(EMAIL_HOST, EMAIL_PORT)
        self.email_server.starttls()
        self.email_server.login(EMAIL_HOST_USER, EMAIL_HOST_PASSWORD)

    def __del__(self):
        print "Killing email server: {}".format(self.email_server)
        self.email_server.quit()

    def notify_users_in_view(self, view, from_addr):
        cursor = self.pg_connection.cursor()
        # Get user record from view
        users = pd.read_sql(
            'select * from {}'.format(view),
            self.pg_connection).to_dict(orient="records")

	# msg['From'] = config.EMAIL_ADDRESS
        # msg['To'] = to
        for user in users:
            msg = MIMEMultipart('alternative')
	    if view == 'scenario1':
	        emailhtmltemplate. html_temp(user['userid'])
		msg['Subject'] = emailhtmltemplate.html_temp.subject
		body = emailhtmltemplate.html_temp.msg
	    elif view == 'scenario2':
		emailhtmltemplate2.html_temp(user['userid'])
		msg['Subject'] = emailhtmltemplate2.html_temp.subject
		body = emailhtmltemplate2.html_temp.msg
            html = """\
            <html>
                <head>
                    <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
                </head>
                <body>
                    """ + body + """
                 </body>
             </html>"""

            part = MIMEText(html, 'html')
            msg.attach(part)

            # message='Subject:{} {}'.format(subject,msg)
            try:
                self.email_server.sendmail(
                    from_addr=from_addr,
                    to_addrs=user['assigned_email'],
                    msg=msg.as_string())

                # Update notification status and date
                query = "UPDATE master " \
                        "SET notification_date='{}', notification_status='{}', notification_reason='{}' " \
                        "WHERE key_name='{}'".format(datetime.datetime.now(), 'YES',
                                                     VIEW_NOTIFICATION_REASON_MAP[view], user['key_name'])
                # import pdb
                # pdb.set_trace()
                cursor.execute(query)
                self.pg_connection.commit()
            except Exception as e:
                print "Could not send email notification to {}".format(
                    user['assigned_email'])
                print e
        cursor.close()


if __name__ == '__main__':
    notifier = Notify(
        host='localhost',
        database='postgres',
        user='postgres',
        password='qwerty',
        port=5433
    )
    for view in VIEW_NOTIFICATION_REASON_MAP:
        notifier.notify_users_in_view(
            view,
            from_addr="honey.shahi@viacomcontractor.com"
        )
