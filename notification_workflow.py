#!/usr/bin/python
# coding: utf-8
import datetime
import psycopg2 as pg
import pandas as pd
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
import smtplib

VIEW_NOTIFICATION_REASON_MAP = {
    'scenario1': 'Not activated',
    'scenario2': 'Not used'
}
EMAIL_HOST = ''
EMAIL_HOST_USER = ''
EMAIL_HOST_PASSWORD = ''
EMAIL_PORT =


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
        from emailhtmltemplate import html_temp
        # Get user record from view
        users = pd.read_sql(
            'select * from {}'.format(view),
            self.pg_connection).to_dict(orient="records")

        # msg['From'] = config.EMAIL_ADDRESS
        # msg['To'] = to
        for user in users:
            html_temp(user['userid'])
            html = """\
            <html>
                <head>
                    <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
                </head>
                <body>
                    """ + html_temp.msg + """
                 </body>
             </html>"""

            part = MIMEText(html, 'html')
            msg = MIMEMultipart('alternative')
            msg['Subject'] = html_temp.subject
            msg.attach(part)

            # message='Subject:{} {}'.format(subject,msg)
            try:
                # self.email_server.sendmail(
                #     from_addr=from_addr,
                #     to_addrs=user['assigned_email'],
                #     msg=msg.as_string())

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
                print e.message
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
