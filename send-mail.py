import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import re
import os.path
import os
import config
import csv
import mymsg


def isValidEmail(email):
    
    if len(email) > 7:
        
        if re.match("[^@]+@[^@]+\.[^@]+", email) != None:
            return True
        return False

def send_email(subject,msg1,to):

    try:

        server=smtplib.SMTP(config.SMTP_MAIL_SERVER)
        server.ehlo()
        server.starttls()
        msg = MIMEMultipart('alternative')
        msg['Subject']=subject
        msg['From']=config.EMAIL_ADDRESS
        msg['To']=to
		
        html="""\
		<html>
			<head>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
            </head>
			<body>
			 	"""+msg1+"""
			 </body>
		 </html>"""
		
        part=MIMEText(html, 'html')

        msg.attach(part)
        #login request to the server
        server.login(config.EMAIL_ADDRESS,config.PASSWORD)
		
        #message='Subject:{} {}'.format(subject,msg)
        server.sendmail(config.EMAIL_ADDRESS,to,msg.as_string())
        server.quit()
        print("success:email sent")
    except:
        print("failure: email not sent")

#subject="Test subject"
#msg="Testing Testing"
#to="gs.io.dev@gmail.com"
#send_email(subject,msg,to)

#----------------------------------------------------------------------
def csv_reader(file_obj):
    """
    Read a csv file
    """
    reader = csv.DictReader(file_obj, delimiter=',')
    for row in reader:
        # print(" ".join(row))
        #assign_email=
        print(row["Assigned Email"])
        assign_mail=row["Assigned Email"]
        username=row["Key Name"]
        mymsg.html_temp(username)
        
        subject=mymsg.html_temp.sub
        msg=mymsg.html_temp.msg1
        #print(msg)
        to=row["Assigned Email"]
        if username:
            if to :
                if isValidEmail(to) == True :
                    
                    send_email(subject,msg,to)

                else:
                    print ("This is not a valid email address")
            else:
                print ("Email address doesn't exist")
        else:
            print ("Key Name  doesn't exist")


#----------------------------------------------------------------------
if __name__ == "__main__":
    
    """
    Start the program
    """
    #command="python3 -m smtpd -n localhost:1024"
    #os.system(command)

    csv_path = config.CSV_FILE_PATH

    if os.path.exists(csv_path):
        
        with open(csv_path) as f_obj:
            csv_reader(f_obj)
    
    else:
        print ("CSV File doesn't exist please check it again")