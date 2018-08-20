# Databricks notebook source
#################################################################################################################################################################
#Desc: Parameterised this list or create a table for the same and join them to extract the data, related to required programs/episodes. 
#      
#      
#Step 1 - Run API connector to load data from Nielsen to NSR_Program_Report_parquet,NSR_Episode_Report_parquet and NSR_Network_Report_parquet  hive tables.
#Step 2 - Run Report generation and Email notification. 
#Initial setup - Hive External table should be created and historical data needs to populated.
#################################################################################################################################################################


## Run from this STEP.
from datetime import date
from datetime import timedelta
from datetime import datetime
from pytz import timezone
from pyspark.sql.types import  StructType,StructField, StringType, LongType, IntegerType


def get_program_dl_sheet_info_func(run_day):
  
  print(" *** In get_program_dl_sheet_info function *** ")
  print("get_program_dl_sheet_info run_day:{}".format(run_day))
  myschema= StructType([
  StructField('Program_Name' ,StringType()),
  StructField('Aired_Day' ,StringType()),
  StructField('DL' ,StringType()),
  StructField('Program_Episode' ,StringType()),
  StructField('Premiere_Date' ,StringType()),
  StructField('Day_Part' ,StringType())
  ]) 
  
  #date_of_interest = date.today() - timedelta(days=2)
  #run_day = date_of_interest.strftime("%A")

  df_all_program = spark.read.format('com.crealytics.spark.excel').schema(myschema).option('sheetName','Programs_DL').option('useHeader','true').options(header='true', inferschema='false').load('/mnt/vmn-datalake-dev/vmn-research-nielsen-scr/Programs_DL.xlsx')
  #df_all_program.show()
  df_all_program.createOrReplaceTempView("df_program_dl_tab")
  #df_all_program.describe()
  pgm_to_run_Str="""select distinct Program_Name,trim(DL) as DL,trim(Program_Episode) as Program_Episode,Premiere_Date,Day_Part from df_program_dl_tab where UPPER(TRIM(Aired_Day)) = '{}' """.format(run_day.upper())
  print ("pgm_to_run_Str: {}".format(pgm_to_run_Str))
  program_to_run = spark.sql(pgm_to_run_Str)
  program_to_run.show()

  program_dl_full_list = program_to_run.select("Program_Name","DL","Program_Episode","Premiere_Date","Day_Part").rdd.map(lambda x: (( x[0],x[1],x[2],x[3],x[4]))  ).collect() 
  #print("program_dl_full_list:{}".format(program_dl_full_list))
  
  
 
  return program_dl_full_list









# COMMAND ----------

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
import smtplib

def alert_notification1(EMAIL_FROM, EMAIL_TO, msg):
  ##AWS Config
  EMAIL_HOST = 'email-smtp.us-east-1.xxxxx.com'
  EMAIL_HOST_USER = 'xxxxxx'
  EMAIL_HOST_PASSWORD = 'xxxxxx'
  EMAIL_PORT = 111
  s = smtplib.SMTP(EMAIL_HOST, EMAIL_PORT)
  s.starttls()
  s.login(EMAIL_HOST_USER, EMAIL_HOST_PASSWORD)
  s.sendmail(EMAIL_FROM, EMAIL_TO , msg.as_string())
  s.quit()	  

# COMMAND ----------


#Email message body is created as part of this function in html format.
def msg_body_creation1(pgm_name):

    global email_line1_str, email_line2_str, email_line3_str, email_line4_str,email_line5_str,email_sourceline_str
    global msg_body,msg_body_html        

    from pytz import timezone
    tz = timezone('EST')
    currentTime = datetime.now(tz) 

    if currentTime.hour < 12:
        greet = 'Good Morning!'
    elif 12 <= currentTime.hour < 18:
        greet = 'Good Afternoon!'
    else:
        greet = 'Good Evening!'

    messageGreeting = """
{greet}

""".format(greet=greet)            

    thankingNote="""

Thanks, <br/>
VDS Digital Analytics and Data Platform Team
"""

    msg_body_html = """\
<html>
<p> {messageGreeting} </p>

  <p>Below is today’s automated Nielsen Social report for last night’s episode of {pgm_name}. </p>
  
  <p>{email_line1_str}</p>
  
  <ul style="list-style-type:disc">  
      <li><b> Twitter:</b> {email_line2_str}</li>      
      <li><b> Facebook:</b> {email_line4_str}</li>
  </ul>
  
  <p>{email_line5_str}</p>
  
<p>  <i> {email_sourceline_str} </i></p>


<p> {thankingNote} </p>
</body>
</html>

""".format(messageGreeting=messageGreeting,pgm_name=pgm_name,email_line1_str=email_line1_str,email_line2_str=email_line2_str,email_line3_str=email_line3_str,email_line4_str=email_line4_str,email_line5_str=email_line5_str,email_sourceline_str=email_sourceline_str,thankingNote=thankingNote)

    #removed Instagram as NSR will no longer report them: <li><b> Instagram:</b> {email_line3_str} </li>          
    
#Email notification, using all the variables created in msg_body_creation function  
def email_notification1(pgm_name,doa,episode_num_pgm_dict):
    
    global msg_body,msg_body_html,Programs_DL_Dict
    EMAIL_FROM = 'noreply@viacom.com'    
    #EMAIL_TO = ['Pranay.Appani@Viacomcontractor.com']#,Dan.Morris@viacom.com','ramesh.nuthalapati@viacom.com'
    
    print("EMAIL_TO:{}".format(EMAIL_TO))
    body = msg_body_html

    
    subject_of_mail="'{}' Nielsen Social Report: {}".format(pgm_name,episode_num_pgm_dict[pgm_name])
    msg = MIMEMultipart('alternative')
    #msg.attach(MIMEText(body, 'plain'))
    msg.attach(MIMEText(body, 'html'))
    
    msg['Subject'] = subject_of_mail
    print ("############subject#########: {}".format(subject_of_mail))
    print ("############body#########: {}".format(body) )
    #Send email
    alert_notification1(EMAIL_FROM,EMAIL_TO, msg)



# COMMAND ----------

#################################################################################################################################################################
#Desc: This Function is called from metrics_process_and_send_email function for each program. 
#      Due to frequent changes in requirements, many unused KPI's are left as is, for any future usage.
#      Rank - This value needs to be populated both from Network and Program/Episode report level. 
#      Season Average - If number of episodes aired are more than 2 or <= 3 from the premiere_dt provided in the reference Program_DL google sheet, only then 
#                       calculate the season average and display it. If not it should be "N/A".
#      WoW calculations - If past week data is not available, then all the WoW values are set as "N/A"
#
#
#################################################################################################################################################################

def current_program_email_stmnts_permiere1(tdy_idx,p7day_idx,doa,pdTodayDFsubset,pdPast7dayDFsubset,pgm_or_epsd,Program_Dl_Premiere_Dt_Dict,pgm_name,Day_Part):

    print (" *** In current_program_email_stmnts function *** ")
    global run_date
    global email_line1_str, email_line2_str, email_line3_str, email_line4_str,email_line5_str,email_sourceline_str

    Total_Social_Intr , Total_Twitter_Intr ,Total_Insta_Intr,Total_Facebook_Intr,Total_Twitter_Uniq_Auth,Total_Facebook_Uniq_Auth = 0,0,0,0,0,0    
    Total_Social_Intr_pct_past7day,Twitter_Intr_pct_past7day,Twitter_Uniq_Auth_pct_past7day,Instagram_Intr_pct_past7day = 0.0,0.0,0.0,0.0
    Facebook_Intr_pct_past7day,Facebook_Uniq_Auth_pct_past7day = 0.0,0.0
    SeasonAvg_Uniques_Twitter_pct_past7day=0.0
    Today_Rank,rank_source,email_line1_str, email_line2_str, email_line3_str, email_line4_str,email_line5_str ='','','','','','','' 
    print("tdy_idx: {},p7day_idx: {}".format(tdy_idx,p7day_idx))
    #print("pdTodayDFsubset['Rank'][0].values[0] : {}".format(pdTodayDFsubset['Rank'][tdy_idx].values[0]))
    
    if pgm_or_epsd == 'P':
       rank_source='Program'
    elif pgm_or_epsd == 'E':
      rank_source='Episode' 

    ## Get Value of Rank from Network
    Today_Pgm_Rank=pdTodayDFsubset['Rank'][tdy_idx].values[0]
    network_Name = pdTodayDFsubset['Network'][tdy_idx].values[0]                     
    NetworkRankStr=""" SELECT Rank FROM NSR_Network_Report_parquet  where to_date(from_unixtime(unix_timestamp(report_date ,'MM/dd/yyyy'), 'yyyy-MM-dd')) = '{run_date}' and UPPER(TRIM(network)) IN ('{network_Name}')  and UPPER(TRIM(Day_Part)) IN ({Day_Part}) """.format(run_date=run_date,network_Name=network_Name.upper(),Day_Part=Day_Part.upper())   
    
    Today_Ntwrk_Rank=(spark.sql(NetworkRankStr).first()).Rank
    
    ##Get the Average Uniques_twitter compared with This week vs Last week.
    first_premiereDt=''
    first_premiereDt=Program_Dl_Premiere_Dt_Dict[pgm_name]
    print ("pgm_name:{} and its 1st premiere date: {}".format(pgm_name,first_premiereDt))
    NumberOfEpisodesAiredStr=""" SELECT count(episode_number) as NumEpdAired from NSR_Episode_Report_parquet where UPPER(TRIM(program_name)) in ("{pgm_name}") 
    and to_date(from_unixtime(unix_timestamp(report_date ,'MM/dd/yyyy'), 'yyyy-MM-dd'))  >= '{first_premiereDt}' """.format(first_premiereDt=first_premiereDt,pgm_name=pgm_name.upper())   

    Number_Of_Episodes_Aired=0
    Number_Of_Episodes_Aired=(spark.sql(NumberOfEpisodesAiredStr).first()).NumEpdAired 
    print("Number_Of_Episodes_Aired: {}".format(Number_Of_Episodes_Aired))
    
    if (Number_Of_Episodes_Aired > 2) and (pdPast7dayDFsubset.empty==False) :
        SeasonAvgUniquesTwitterStr=""" SELECT This_Week.program_name, This_Week.avg_uniqes_twitter,Last_week.avg_uniqes_twitter, (This_Week.avg_uniqes_twitter-Last_week.avg_uniqes_twitter)/Last_week.avg_uniqes_twitter as Avg_Diff from 
        (SELECT program_name,count(episode_number),avg(uniques_twitter) as avg_uniqes_twitter from NSR_Episode_Report_parquet where UPPER(TRIM(program_name)) in ("{pgm_name}") 
        and (report_date >= '{first_premiereDt}' and report_date <= (  date_sub( '{run_date}', 7 ) )  ) and UPPER(TRIM(Day_Part)) IN ({Day_Part})  group by program_name) Last_week 
        join 
        (SELECT program_name,count(episode_number),avg(uniques_twitter) as avg_uniqes_twitter from NSR_Episode_Report_parquet where UPPER(TRIM(program_name)) in ("{pgm_name}") 
        and (report_date >= '{first_premiereDt}' and report_date <= '{run_date}') and UPPER(TRIM(Day_Part)) IN ({Day_Part}) group by program_name) This_Week
        on (This_Week.program_name = Last_week.program_name) """.format(first_premiereDt=first_premiereDt,run_date=run_date,pgm_name=pgm_name.upper(),Day_Part=Day_Part.upper()) 
        
        #print("AvgUniquesTwitterStr: {}".format(AvgUniquesTwitterStr))
        SeasonAvg_Uniques_Twitter_pct_past7day=0.0
        SeasonAvg_Uniques_Twitter_pct_past7day=(spark.sql(SeasonAvgUniquesTwitterStr).first()).Avg_Diff 
        #print("SeasonAvg_Uniques_Twitter_pct_past7day: {}".format(formatting('p',SeasonAvg_Uniques_Twitter_pct_past7day)))
    else:
        SeasonAvg_Uniques_Twitter_pct_past7day="N/A"
    
    ## Get values of Total Social Interations & its Pct from last week for every program record.
    
    ##Values under usage
    Total_Social_Intr=pdTodayDFsubset['Total_Interactions'][tdy_idx].values[0]

    ## Get values of Total Twitter Interations, Twitter Uniques for every program record.
    Total_Twitter_Intr=pdTodayDFsubset['Interactions_Twitter'][tdy_idx].values[0]
    Total_Twitter_Uniqs=pdTodayDFsubset['Uniques_Twitter'][tdy_idx].values[0]      
    Total_Facebook_Intr=pdTodayDFsubset['Interactions_Facebook'][tdy_idx].values[0]

    ##Values under non-usage
    Total_Twitter_Uniq_Eng=pdTodayDFsubset['Unique_Engagers_Twitter'][tdy_idx].values[0]
    Total_Twitter_Uniq_Auth=pdTodayDFsubset['Unique_Authors_Twitter'][tdy_idx].values[0] 
    Total_Insta_Intr=pdTodayDFsubset['Interactions_Instagram'][tdy_idx].values[0]
    Total_Facebook_Uniq_Auth=pdTodayDFsubset['Unique_Authors_Facebook'][tdy_idx].values[0]
    #Total_Facebook_Uniq_Eng=pdTodayDFsubset['Unique_Engagers_Facebook'][tdy_idx].values[0]
    
## Past 7 day comparison for WoW values
    if (pdPast7dayDFsubset.empty==False):
        
        ##Values under usage
        Total_Social_Intr_pct_past7day= ( (pdTodayDFsubset['Total_Interactions'][tdy_idx].values[0] / pdPast7dayDFsubset['Total_Interactions'][p7day_idx].values[0]) -1 )
        Total_Social_Intr_pct_past7day1=((pdTodayDFsubset['Total_Interactions'][tdy_idx].values[0] -pdPast7dayDFsubset['Total_Interactions'][p7day_idx].values[0] )/ pdPast7dayDFsubset['Total_Interactions'][p7day_idx].values[0])
        
        if pdPast7dayDFsubset['Interactions_Twitter'][p7day_idx].values[0] != 0:        
           Twitter_Intr_pct_past7day= (pdTodayDFsubset['Interactions_Twitter'][tdy_idx].values[0] - pdPast7dayDFsubset['Interactions_Twitter'][p7day_idx].values[0] )/ pdPast7dayDFsubset['Interactions_Twitter'][p7day_idx].values[0]
        if pdPast7dayDFsubset['Interactions_Twitter'][p7day_idx].values[0] == 0:        
           Twitter_Intr_pct_past7day = "N/A"
            
        if pdPast7dayDFsubset['Uniques_Twitter'][p7day_idx].values[0] != 0: 
           Twitter_Uniqs_twitter_pct_past7day= ( pdTodayDFsubset['Uniques_Twitter'][tdy_idx].values[0]  - pdPast7dayDFsubset['Uniques_Twitter'][p7day_idx].values[0] ) / pdPast7dayDFsubset['Uniques_Twitter'][p7day_idx].values[0]
        if pdPast7dayDFsubset['Uniques_Twitter'][p7day_idx].values[0] == 0: 
           Twitter_Uniqs_twitter_pct_past7day = "N/A"
            
        if pdPast7dayDFsubset['Interactions_Facebook'][p7day_idx].values[0] != 0: 
           Facebook_Intr_pct_past7day= (pdTodayDFsubset['Interactions_Facebook'][tdy_idx].values[0] - pdPast7dayDFsubset['Interactions_Facebook'][p7day_idx].values[0]) / pdPast7dayDFsubset['Interactions_Facebook'][p7day_idx].values[0]
        if pdPast7dayDFsubset['Interactions_Facebook'][p7day_idx].values[0] == 0:
           Facebook_Intr_pct_past7day = "N/A"
          

        ##Values under non-usage
        #Twitter_Uniq_Auth_pct_past7day= ( pdTodayDFsubset['Unique_Authors_Twitter'][tdy_idx].values[0]  - pdPast7dayDFsubset['Unique_Authors_Twitter'][p7day_idx].values[0] ) / pdPast7dayDFsubset['Unique_Authors_Twitter'][p7day_idx].values[0]
        #Twitter_Uniq_Eng_pct_past7day= ( pdTodayDFsubset['Unique_Engagers_Twitter'][tdy_idx].values[0]  - pdPast7dayDFsubset['Unique_Engagers_Twitter'][p7day_idx].values[0] ) / pdPast7dayDFsubset['Unique_Engagers_Twitter'][p7day_idx].values[0]
        #Instagram_Intr_pct_past7day= (pdTodayDFsubset['Instagram_Interactions_Of_Total'][tdy_idx].values[0] - pdPast7dayDFsubset['Instagram_Interactions_Of_Total'][p7day_idx].values[0]) 
        #Facebook_Uniq_Auth_pct_past7day= (pdTodayDFsubset['Unique_Authors_Facebook'][tdy_idx].values[0] - pdPast7dayDFsubset['Unique_Authors_Facebook'][p7day_idx].values[0])/ pdPast7dayDFsubset['Unique_Authors_Facebook'][p7day_idx].values[0]
        #Facebook_Uniq_Eng_pct_past7day= (pdTodayDFsubset['Unique_Engagers_Facebook'][tdy_idx].values[0] - pdPast7dayDFsubset['Unique_Engagers_Facebook'][p7day_idx].values[0])/ pdPast7dayDFsubset['Unique_Engagers_Facebook'][p7day_idx].values[0]
        
    elif (pdPast7dayDFsubset.empty==True):        
        
        #After User Eric feedback, added below lines to show as 'N/A' if this show is airing for 1st time and there is no previous week data to compare.
        ##Values under usage
        Total_Social_Intr_pct_past7day="N/A"
        Twitter_Intr_pct_past7day="N/A"
        Twitter_Uniqs_twitter_pct_past7day="N/A"
        Facebook_Intr_pct_past7day="N/A"
        SeasonAvg_Uniques_Twitter_pct_past7day="N/A"
        
        ##Values under non-usage
        #Twitter_Uniq_Auth_pct_past7day="N/A"
        #Instagram_Intr_pct_past7day="N/A"
        #Facebook_Uniq_Auth_pct_past7day="N/A"
        #Twitter_Uniq_Eng_pct_past7day="N/A"
        #Facebook_Uniq_Eng_pct_past7day="N/A"

    #CREATING EMAIL STATEMENTS LINE BY LINE. ## ChangeRequest Jul-23-2018, include Instagram totals as well and state the same in line 1 of email.
    email_line1_str="Social volume across Twitter, Instagram and Facebook drew {} social interactions ({} vs. Season 1 Premiere)".format(formatting('k',Total_Social_Intr) , formatting('p',Total_Social_Intr_pct_past7day)    ) 

    ##TWITTER EMAIL STATEMENTS  
    email_line2_str="{} interactions ({} vs S1 , {} vs season avg) and {} uniques twitter ({} vs S1 Premiere).".format(formatting('k',Total_Twitter_Intr),formatting('p',Twitter_Intr_pct_past7day),formatting('p',SeasonAvg_Uniques_Twitter_pct_past7day) ,formatting('k',Total_Twitter_Uniqs),formatting('p',Twitter_Uniqs_twitter_pct_past7day)  ) 

    ##INSTAGRAM EMAIL STATEMENTS
    if Total_Insta_Intr == 0:
       email_line3_str="Data not available." 
    else:
       email_line3_str="{} interactions ({} vs S1 Premiere).".format(formatting('k',Total_Insta_Intr),formatting('p',Instagram_Intr_pct_past7day)      ) 

    ##FACEBOOK EMAIL STATEMENTS   
    if (Total_Facebook_Intr == 0 ) :
       email_line4_str="Data not available.".format(formatting('k',Total_Facebook_Intr),formatting('p',Facebook_Intr_pct_past7day) )
    
    else:
       email_line4_str="{} interactions ({} vs S1 Premiere).".format(formatting('k',Total_Facebook_Intr),formatting('p',Facebook_Intr_pct_past7day) ) 
      
    ##NETWORK RANK EMAIL STATEMENTS
    email_line5_str="We ranked {} in Network ({}, Cable, Primetime, No Sports Events or Specials, Live or New) and {} in {} ({}, Cable, Primetime, No Sports Events or Specials, Live or New).".format( Today_Ntwrk_Rank,doa,Today_Pgm_Rank,rank_source,doa ) 
    
    ##METRICS SOURCE EMAIL STATEMENTS
    email_sourceline_str="""Source: Nielsen Social {}""".format(rank_source)
  




# COMMAND ----------



if __name__ == "__main__":
  #Widget Parameterised run 
  dbutils.widgets.text("adhoc_run", "N", "EmptyLabel")
  dbutils.widgets.text("adhoc_date", "9999-01-01", "EmptyLabel")
  
  
  adhoc_run=dbutils.widgets.get("adhoc_run")
  adhoc_date=dbutils.widgets.get("adhoc_date")
  #adhoc_run='Y'
  #adhoc_date="2018-07-18" #"2018-06-19" For Monday Run
  print ("Widget adhoc_run:{}".format(adhoc_run))
  print ("Widget adhoc_date:{}".format(adhoc_date))
  
  
  if adhoc_run == 'Y':
    ##Adhoc date
    run_date = datetime.strptime(adhoc_date, "%Y-%m-%d").date()-timedelta(days=1)  #datetime.strptime(adhoc_date_tmp, "%Y-%m-%d")
    run_past7Days = (run_date - timedelta(days=7) ).strftime("%Y-%m-%d")
    run_day = run_date.strftime("%A")

    print ('#############Adhoc dates##########')
    print ('{} run_date, {} adhoc_date  ,{} adhoc_past7Days ,run_day {}'.format(adhoc_date,run_date,run_past7Days,run_day))
    
  else:
    ##System date
    system_date = date.today() - timedelta(days=1)   
    system_past7Days = date.today() - timedelta(days=8)
    run_day = system_date.strftime("%A")
    run_date=system_date
    run_past7Days=system_past7Days    
    print ('#############System dates##########')
    print ('{} system_date ,{} system_past7Days,run_day {} '.format(system_date,system_past7Days,run_day))

  Program_Dl_Full_List = get_program_dl_sheet_info(run_day)
  
  msg_body_creation1(pgm_name)
  email_notification1(pgm_name,doa,episode_num_pgm_dict)  
  
