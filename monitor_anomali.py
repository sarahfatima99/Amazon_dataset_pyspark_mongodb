import pandas as pd
import numpy as np
import smtplib
from email.mime.text import MIMEText
from pyspark.sql.functions import col, round, stddev,mean
import os
from dotenv import load_dotenv
from email.mime.multipart import MIMEMultipart
import time


class anomali:

    sender_email = ''
    receiver_email=''
    subject = 'Data Quality Alert - Anomaly Detected'
    password = ''
    

    def __init__(self):

        load_dotenv()

        self.sender_email = os.getenv('EMAIL_ADDRESS')
        self.receiver_email = os.getenv('EMAIL_ADDRESS')
        self.password = os.getenv('PASSWORD')


    def send_alert(self,avg_anomali_prod,prod_stddev):

        if  (len(avg_anomali_prod) != 0):   
            body = 'Average ratings anomali detetcted in following items : \n'
            
            for items in avg_anomali_prod:
                for key, value in items.items():
                        body += f"{key}: {value}\n"
            
            body += '\n\n'

        if  (len(prod_stddev) != 0):   
            body = 'Product stdev ratings anomali detetcted in following items : \n\n'
            for items in prod_stddev:
                for key, value in items.items():
                    body += f"{key}: {value}\n"

        msg = MIMEMultipart()
        msg['From'] = self.sender_emails
        msg['To'] = self.receiver_email
        msg['Subject'] = self.subject
        msg.attach(MIMEText(body, 'plain'))

        server = smtplib.SMTP('smtp.gmail.com', 587)  
        server.starttls()
        server.login(self.sender_email, self.password) 
        server.sendmail(self.sender_email, self.receiver_email, msg.as_string())
        server.quit()

    def meansure_metric(self,df):

        mean_threshold = 4
        std_threshold = 1
        avg_anomali_prod = []
        std_dev_prod = []


        product_review=df.groupBy('asin').agg(
            round(mean("overall"),2).alias("Average_ratings"),
            round(stddev("overall"),2).alias('standard_dev')
            )
        product_review.show()

        df_length = df.count()
        

        for i in range(0,25):
         
            single_prod = product_review.collect()[i].asDict()
            prod_mean = single_prod['Average_ratings']
            prod_stddev = single_prod['standard_dev']
            
            if abs(prod_mean - mean_threshold) < mean_threshold * 0.1:
                avg_anomali_prod.append({'Prod_id':single_prod['asin'],'average':single_prod['Average_ratings']})

            if  prod_stddev is None or abs(prod_stddev - std_threshold) > std_threshold * 0.1 :
                std_dev_prod.append({'Prod_id':single_prod['asin'],'standard dev':single_prod['standard_dev']})
        
        if len(std_dev_prod) != 0 and len(avg_anomali_prod) != 0:
            self.send_alert(avg_anomali_prod,std_dev_prod)


       