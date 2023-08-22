import pandas as pd
import numpy as np
import smtplib
from email.mime.text import MIMEText
import os
from dotenv import load_dotenv
from email.mime.multipart import MIMEMultipart


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
        msg['From'] = self.sender_email
        msg['To'] = self.receiver_email
        msg['Subject'] = self.subject
        msg.attach(MIMEText(body, 'plain'))

        server = smtplib.SMTP('smtp.gmail.com', 587)  
        server.starttls()
        server.login(self.sender_email, self.password) 
        server.sendmail(self.sender_email, self.receiver_email, msg.as_string())
        server.quit()

    
    def meansure_metric(self,product_review):

        mean_threshold = 1
        std_threshold = 4
        avg_anomali_prod = []
        std_dev_prod = []
        reviews = product_review.collect()

        for review in reviews:        
          
            prod_mean = review['Average_ratings']
            prod_stddev = review['standard_dev']
            
            if abs(prod_mean - mean_threshold) < mean_threshold * 0.1:
                avg_anomali_prod.append({'Prod_id':review['asin'],'average':review['Average_ratings']})

            if  prod_stddev is None or abs(prod_stddev - std_threshold) > std_threshold * 0.1 :
                std_dev_prod.append({'Prod_id':review['asin'],'standard dev':review['standard_dev']})
        
        if len(std_dev_prod) != 0 and len(avg_anomali_prod) != 0:
            self.send_alert(avg_anomali_prod,std_dev_prod)

