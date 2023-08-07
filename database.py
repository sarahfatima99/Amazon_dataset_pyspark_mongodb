import pymongo
import os
from dotenv import load_dotenv
from validate_data import validate_data
from datetime import datetime


class Database:

    __client__ = None
    __db__ = None

    def __init__(self):

        load_dotenv()
        self.db_server=os.getenv('DATABASE')        
        
        if self.__client__ !=  None:            
            raise Exception("Cannot create multiple instances of Database class. Use getInstance() method instead.")
        
        self.__client__ = pymongo.MongoClient(self.db_server)
        self.__db__ = self.__client__['DE_assessment']  


    def get_instance(self):

        if self.__instance == None:

            print('no instance found')
            Database()

        return self.__client__
    

    def insert_one_into_collection(self,df,row_num):

        document = df.collect()[row_num].asDict()
        result = self.validate_row(document)
        

        status = result['status']
        errors = result['errors']

        if status == True:

            collection = self.__db__["Amazon_reviews"]
            inserted_document = collection.insert_one(document)
            self.insert_into_audit_logs(id= document['asin'],status='Successfully Inserted',isTransformed=0,error_desc='')
        
        else:
            self.insert_into_audit_logs(id= document['asin'],status='error',isTransformed=0,error_desc=errors)      
            
           

    def insert_into_audit_logs(self,id,status,isTransformed,error_desc):

        collection = self.__db__["Audit_logs"]
        audit_log_doc = {
        'object_id':id,
        'status':status,
        'error_desc':error_desc,
        'isTransformed':isTransformed,
        'inserted_time':datetime.utcnow()

            }
        
        collection.insert_one(audit_log_doc)



    def insert_bulk_into_collection(self,df,start,stop):
        
        document = []

        for i in range(start,stop+1):

            single_doc = df.collect()[i].asDict()  
            result = self.validate_row(single_doc)
            status = result['status']
            errors = result['errors']  

            if status == True:
                document.append(single_doc)
                collection = self.__db__["Amazon_reviews"]
                inserted_document = collection.insert_many(document)

            else:
                pass       
        
        
        


    def update_data(self,filter_column,filter_value,update_column,update_value):

        filter_criteria = {filter_column: filter_value}  
        update_values = {"$set": {update_column: update_value}} 

        collection = self.__db__["Amazon_reviews"] 
        result = collection.update_many(filter_criteria, update_values)


    def retrieve_data(self,filter_column,filter_value):
        
        query = {filter_column: {"$gte": filter_value}}  
        collection = self.__db__["Amazon_reviews"] 
        results_cursor = collection.find(query)
        results_list = list(results_cursor)

        for document in results_list:
              print(document)

    def validate_row(self,document):
        
        vd = validate_data(document)
        result = vd.validate_all_checks()
        return result
       
 
       
        





    
    

    

