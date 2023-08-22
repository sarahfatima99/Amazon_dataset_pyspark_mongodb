import os
import pymongo
from dotenv import load_dotenv
from validate_data import validate_data
from datetime import datetime
from pymongo.errors import PyMongoError



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
        
        try:

            document = df.collect()[row_num].asDict()
            result = self.validate_row(document)        

            status = result['status']
            errors = result['errors']

            if status:

                collection = self.__db__["Amazon_reviews"]
                inserted_document = collection.insert_one(document)
                self.insert_into_audit_logs(id= document['asin'],
                                            status='Successfully Inserted',
                                            isTransformed=0,
                                            error_desc='')
            
            else:
                self.insert_into_audit_logs(id= document['asin'],
                                            status='error',
                                            isTransformed=0,
                                            error_desc=errors)    
                 
        except PyMongoError as e:
                        
            self.insert_into_audit_logs(id=document.get('asin', ''),
                                        status='error', isTransformed=0,
                                        error_desc=str(e))   

   
    def insert_bulk_into_collection(self,df):
        
        try:
            
            document = []
            df_list = df.collect()
                    
            for single_doc in df_list:
                single_dict = single_doc.asDict()
                result = self.validate_row(single_dict)
            
                status = result['status']
                errors = result['errors']  

                if status == True:
                    document.append(single_dict)               
                                    
                else:
                    self.insert_into_audit_logs(id= single_doc['asin'],
                                            status='error',
                                            isTransformed=0,
                                            error_desc=errors)  
            
            if len(document) != 0:      
                collection = self.__db__["Amazon_reviews"]    
                collection.insert_many(document)  
                             
        except PyMongoError as e:
            self.insert_into_audit_logs(id='', status='error', isTransformed=0, error_desc=str(e))
    

    def insert_into_audit_logs(self,id,status,isTransformed,error_desc):
        
        try:

            collection = self.__db__["Audit_logs"]
            audit_log_doc = {
            'asin':id,
            'status':status,
            'error_desc':error_desc,
            'isTransformed':isTransformed,
            'inserted_time':datetime.utcnow()

                }

            collection.insert_one(audit_log_doc)    
                
        except PyMongoError as e:
            print(f"Error inserting into Audit_logs: {str(e)}")

    def update_data(self,filter_column,filter_value,update_column,update_value):
        
        try:
            
            filter_criteria = {filter_column: filter_value}  
            update_values = {"$set": {update_column: update_value}} 

            collection = self.__db__["Amazon_reviews"] 
            result = collection.update_many(filter_criteria, update_values)
            
        except PyMongoError as e:
            print(f"Error updating the data: {str(e)}")



    def retrieve_data(self,filter_column,filter_value):
        
        try:
        
            collection = self.__db__["Amazon_reviews"] 
            results_cursor = collection.find({},{filter_column:  filter_value})
            print(results_cursor)
            results_list = list(results_cursor)
            print(results_list)
            
        except PyMongoError as e:
            print(f"Error retrieving the data: {str(e)}")

        

    def validate_row(self,document):
        
        vd = validate_data()
        result = vd.validate_all_checks(document)
        return result
       
 
       
        





    
    
