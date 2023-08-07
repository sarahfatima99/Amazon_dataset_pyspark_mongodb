import pymongo
import os
from dotenv import load_dotenv


class Database:

    __root_path__ = os.path.realpath(os.path.join(os.path.dirname(__file__), '..'))
    __config_path__ = __root_path__ +'\Amazon_dataset_pyspark_mongodb\config.ini'
    db_server = None
    __client = None
    __db__ = None

    def __init__(self):

        load_dotenv()
        self.db_server=os.getenv('DATABASE')        
        
        if self.__client !=  None:            
            raise Exception("Cannot create multiple instances of Database class. Use getInstance() method instead.")
        
        self.__client = pymongo.MongoClient(self.db_server)
        self.__db__ = self.__client['DE_assessment']  


    def get_instance(self):

        if self.__instance == None:

            print('no instance found')
            Database()

        return self.__client
    

    def insert_one_into_collection(self,df,row_num):

        document = df.collect()[row_num].asDict()
        collection = self.__db__["Amazon_reviews"]
        inserted_document = collection.insert_one(document)
        print("Inserted document ID:", inserted_document.inserted_id)


    def insert_bulk_into_collection(self,df,start,stop):
        document = []

        for i in range(start,stop+1):
            document.append(df.collect()[i])
        print(document)

        
        # collection = self.__db__["Amazon_reviews"]
        # inserted_document = collection.insert_many(document)
        # print("Inserted document ID:", inserted_document.inserted_id)





    
    

    

