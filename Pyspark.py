# TASK:
# 1. import data from all 6 types of product
# 2. merge all data 
# 3. apply transformation to the dataset
# 4. insert into database

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, mean, count , max, min, lower
from pyspark.sql.types import IntegerType
from database import Database
from pyspark.sql import functions as F
import os


class pyspark:

    __root_path__ = os.path.realpath(os.path.join(os.path.dirname(__file__), '..'))
    __folder_path__ = __root_path__ +'\Amazon_dataset_pyspark_mongodb\datasets'
    spark =  None
    __db_obj__ = None
    __vd_obj__ = None

    def __init__(self):

        self.spark = SparkSession.builder.appName("DETask") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "4") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "200").getOrCreate() \
        
        self.__db_obj__ = Database()



    def load_data(self):

        file_path = self.__folder_path__ + '\\' +'AMAZON_FASHION_5.json'
        df = self.spark.read.json(file_path)
        df=df.withColumn('overall', col('overall').cast(IntegerType()))
        self.__db_obj__.insert_one_into_collection(df,row_num=1)

       

    def summariz_data(self,df):

        product_reviewer_summary=df.groupBy('asin','reviewerID').agg(
            round(mean("overall"),2).alias("Average_ratings")
            ,count('overall').alias('total_rating')
            )
       
        product_summary = df.groupBy('asin').agg(
            max('overall').alias('max_rating')
           ,min('overall').alias('min_rating')
           ,count('overall').alias('counts'))
            

    

    def transform_data(self,df):

        pass   





  

   