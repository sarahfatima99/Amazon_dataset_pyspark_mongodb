# TASK:
# 1. import data from all 6 types of product
# 2. merge all data 
# 3. apply transformation to the dataset
# 4. insert into database
# product_review.write.mode("overwrite").parquet('C:\sarah_fatima_parquet\product_review.parquet')

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, mean, count , max, min, regexp_replace,lower, stddev
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F
from monitor_anomali import anomali
from database import Database
import os


class pyspark:

    __root_path__ = os.path.realpath(os.path.join(os.path.dirname(__file__), '..'))
    __folder_path__ = os.path.join(__root_path__,'Amazon_dataset_pyspark_mongodb','datasets')
    spark =  None
    __db_obj__ = None
    __vd_obj__ = None
    __anomali_obj__ = None
    

    def __init__(self):

        try:
            
            self.spark = SparkSession.builder.appName("DETask") \
            .config("spark.executor.memory", "8g") \
            .config("spark.executor.cores", 8) \
            .config("spark.driver.memory", "4g") \
            .config("spark.sql.shuffle.partitions", "200")\
            .config("spark.memory.offHeap.enabled", "true") \
            .config("spark.memory.offHeap.size", "4g") \
            .getOrCreate()
            
      
            self.__db_obj__ = Database()
            self.__anomali_obj__ = anomali()
            
        except Exception as e:
            print(f"An error occurred during initialization: {str(e)}")



    def load_data(self):

        file_path = os.path.join(self.__folder_path__ ,'All_Beauty.json')
        df = self.spark.read.json(file_path)
               
        df = self.apply_transformation(df)
        prod_review =  self.summariz_data(df)
       
        self.__anomali_obj__.meansure_metric(prod_review)
       
        
        df.persist()
        self.__db_obj__.insert_bulk_into_collection(df)


    def summariz_data(self,df):
        
        product_reviewer_summary=df.groupBy('asin','reviewerID').agg(
            round(mean("overall"),2).alias("Average_ratings")
            ,count('overall').alias('total_rating')
            )

        product_review=df.groupBy('asin').agg(
            round(mean("overall"),2).alias("Average_ratings"),
            round(stddev("overall"),2).alias('standard_dev')
            )
       
        product_summary = df.groupBy('asin').agg(
            max('overall').alias('max_rating')
           ,min('overall').alias('min_rating')
           ,count('overall').alias('counts'))
        
        
        
        return product_review
       
       
    def apply_transformation(self,df):
        
        try:         
            df_transformed = (
            df.withColumn('overall', col("overall").cast(IntegerType()))
            .withColumn("reviewerName", lower(col("reviewerName")))
            .withColumn("reviewerName", regexp_replace(col("reviewerName"), r'[^\w\s]', ''))
            .withColumn("reviewText", regexp_replace(col("reviewText"), r'[^\w\s]', ''))
    )           
            
        except Exception as e:
            print(f"An error occurred during transformation: {str(e)}")

       
        return df_transformed
      






  

   