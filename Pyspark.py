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
        self.__db_obj__.insert_one_into_collection(df,3)

        # self.__db_obj__.insert_bulk_into_collection(df,start,stop)


        # self.partition_data(df)

        # single_row = df.limit(1).collect()[0].asDict()
        # print(single_row)
        # print(df.limit(5).collect()[0].asDict())
        # self.__db_obj__.insert_one(single_row)
   
        # insert_single_row(single_row)
        # self.summariz_data(df)

    
    def partition_data(self,df):
        df = df.withColumn('partition_num', F.spark_partition_id())
        df.persist()
        total_partition = [int(row.partition_num) for row 
                           in df.select('partition_num').distinct().collect()]
        print(total_partition)
        

        # total_partition = [int(row.partition_num) for row in 
        # 

        # for each_df in total_partition:
        #     sample_dict[each_df] = df.where(df.partition_num == each_df) 
        # pass


    def transform_data(self,df):

        pass   

    def summariz_data(self,df):

        product_reviewer_summary=df.groupBy('asin','reviewerID').agg(
            round(mean("overall"),2).alias("Average_ratings")
            ,count('overall').alias('total_rating')
            )
       
        product_summary = df.groupBy('asin').agg(
            max('overall').alias('max_rating')
           ,min('overall').alias('min_rating')
           ,count('overall').alias('counts'))
            

        # product_reviewer_summary.write.parquet('product_reviewer_summary.parquet')
        # product_summary.write.parquet('product_summary.parquet')
        


        





  

   