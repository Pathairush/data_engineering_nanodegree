from pyspark.sql import SparkSession
import datetime
import pyspark.sql.functions as F
import pyspark.sql.types as T
import logging
import os
import shutil

def extract_data(spark, input_file):
    
    '''
    extract sas data into spark dataframe
    '''
    
    df = spark.read.format('com.github.saurfang.sas.spark').load(input_file)
    
    # cast column type
    df = df.withColumn('cicid', F.col('cicid').cast(T.IntegerType()))
    df = df.withColumn('i94yr', F.col('i94yr').cast(T.IntegerType()))
    df = df.withColumn('i94mon', F.col('i94mon').cast(T.IntegerType()))
    df = df.withColumn('biryear', F.col('biryear').cast(T.IntegerType()))
    
    df = df.withColumn('load_data_timestamp', F.lit(datetime.datetime.now()))
    
    return df

def stage_data(spark, df ,output_file):
    
    '''
    write immigration data to staging_fact parquet file
    '''
    
    if os.path.exists(output_file):
        shutil.rmtree(output_file)

    # convert sas format to delta format
    df.write.format('delta').mode('overwrite').save(output_file)

def main():
    
    input_file = '/data/18-83510-I94-Data-2016/i94_may16_sub.sas7bdat'
    output_file = '/home/workspace/output/staging_fact'
    
    spark = SparkSession.builder\
    .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11,io.delta:delta-core_2.11:0.6.1")\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .enableHiveSupport().getOrCreate()
 
    df = extract_data(spark, input_file)
    stage_data(spark, df, output_file)
    
if __name__ == "__main__":
    
    main()