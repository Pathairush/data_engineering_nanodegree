from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import logging
import datetime
import os
from helper import upsert_table, clean_column_name

def transform_data(spark, input_file):
    
    '''
    transform data before writing to delta file
    '''
    
    fact = spark.read.format('delta').load(input_file)
    dim_date = fact.select(F.col('arrived_date').alias('date'))\
    .unionByName(
        fact.select(F.col('departured_date').alias('date'))
    ).distinct()
    
    dim_date = dim_date.where('date IS NOT NULL')
    
    dim_date = dim_date.withColumn('year', F.year('date'))
    dim_date = dim_date.withColumn('month', F.month('date'))
    dim_date = dim_date.withColumn('day', F.dayofmonth('date'))
    dim_date = dim_date.withColumn('week_of_year', F.weekofyear('date'))
    dim_date = dim_date.withColumn('day_of_week', F.dayofweek('date'))
    dim_date = dim_date.withColumn('load_data_timestamp', F.lit(datetime.datetime.now()))
    
    return dim_date

def main():
    
    input_file = "/home/workspace/output/fact_table"
    output_file = '/home/workspace/output/dim_date'
    
    spark = SparkSession.builder\
    .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11,io.delta:delta-core_2.11:0.6.1")\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .enableHiveSupport().getOrCreate()

    df = transform_data(spark, input_file)
    upsert_table(spark, df, "source.date = update.date", output_file)

if __name__ == "__main__":
    
    main()