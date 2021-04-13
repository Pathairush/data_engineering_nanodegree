from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import logging
import datetime
import os
from helper import upsert_table

def transform_data(spark, input_file):
    
    '''
    transform data before writing to delta file
    '''
        
    staging_fact = spark.read.parquet(input_file)
    dim_user_columns = ['cicid', 'i94yr', 'i94mon', 'biryear', 'gender', 'i94cit', 'i94res']
    dim_user = staging_fact.select(*dim_user_columns)
    user_rename_column = {
        'cicid' : 'cicid',
        'i94yr' : 'year',
        'i94mon' : 'month',
        'biryear' : 'birth_year',
        'gender' : 'gender',
        'i94cit' : 'i94cit',
        'i94res' : 'i94res'
    }

    dim_user = dim_user.toDF(*[user_rename_column[c] for c in dim_user.columns])
    for c in ['i94cit', 'i94res']:
        dim_user = dim_user.withColumn(c, F.substring(F.col(c).cast(T.StringType()), 1, 3))

    map_country_code = spark.read.csv('/home/workspace/output/mapping_data/city_code.csv', header=True)
    map_residence_code = spark.read.csv('/home/workspace/output/mapping_data/residence_code.csv', header=True)

    dim_user = dim_user.join(map_country_code, 'i94cit', 'left')
    dim_user = dim_user.join(map_residence_code, 'i94res', 'left')
    dim_user = dim_user.drop(*['i94res', 'i94cit'])

    dim_user = dim_user.withColumn('born_country', F.lower(F.col('born_country')))
    dim_user = dim_user.withColumn('residence_country', F.lower(F.col('residence_country')))
    dim_user = dim_user.withColumn('load_data_timestamp', F.lit(datetime.datetime.now()))
    
    return dim_user

def main():
    
    input_file = '/home/workspace/output/staging_fact'
    output_file = '/home/workspace/output/dim_user'
    
    spark = SparkSession.builder\
    .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11,io.delta:delta-core_2.11:0.6.1")\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .config("spark.sql.autoBroadcastJoinThreshold", -1)\
    .enableHiveSupport().getOrCreate()

    df = transform_data(spark, input_file)
    upsert_table(spark, df, "source.cicid = update.cicid", output_file, partition_columns = ['year', 'month'])

if __name__ == "__main__":
    
    main()