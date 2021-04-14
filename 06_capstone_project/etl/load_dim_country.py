from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import logging
import datetime
import os
from helper import upsert_table, clean_column_name
import configparser

def transform_data(spark, input_file):
    
    '''
    transform data before writing to delta file
    '''
        
    temp = spark.read.csv(input_file, header=True)
    temp = clean_column_name(temp)
    dim_country = temp.groupby('country').agg(F.mean('averagetemperature').alias('avg_temp'),
                                                   F.first('latitude').alias('latitude'),
                                                   F.first('longitude').alias('longitude'))
    dim_country = dim_country.withColumn('country', F.lower(F.col('country')))
    dim_country = dim_country.withColumn('load_data_timestamp', F.lit(datetime.datetime.now()))
    
    return dim_country

def main():
    
    cfg = configparser.ConfigParser()
    cfg.read('/Users/pathairs/Documents/projects/data_engineering/06_capstone_project/etl/config.cfg')

    input_file = os.path.join(cfg['PATH']['DEV'], cfg['DATA_FILE']['GLOBAL_TEMPERATURE'])
    output_file = os.path.join(cfg['PATH']['DEV'], cfg['OUTPUT_FILE']['DIM_COUNTRY'])

    spark = SparkSession.builder\
            .config("spark.jars.packages", cfg['SPARK_CONFIG']['JAR_PACKAGE'])\
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
            .enableHiveSupport().getOrCreate()

    df = transform_data(spark, input_file)
    upsert_table(spark, df, "source.country = update.country", output_file)

if __name__ == "__main__":
    
    main()