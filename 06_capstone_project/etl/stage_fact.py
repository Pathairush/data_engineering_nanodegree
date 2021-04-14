from pyspark.sql import SparkSession
import datetime
import pyspark.sql.functions as F
import pyspark.sql.types as T
import logging
import os
import shutil
import configparser

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
    df = df.withColumn('arrdate', F.col('arrdate').cast(T.IntegerType()))
    df = df.withColumn('depdate', F.col('depdate').cast(T.IntegerType()))
    
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

    cfg = configparser.ConfigParser()
    cfg.read('/Users/pathairs/Documents/projects/data_engineering/06_capstone_project/etl/config.cfg')
    
    input_file = os.path.join(cfg['PATH']['DEV'], cfg['DATA_FILE']['IMMIGRATION'])
    output_file = os.path.join(cfg['PATH']['DEV'], cfg['OUTPUT_FILE']['STAGE_FACT'])
    
    spark = SparkSession.builder\
    .config("spark.jars.packages", cfg['SPARK_CONFIG']['JAR_PACKAGE'])\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .enableHiveSupport().getOrCreate()
 
    df = extract_data(spark, input_file)
    stage_data(spark, df, output_file)
    
if __name__ == "__main__":
    
    main()