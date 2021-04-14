from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import logging
import datetime
import os
from helper import upsert_table, clean_column_name
import configparser

def transform_data(spark, input_file, cfg):
    
    '''
    transform data before writing to delta file
    '''
    
    staging_fact = spark.read.parquet(input_file)
    fact_columns = ['cicid', 'i94yr', 'i94mon', 'arrdate', 'depdate', 'i94mode', 'i94port', 'airline', 'fltno', 'i94visa', 'visatype', 'i94addr']
    fact = staging_fact.select(*fact_columns)

    immigration_rename_column = {
        'cicid' : 'cicid',
        'i94yr' : 'year',
        'i94mon' : 'month',
        'arrdate' : 'arrived_date',
        'depdate' : 'departured_date',
        'i94mode' : 'i94mode',
        'i94port' : 'i94port',
        'airline' : 'airline',
        'fltno' : 'flight_no',
        'i94visa' : 'i94visa',
        'visatype': 'visa_type',
        'i94addr' : 'i94addr'
    }

    fact = fact.toDF(*[immigration_rename_column[c] for c in fact.columns])

    map_port_code = spark.read.csv(os.path.join(cfg['PATH']['DEV'], 'output/mapping_data/port_code.csv'), header=True)
    map_mode_code = spark.read.csv(os.path.join(cfg['PATH']['DEV'], 'output/mapping_data/mode_code.csv'), header=True)
    map_visa_code = spark.read.csv(os.path.join(cfg['PATH']['DEV'], 'output/mapping_data/visa_code.csv'), header=True)

    fact = fact.join(F.broadcast(map_port_code), 'i94port', 'left')
    fact = fact.join(F.broadcast(map_mode_code), 'i94mode', 'left')
    fact = fact.join(F.broadcast(map_visa_code), 'i94visa', 'left')

    fact = fact.drop(*['i94visa','i94mode', 'i94port'])
    fact = fact.withColumnRenamed('i94addr', 'state_code')

    fact = fact.withColumn('base_sas_date', F.lit("1960-01-01"))
    fact = fact.withColumn('arrived_date', F.expr('date_add(base_sas_date, arrived_date)'))
    fact = fact.withColumn('departured_date', F.expr('date_add(base_sas_date, departured_date)'))
    fact = fact.drop('base_sas_date')

    fact = fact.withColumn('id', F.concat_ws('_', F.col('cicid'), F.col('year'), F.col('month')))
    fact = fact.withColumn('load_data_timestamp', F.lit(datetime.datetime.now()))

    fact = fact.select(*['id', 'cicid', 'year', 'month', 'arrived_date', 'departured_date',
                         'airline', 'flight_no', 'visa_type', 'immigration_port', 'transportation', 'visa_code', 'state_code', 'load_data_timestamp'])
    
    return fact

def main():
    
    cfg = configparser.ConfigParser()
    cfg.read('/Users/pathairs/Documents/projects/data_engineering/06_capstone_project/etl/config.cfg')

    input_file = os.path.join(cfg['PATH']['DEV'], cfg['OUTPUT_FILE']['STAGE_FACT'])
    output_file = os.path.join(cfg['PATH']['DEV'], cfg['OUTPUT_FILE']['FACT_TABLE'])
    
    spark = SparkSession.builder\
            .config("spark.jars.packages", cfg['SPARK_CONFIG']['JAR_PACKAGE'])\
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
            .enableHiveSupport().getOrCreate()
    
    df = transform_data(spark, input_file, cfg)
    upsert_table(spark, df, "source.id = update.id", output_file, partition_columns = ['year', 'month'])

if __name__ == "__main__":
    
    main()