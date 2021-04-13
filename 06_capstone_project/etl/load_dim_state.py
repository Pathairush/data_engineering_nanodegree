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
        
    us_demo = spark.read.csv(input_file, sep = ';', header=True)
    us_demo = clean_column_name(us_demo)

    index_columns = ['city', 'state', 'median_age', 'male_population', 'female_population', 'total_population',
                     'number_of_veterans', 'foreign_born', 'average_household_size', 'state_code']
    piv_us_demo = us_demo.groupby(*index_columns).pivot('race').agg(F.first('count').alias('count'))
    piv_us_demo = clean_column_name(piv_us_demo)

    dim_state = piv_us_demo.groupby('state_code', 'state').agg(
        F.expr('approx_percentile(median_age, .5)').alias('median_age'),
        F.sum('male_population').alias('male_population'),
        F.sum('female_population').alias('female_population'),
        F.sum('total_population').alias('total_population'),
        F.sum('number_of_veterans').alias('number_of_veterans'),
        F.sum('foreign_born').alias('foreign_born'),
        F.expr('approx_percentile(average_household_size, .5)').alias('median_household_size'),
        F.sum('american_indian_and_alaska_native').alias('american_indian_alaska_native'),
        F.sum('asian').alias('asian'),
        F.sum('black_or_african_american').alias('black_african_american'),
        F.sum('hispanic_or_latino').alias('hispanic_latino'),
        F.sum('white').alias('white')
    )
    arrange_column = ['state_code', 'state', 'median_age', 'male_population', 'female_population',
                      'total_population', 'number_of_veterans', 'foreign_born', 'median_household_size',
                      'american_indian_alaska_native', 'asian', 'black_african_american', 'hispanic_latino', 'white']
    dim_state = dim_state.select(*arrange_column)
    dim_state = dim_state.withColumn('load_data_timestamp', F.lit(datetime.datetime.now()))
    
    return dim_state

def main():
    
    input_file = '/home/workspace/data/us-cities-demographics.csv'
    output_file = '/home/workspace/output/dim_state'
    
    spark = SparkSession.builder\
    .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11,io.delta:delta-core_2.11:0.6.1")\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .enableHiveSupport().getOrCreate()

    df = transform_data(spark, input_file)
    upsert_table(spark, df, "source.state_code = update.state_code", output_file)

if __name__ == "__main__":
    
    main()