from pyspark.sql import SparkSession
import os
import configparser

def check_number_of_row_more_than_zero(sdf):
    '''
    check that number of row in spark dataframe more than 0
    '''
    return sdf.count() > 0

def check_primary_key_is_not_null(sdf, pk):
    '''
    check that primary key in spark dataframe is not null
    '''
    return sdf.where(f'{pk} is NULL').count() == 0

def check_primary_key_is_unique(sdf, pk):
    '''
    check that primary key in spark dataframe is not null
    '''
    return sdf.select(pk).distinct().count() == sdf.count()

def main():

    cfg = configparser.ConfigParser()
    cfg.read('/Users/pathairs/Documents/projects/data_engineering/06_capstone_project/etl/config.cfg')
    
    files = [cfg['OUTPUT_FILE']['FACT_TABLE'], cfg['OUTPUT_FILE']['DIM_USER'], cfg['OUTPUT_FILE']['DIM_DATE'], cfg['OUTPUT_FILE']['DIM_STATE'], cfg['OUTPUT_FILE']['DIM_COUNTRY']]
    primary_keys = ['id', 'cicid', 'date', 'state_code', 'country']
    map_primary_key = {file:pk for file, pk in zip(files, primary_keys)}

    spark = SparkSession.builder\
            .config("spark.jars.packages", cfg['SPARK_CONFIG']['JAR_PACKAGE'])\
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
            .enableHiveSupport().getOrCreate()

    for file in files:

        table_path = os.path.join(cfg['PATH']['DEV'], file)
        sdf = spark.read.format('delta').load(table_path)

        if not check_number_of_row_more_than_zero(sdf):
            raise ValueError(f"DQ CHECK FAILED - {file}, check_number_of_row_more_than_zero")

        if not check_primary_key_is_not_null(sdf, map_primary_key[file]):
            raise ValueError(f"DQ CHECK FAILED - {file}, check_primary_key_is_not_null")

        if not check_primary_key_is_unique(sdf, map_primary_key[file]):
            raise ValueError(f"DQ CHECK FAILED - {file}, check_primary_key_is_unique")

    print('DQ CHECK PASSED')

if __name__ == "__main__":
    
    main()