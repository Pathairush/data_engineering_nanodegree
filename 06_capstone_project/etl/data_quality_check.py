from pyspark.sql import SparkSession
import os

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
    
    output_path = './output'
    files = ['staging_fact', 'fact_table', 'dim_user', 'dim_date', 'dim_state', 'dim_country']
    primary_keys = ['cicid', 'id', 'cicid', 'date', 'state_code', 'country']
    map_primary_key = {file:pk for file, pk in zip(files, primary_keys)}

    spark = SparkSession.builder\
        .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11,io.delta:delta-core_2.11:0.6.1")\
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
        .enableHiveSupport().getOrCreate()

    for file in files:

        table_path = os.path.join(output_path, file)
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