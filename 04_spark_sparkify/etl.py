import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, LongType, TimestampType, DateType, DoubleType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
    Create spark session with hadoop-aws jar
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """
    Read song data from json files
    Insert data to songs_table and artists_table
    Write table to parquet files with suitable partitioned columns
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')

    # create song_schema
    song_schema = StructType([
        StructField('artist_id', StringType(), False),
        StructField('artist_latitude', DecimalType(11,8), True),
        StructField('artist_longitude', DecimalType(11,8), True),
        StructField('artist_location', StringType(), True),
        StructField('artist_name', StringType(), True),
        StructField('duration', DecimalType(11,8), True),
        StructField('num_songs', IntegerType(), True),
        StructField('song_id', StringType(), False),
        StructField('title', StringType(), True),
        StructField('year', IntegerType(), True),
    ])

    # read song data file
    df = spark.read.json(song_data, schema=song_schema)


    # extract columns to create songs table
    songs_table = df.select('song_id','title','artist_id','year','duration')

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(os.path.join(output_data,'songs_table'), partitionBy=['year','artist_id'], compression='snappy', mode='overwrite')

    # extract columns to create artists table
    artists_table = df.select('artist_id',
                              col('artist_name').alias('name'),
                              col('artist_location').alias('location'),
                              col('artist_latitude').alias('latitude'),
                              col('artist_longitude').alias('longitude'),
                             )

    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists_table'), compression='snappy', mode='overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Read log data from json files
    Insert data to users_table, time_table, and songplays_table
    Write table to parquet files with suitable partitioned columns
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # create schema
    artists_schema = StructType([
        StructField('artist', StringType(), True),
        StructField('auth', StringType(), True),
        StructField('firstName', StringType(), True),
        StructField('gender', StringType(), True),
        StructField('itemInSession', LongType(), True),
        StructField('lastName', StringType(), True),
        StructField('length', DecimalType(11,8), True),
        StructField('level', StringType(), True),
        StructField('location', StringType(), True),
        StructField('method', StringType(), True),
        StructField('page', StringType(), True),
        StructField('registration', DoubleType(), True),
        StructField('sessionId', LongType(), True),
        StructField('song', StringType(), True),
        StructField('status', LongType(), True),
        StructField('ts', LongType(), False),
        StructField('userAgent', StringType(), True),
        StructField('userId', StringType(), False)
    ])

    # read log data file
    df = spark.read.json(log_data, schema=artists_schema)

    # filter by actions for song plays
    df = df.where('page = "NextSong"')

    # extract columns for users table    
    users_table = df.select(
        F.col('userId').alias('user_id'),
        F.col('firstName').alias('first_name'),
        F.col('lastName').alias('last_name'),
        F.col('gender'),
        F.col('level')
    )
    
    # handle duplicated value
    users_table = users_table.drop_duplicates(subset=['user_id'])

    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data,'users_table'), compression='snappy', mode='overwrite')

    # create timestamp column from original timestamp column
    df = df.withColumn('timestamp', F.from_unixtime(col('ts')/1000).alias('ts').cast(TimestampType()))

    # create datetime column from original timestamp column
    df = df.withColumn('datetime', F.from_unixtime(col('ts')/1000).alias('ts').cast(DateType()))

    # extract columns to create time table
    time_table = df.select(
        F.col('timestamp').alias('start_time'),
        F.hour('timestamp').alias('hour'),
        F.dayofyear('timestamp').alias('day'),
        F.weekofyear('timestamp').alias('week'),
        F.month('timestamp').alias('month'),
        F.year('timestamp').alias('year'),
        F.dayofweek('timestamp').alias('weekday')
    ).distinct()

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data,'time_table'), partitionBy=['year','month'], compression='snappy', mode='overwrite')

    # read in song data to use for songplays table
    song_schema = StructType([
        StructField('artist_id', StringType(), False),
        StructField('artist_latitude', DecimalType(11,8), True),
        StructField('artist_longitude', DecimalType(11,8), True),
        StructField('artist_location', StringType(), True),
        StructField('artist_name', StringType(), True),
        StructField('duration', DecimalType(11,8), True),
        StructField('num_songs', IntegerType(), True),
        StructField('song_id', StringType(), False),
        StructField('title', StringType(), True),
        StructField('year', IntegerType(), True),
    ])

    # read song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    song_df = spark.read.json(song_data, schema=song_schema)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df\
    .join(song_df, df.song==song_df.title)\
    .join(time_table, df.timestamp==time_table.start_time)\
    .select(
        F.col('timestamp').alias('start_time'),
        F.col('userId').alias('user_id'),
        F.col('level'),
        F.col('song_id'),
        F.col('artist_id'),
        F.col('sessionId').alias('session_id'),
        F.col('location'),
        F.col('userAgent').alias('user_agent'),
        time_table.year,
        F.col('month')
    )
    
    # create songplay_id
    songplays_table = songplays_table.withColumn('songplay_id', F.monotonically_increasing_id())
    songplays_table = songplays_table.select(['year','month','songplay_id','start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent'])

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data,'songplays_table'), partitionBy=['year','month'], compression='snappy', mode='overwrite')

def main():
    """
    Run Datalake with spark - Spakify project
    Read data from `udacity-dend` bucket
    Write data to `psbucket-190238091238901` bucket (my personal bucket)
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://psbucket-190238091238901/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    
    spark.stop();

if __name__ == "__main__":
    main()
