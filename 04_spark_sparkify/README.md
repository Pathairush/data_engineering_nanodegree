
# Datalake with Spark - Sparkify

## Propose of this database and analytics goal

 The purpose of this database is to enable Sparkifty and their analytics team to
    
   1. Develop a better understanding for their user behavior in various dimensions such as user, song, artist, and geo-location. Sparkify team can develop a customer segmentation based on the provided database.
   2. Sparkift can develop a propensity to subscribe model to identify who will likely to convert from free to paid level given the transaction timestamp as well as the date that customer convert from the free to paid level.
   3. The customer insight dashboard can be built upon the provided data. The example of metric is daily average number of user in the Sparkify platform. 
        
## Database Schema Design

The provided database consists of 1 fact table and 4 dimension tables as following.
Schema design concept - **STAR Schema**

### Fact Table
- **songplays** - records in log data associated with song plays i.e. records with page NextSong <br>
Columns  - `songplay_id`, `start_time`, `user_id`, `level, song_id`, `artist_id`, `session_id`, `location`, `user_agent`

### Dimension Tables
- **users** - users in the app <br>
Columns  - `user_id`, `first_name`, `last_name`, `gender`, `level`
- **songs** - songs in music database <br>
Columns  - `song_id`, `title`, `artist_id`, `year`, `duration`
- **artists** - artists in music database <br>
Columns  - `artist_id`, `name`, `location`, `latitude`, `longitude`
- **time** - timestamps of records in songplays broken down into specific units <br>
Columns  - `start_time`, `hour`, `day`, `week`, `month`, `year`, `weekday`

### ER Diagram
![figure](https://github.com/Pathairush/data_engineering/blob/master/01_postgresql_sparkify/Sparkify.png)

### How to run this project
Please input your `aws_access_key_id` and `aws_secret_key_access` in dl.cfg file.

Then, you can type the following command your terminal that already configured 
```
python etl.py
```
The script will run on the spark cluster configured with the provided `aws_access_key_id` and `aws_secret_key_access` in dl.cfg file.

The script will ingest data from S3 bucket `s3://udacity-dend.com/` . It will process the data and return the 5 metnioned above tables in database design schema section. You can change the output path of the created table in argument `output_data` in `etl.py` main function.
 
 ### Files explanation
 1. `dl.cfg` - config file containing your `aws_access_key_id` and `aws_secret_key_access` for running script in spark clsuter.
 2. `etl.py` - ETL job for ingesting data from `s3://udacity-dend.com/` and create the provided tables as desgined.