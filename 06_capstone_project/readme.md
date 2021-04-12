# US Immigration capstone project

## Introduction 

Hi, everyone. Welcome to my data engineering nanodegree capstone project.  This project aims to give me an opportunity to utilize what I learnt from the nanodegree. In short, we will build a data pipeline for the `I94 immigration` data and combine them with several useful other data souces such as `world temperature` and `US city demographic` data. This project aims to show you about the end-to-end process of building a data pipeline from several data sources.

## Benefits ?

Many interested business questions can be answered with this dataset. Here are examples

1. What is the current state of immigrant people in US ? 
2. What is changes of immigration pattern compared between different time period (80s, 90s, ..., 2020) ? 
3. What factors are correlated to the immigration pattern ? 
4. What is the number of immigration people in the next following month ?

Those answers can help US immigration department to design a better policy to take care of the immigrant people. Also, not only from data analytics and business side, you will find the data pipeline architecture and ETL code for running this project schedually. This will be a good start for anyone who are looking for where to start thier data engineer journey.

## Data

1. `I94 immigration` - Dataset contains information about the Arrival-Departure Record Card, is a form used by U.S. Customs and Border Protection (CBP) intended to keep track of the arrival and departure to/from the United States of people who are not United States citizens or lawful permanent residents. The data in this project is only from year 2016 [more detail](https://travel.trade.gov/research/reports/i94/historical/2016.html)
2. `World Temperature` - Dataset contains the global land temperature by city starting from year 1750 till 2015. [more detail](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data)
3. `US City demographic` - Dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000. Data comes from the US Census Bureau's 2015 American Community Survey. [more detail](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/information/)

## Foundings 

We explore data in many different ways, here are what you need to know why we came up with the following data model design and schema. 

### I94 Immigration
- `visapost`, `occup`, `entdepu`, `insnum` columns have a high missing value percentage ( > 50 % of the total column in 1 month), thus we decide to drop those columns.
- `admnum`, `dtaddto`, `matflag`, `entdepa`, `entdepd`, `dtadfile`, `count` seem not have a meaningful value for further understanding of the data, drop those columns as well.
- `i94bir`, `biryear` have the same information so we keep only one `biryear`.

###  World temperature
- For world temperature data, because we only have `country` level of data in fact table,  we decide to aggregate the world temperature data into `country` level so that we can join it with `born_country`, `residence_country` columns.

### US demographic
- For US demographics dataset, we saw that the data is in the count number of each race level. So, we decided to aggregate them to `state_code` level so that we can join it with the fact table `state_code`.

### General 
- For null value, we decide to leave it as it is if it's not a primary or foreign key.

## Data model

From the foundings in previous section, here are the data model that we decide to apply for this project.

![img](https://github.com/Pathairush/data_engineering/blob/master/06_capstone_project/image/capstone_dbdiagram.png)

## Data pipeline




###  Create data model

PUT AIRFLOW DIAGRAM HERE

In each airflow task, we provide a `python`  script to ingest, transform and load data into delta format.
For more detail about the ETL script, you can see the code in `etl` folder.

### Data quality check

### Data dictionary

### Summary

***Clearly state the rationale for the choice of tools and technologies for the project.***

#### Technology

In any data pipeline, there will be 3 main components that we need to selectively choose for building the whole project. `storage format`, `computation engine`, and `orchestrator`. There are a lot of tools and technology out there, but here are what I decided to use in this capstone project.

#### [Delta Lake](https://delta.io/) `storage format`
![img](https://github.com/Pathairush/data_engineering/blob/master/06_capstone_project/image/delta-lake-logo.png)

Delta Lake  is an  [open source storage layer](https://github.com/delta-io/delta)  that brings reliability to  [data lakes](https://databricks.com/discover/data-lakes/introduction). Delta Lake provides ACID transactions, scalable metadata handling, and unifies streaming and batch data processing. Delta Lake runs on top of your existing data lake and is fully compatible with Apache Spark APIs.

In short, delta lake is an unpdated version of parquet format. The development team brings many useful features to fix the problem of storaing data in NoSQL format. For example, I decided to use the delta lake format in this project becasue it provides the `UPSERT` ability compared to parquet that you have to code it by yourself. It helps heavy-lifting unnecessary thing and help you focusing on only the data. Also there are other useful features such as ACID transaction and metadata handling.

#### [Apache Spark](https://spark.apache.org/docs/2.4.3/)  `computation engine`
![img](https://github.com/Pathairush/data_engineering/blob/master/06_capstone_project/image/spark_logo.png)

Apache Spark is a fast and general-purpose cluster computing system. It provides high-level APIs in Java, Scala, Python and R, and an optimized engine that supports general execution graphs. It also supports a rich set of higher-level tools including [Spark SQL](https://spark.apache.org/docs/2.4.3/sql-programming-guide.html) for SQL and structured data processing, [MLlib](https://spark.apache.org/docs/2.4.3/ml-guide.html) for machine learning, [GraphX](https://spark.apache.org/docs/2.4.3/graphx-programming-guide.html) for graph processing, and [Spark Streaming](https://spark.apache.org/docs/2.4.3/streaming-programming-guide.html).

#### [Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/)  `orchestrator`
![img](https://github.com/Pathairush/data_engineering/blob/master/06_capstone_project/image/airflow_logo.png)

Airflow is a platform to programmatically author, schedule and monitor workflows.

Use Airflow to author workflows as Directed Acyclic Graphs (DAGs) of tasks. The Airflow scheduler executes your tasks on an array of workers while following the specified dependencies. Rich command line utilities make performing complex surgeries on DAGs a snap. The rich user interface makes it easy to visualize pipelines running in production, monitor progress, and troubleshoot issues when needed.

***Propose how often the data should be updated and why.***

`I94 immigration` data usually update in the monthly basis. So, our data pipeline should be aligned with this schedule. There is no point to run the data pipeline every day without a new data came in. Other dimensional tables are one times (`US demographic`), or monthly (`World temperature`) updated as well. 


**Write a description of how you would approach the problem differently under the following scenarios:**

   -  The data was increased by 100x.
	   - Because  we leverage the power of spark. there is no need to worry about the scaling size of underlying computation engine. In case, we reach the limit we can incerease the cluster size that spark is running on. Spark also works in a distributed way, so horizontal scaling is always an option to go for.
	   
   -  The data populates a dashboard that must be updated on a daily basis by 7am every day.
	   - We can meet this requirement with the SLA option provided by Airflow. This will gaurantee that the data should be populated before 7 AM every day. In case your task failed, you can fix the problem by shifting the start ETL time earlier or increase the computation power of spark.

   -  The database needed to be accessed by 100+ people.
	   - We can store the data in any data warehouse options, and let the people accessed our data. The underlying data format can still be a `delta` format.

