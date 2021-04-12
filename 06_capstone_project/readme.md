# US Immigration capstone project

## Introduction 

Hi, everyone. Welcome to my data engineering nanodegree capstone project.  This project aims to allow me to utilize what I learned from the nanodegree. In short, we will build a data pipeline for the `I94 immigration` data and combine them with several useful other data sources such as `world temperature` and `US city demographic` data. This project aims to show you the end-to-end process of building a data pipeline from several data sources.

## Benefits

We can answer many exciting business questions with this dataset. Here are examples

1. What is the current state of immigrant people in the US? 
2. What are immigration patterns compared between different times (the 80s, 90s, ..., 2020)? 
3. What factors correlate to the immigration pattern? 
4. What is the number of immigration people in the next following month?

Those answers can help US immigration department to design a better policy to take care of the immigrant people. Also, not only from the data analytics and business side, you will find the data pipeline architecture and ETL code for running this project. This project will be a good start for anyone looking for where to start their data engineer journey.

## Data

1. `I94 immigration` - Dataset contains information about the Arrival-Departure Record Card, is a form used by US Customs and Border Protection (CBP) intended to keep track of the arrival and departure to/from the United States of people who are not United States citizens or lawful permanent residents. The data in this project is only from the year 2016 [more detail](https://travel.trade.gov/research/reports/i94/historical/2016.html)
2. `World Temperature` - Dataset contains the city's global land temperature from the year 1750 till 2015. [more detail](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data)
3. `US City demographic` - Dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000. Data comes from the US Census Bureau's 2015 American Community Survey. [more detail](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/information/)

## Foundings 

We explore data in many different ways. Here are what you need to know why we came up with the following data model design and schema. 

### I94 Immigration
- `visapost`, `occup`, `entdepu`, `insnum` columns have a high missing value percentage ( > 50 % of the total column in 1 month). Thus we decide to drop those columns.
- `admnum`, `dtaddto`, `matflag`, `entdepa`, `entdepd`, `dtadfile`, `count` seem not have a meaningful value for further understanding of the data, drop those columns as well.
- `i94bir`, `biryear` have the same information, so we keep only one `biryear`.

###  World temperature
- For world temperature data, because we only have the `country` level of data in the fact table,  we decide to aggregate the world temperature data into `country` level to join it with `born_country`, `residence_country` columns.

### US demographic
For the US demographics dataset, we saw that the data is in each race level's count number. So, we decided to aggregate them to the `state_code` level to join it with the fact table `state_code`.

### General 
- For the null value, we decide to leave it as it is if it's not a primary or foreign key.

## Data model

From the previous section's findings, here is the data model that we decide to apply for this project.

![img](https://github.com/Pathairush/data_engineering/blob/master/06_capstone_project/image/capstone_dbdiagram.png)

## Data pipeline

###  Create a data model

In each airflow task, we provide a `python`  script to ingest, transform and load data into delta format.
For more detail about the ETL script, you can see the code in the `etl` folder. You can find the running ETL step in `capstone_project.ipynb` file.

### Data quality check

### Data dictionary

## Fact table

| Column name         | Description                                       |
|---------------------|---------------------------------------------------|
| id                  | The unique id of I94 form transaction.            |
| cicid               | Id of I94 form                                    |
| year                | Form submitted year                               |
| month               | Form submitted month                              |
| arrived_date        | Arrived date in USA                               |
| departured_date     | Departure date from USA                           |
| airline             | Airline used to arrive in US                      |
| flight_no           | Flight number                                     |
| visa_type           | VISA type                                         |
| immigration_port    | Port number                                       |
| transportation      | Transportation way (Air, Sea, Land, Not reported) |
| visa_code           | VISA code (Business, Pleasure, Student)           |
| state_code          | Arrival state                                     |
| load_data_timestamp | Timestamp when loaded the data                    |

## dim_user

| Column name         | Description                    |
|---------------------|--------------------------------|
| cicid               | Id of I94 form                 |
| year                | Form submitted year            |
| month               | Form submitted month           |
| birth_year          | Respondent's birth year        |
| gender              | Respondent's gender            |
| born_country        | Respondent's born country      |
| residence_country   | Respondent's residence country |
| load_data_timestamp | Timestamp when loaded the data |

## dim_state

| Column name                   | Description                                              |
|-------------------------------|----------------------------------------------------------|
| state_code                    | state code                                               |
| state                         | state name                                               |
| median_age                    | median age in that state                                 |
| male_population               | number of male population                                |
| female_population             | number of female population                              |
| total_population              | number of total population                               |
| number_of_vaterans            | number of vaterans                                       |
| foreign_born                  | number of foreign born                                   |
| median_household_size         | median household size in that state                      |
| american_indian_alaska_native | number of american, indian, alaska, or native population |
| asian                         | number of asian population                               |
| black_african_american        | number of black, african population                      |
| hispanic_latino               | number of hispanic or latino population                  |
| white                         | number of white population                               |
| load_data_timestamp           | Timestamp when loaded the data                           |

## dim_date

| Column name         | Description                    |
|---------------------|--------------------------------|
| date                | date in format YYYY-mm-dd      |
| year                | year                           |
| month               | month                          |
| day                 | number of day in month         |
| week_of_year        | number of week of year         |
| day_of_week         | number of day of week          |
| load_data_timestamp | Timestamp when loaded the data |

## dim_country

| Column name         | Description                    |
|---------------------|--------------------------------|
| country             | country name                   |
| avg_temp            | average land temperature       |
| latitude            | latitude                       |
| longitude           | longitude                      |
| load_data_timestamp | Timestamp when loaded the data |


### Summary

***Clearly state the rationale for the choice of tools and technologies for the project.***

#### Technology

There will be three main components in any data pipeline that we need to selectively choose for building the whole project. `storage format`, `computation engine`, and `orchestrator`. There are many tools and technology out there, but here is what I decided to use in this capstone project.

#### [Delta Lake](https://delta.io/) `storage format`
![img](https://github.com/Pathairush/data_engineering/blob/master/06_capstone_project/image/delta-lake-logo.png)

Delta Lake is an  [open source storage layer](https://github.com/delta-io/delta)  that brings reliability to  [data lakes](https://databricks.com/discover/data-lakes/introduction). Delta Lake provides ACID transactions, scalable metadata handling, and unifies streaming and batch data processing. Delta Lake runs on top of your existing data lake and is fully compatible with Apache Spark APIs.

In short, delta lake is an updated version of parquet format. The development team brings many valuable features to fix the problem of storing data in NoSQL format. For example, I decided to use the delta lake format in this project because it provides the `UPSERT` ability compared to parquet that you have to code by yourself. It helps heavy-lifting unnecessary thing and help you focus on only the data. Also, there are other valuable features such as ACID transactions and metadata handling.

#### [Apache Spark](https://spark.apache.org/docs/2.4.3/)  `computation engine`
![img](https://github.com/Pathairush/data_engineering/blob/master/06_capstone_project/image/spark_logo.png)

Apache Spark is a fast and general-purpose cluster computing system. It provides high-level APIs in Java, Scala, Python, and R and an optimized engine that supports general execution graphs. It also supports a rich set of higher-level tools, including [Spark SQL](https://spark.apache.org/docs/2.4.3/sql-programming-guide.html) for SQL and structured data processing, [MLlib](https://spark.apache.org/docs/2.4.3/ml-guide.html) for machine learning, [GraphX](https://spark.apache.org/docs/2.4.3/graphx-programming-guide.html) for graph processing, and [Spark Streaming](https://spark.apache.org/docs/2.4.3/streaming-programming-guide.html).

#### [Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/)  `orchestrator`
![img](https://github.com/Pathairush/data_engineering/blob/master/06_capstone_project/image/airflow_logo.png)

Airflow is a platform to programmatically author, schedule, and monitor workflows.

Use Airflow to author workflows as Directed Acyclic Graphs (DAGs) of tasks. The Airflow scheduler executes your tasks on an array of workers while following the specified dependencies. Rich command line utilities make performing complex surgeries on DAGs a snap. The rich user interface makes it easy to visualize pipelines running in production, monitor progress, and troubleshoot issues when needed.

***Propose how often the data should be updated and why.***

`I94 immigration` data usually update every month. So, we should align our data pipeline with this schedule. There is no point in running the data pipeline every day without new data came in. Other dimensional tables are one time (`US demographic`) or monthly (`World temperature`) updated. 


**Write a description of how you would approach the problem differently under the following scenarios:**

   -  The data was increased by 100x.
	   - Because we leverage the power of spark. There is no need to worry about the scaling size of the underlying computation engine. In case we reach the limit, we can increase the cluster size that the spark is running on. Spark also works in a distributed way, so horizontal scaling is always an option to go for.
	   
   -  The data populates a dashboard that must be updated on a daily basis by 7 am every day.
	   - We can meet this requirement with the SLA option provided by Airflow. This feature will guarantee that the system should populate the data before 7 am every day. In case your task failed, you can fix the problem by shifting the start ETL time earlier or increasing the spark's computation power.

   -  The database needed to be accessed by 100+ people.
	   We can store the data in any data warehouse options and let them access our data. The underlying data format can still be a `delta` format.

