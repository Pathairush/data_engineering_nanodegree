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
2. `World Temperature` - Dataset contains the global land temperature by city starting from year 1750 till 2015. [more detail](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/information/)
3. `US City demographic` - Dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,00. Data comes from the US Census Bureau's 2015 American Community Survey. [more detail](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/information/)

## Foundings 

We explore data in many different ways, here are what you need to know why we came up with the following data model design and schema. 

### I94 Immigration
- `visapost`, `occup`, `entdepu`, `insnum` columns have a high missing value percentage ( > 50 % of the total column in 1 month), thus we decide to drop those columns.
- `admnum`, `dtaddto`, `matflag`, `entdepa`, `entdepd`, `dtadfile`, `count` seem not have a meaningful value for further understanding of the data, drop those columns as well.
- `i94bir`, `biryear` have the same information so we keep only one `biryear`.

###  World temperature
- For world temperature data, because we only have `country` level of data in fact table,  we decide to aggregate the world temperature data into `country` level so that we can join it with `born_country`, `residence_country` columns

### US demographic
- For US demographics dataset, we saw that the data is in the count number of each race level. So, we decided to aggregate them to `state_code` level so that we can join it with the fact table `state_code`

### General 
- For null value, we decide to leave it as it is if it's not a primary or foreign key.

## Conceptual data model

From the foundings in previous section, here are the data model that we decide to apply for this project.



