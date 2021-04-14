from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

import sys
sys.path.append('/Users/pathairs/Documents/projects/data_engineering/06_capstone_project/etl/')
import extract_fact_label  

pyspark_app_home = Variable.get("PYSPARK_APP_HOME")
default_args = {
    "owner" : "pathairs",
    "retries" : 1,
    "retry_delay" : timedelta(minutes=1)
}

dag = DAG(
    'capstone_data_pipeline',
    default_args = default_args,
    description = 'ETL job for I94 immigration data pipeline',
    schedule_interval = None,
    start_date = days_ago(1)
)

extract_mapping_label = PythonOperator(
    task_id = "extract_mapping_label",
    dag = dag,
    python_callable = extract_fact_label.main
)

stage_fact_table = SparkSubmitOperator(
    task_id = "stage_fact_table",
    dag = dag,
    conn_id = 'spark',
    application = '/Users/pathairs/Documents/projects/data_engineering/06_capstone_project/etl/stage_fact.py',
    packages = 'saurfang:spark-sas7bdat:3.0.0-s_2.12,io.delta:delta-core_2.12:0.8.0',
    name = 'stage_fact_table'
)

load_fact_table = SparkSubmitOperator(
    task_id = "load_fact_table",
    dag = dag,
    conn_id = 'spark',
    application = '/Users/pathairs/Documents/projects/data_engineering/06_capstone_project/etl/load_fact.py',
    packages = 'saurfang:spark-sas7bdat:3.0.0-s_2.12,io.delta:delta-core_2.12:0.8.0',
    name = 'load_fact_table'
)

load_dim_user = SparkSubmitOperator(
    task_id = "load_dim_user",
    dag = dag,
    conn_id = 'spark',
    application = '/Users/pathairs/Documents/projects/data_engineering/06_capstone_project/etl/load_dim_user.py',
    packages = 'saurfang:spark-sas7bdat:3.0.0-s_2.12,io.delta:delta-core_2.12:0.8.0',
    name = 'load_dim_user'
)

load_dim_date = SparkSubmitOperator(
    task_id = "load_dim_date",
    dag = dag,
    conn_id = 'spark',
    application = '/Users/pathairs/Documents/projects/data_engineering/06_capstone_project/etl/load_dim_date.py',
    packages = 'saurfang:spark-sas7bdat:3.0.0-s_2.12,io.delta:delta-core_2.12:0.8.0',
    name = 'load_dim_date'
)

load_dim_state = SparkSubmitOperator(
    task_id = "load_dim_state",
    dag = dag,
    conn_id = 'spark',
    application = '/Users/pathairs/Documents/projects/data_engineering/06_capstone_project/etl/load_dim_state.py',
    packages = 'saurfang:spark-sas7bdat:3.0.0-s_2.12,io.delta:delta-core_2.12:0.8.0',
    name = 'load_dim_state'
)

load_dim_country = SparkSubmitOperator(
    task_id = "load_dim_country",
    dag = dag,
    conn_id = 'spark',
    application = '/Users/pathairs/Documents/projects/data_engineering/06_capstone_project/etl/load_dim_country.py',
    packages = 'saurfang:spark-sas7bdat:3.0.0-s_2.12,io.delta:delta-core_2.12:0.8.0',
    name = 'load_dim_country'
)

data_quality_check = SparkSubmitOperator(
    task_id = "data_quality_check",
    dag = dag,
    conn_id = 'spark',
    application = '/Users/pathairs/Documents/projects/data_engineering/06_capstone_project/etl/data_quality_check.py',
    packages = 'saurfang:spark-sas7bdat:3.0.0-s_2.12,io.delta:delta-core_2.12:0.8.0',
    name = 'data_quality_check'
)

extract_mapping_label >> stage_fact_table >> load_fact_table >> [load_dim_user, load_dim_date] >> data_quality_check
load_dim_state >> data_quality_check
load_dim_country >> data_quality_check

if __name__ == "__main__":
    dag.cli()