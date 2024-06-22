from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.apache.hive.transfers.hive_to_mysql import HiveToMySqlOperator

import csv
import os
import glob
import pandas as pd
from datetime import datetime, timedelta

default_args = {
    'owner':'airflow',
    'email_on_failure': False,
    'email_on_retry':False,
    'email':'abc@abc.com',
    'retries':1,
    'retry_delay':timedelta(minutes=5)
}

def merge_file():
    joined_files = os.path.join("/opt/airflow/dags/files", "*.csv")
    joined_list = glob.glob(joined_files)
    df = pd.concat(map(pd.read_csv, joined_list), ignore_index=True) 
    df.to_csv('/opt/airflow/dags/files/features_combined.csv',index=False) 




with DAG('feature_pipeline', start_date=datetime(2024,5,6),
         schedule_interval='@daily',default_args=default_args, catchup=False) as dag:
    
    file_read = FileSensor(
        task_id = 'file_read',
        fs_conn_id = 'feature_files_path',
        filepath = '*.csv',
        poke_interval=5,timeout=20
    )

    combine_files = PythonOperator(
        task_id='combine_files',
        python_callable = merge_file
    )    

    hdfs_transfer = BashOperator(
        task_id='hdfs_transfer',
        bash_command="""
                hdfs dfs -mkdir -p /features && \
                hdfs dfs -put -f $AIRFLOW_HOME/dags/files/features_combined.csv /features
        """
        )
    
    create_feature_hivetable = HiveOperator(
        task_id='create_feature_hivetable',
        hive_cli_conn_id='hive_conn',
        hql = """
            CREATE EXTERNAL TABLE IF NOT EXISTS feature_data(
            Store STRING, 
            Dates STRING, 
            Temperature STRING, 
            Fuel_Price STRING, 
            MarkDown1 STRING, 
            MarkDown2 STRING, 
            MarkDown3 STRING, 
            MarkDown4 STRING, 
            MarkDown5 STRING, 
            CPI STRING, 
            Unemployment STRING, 
            IsHoliday STRING            
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """
    )

    feature_processing = SparkSubmitOperator(
        task_id='feature_processing',
        application='/opt/airflow/dags/scripts/feature_processing.py',
        conn_id='spark_conn',
        verbose=False
    )

    read_hivetable = HiveOperator(
        task_id='read_hivetable',
        hive_cli_conn_id='hive_conn',
        hql = """
            select * from feature_data where store!='Store'
        """
    )

    hive_to_mysql = HiveToMySqlOperator(
    task_id='hive_to_mysql',
    sql="""
        
        SELECT * FROM
        (SELECT * FROM feature_data WHERE store!='Store') AS subquery
     """,
    
    mysql_table='feature_mysql_table_landing',
    hiveserver2_conn_id='hive_conn',
    mysql_conn_id='mysql_conn',
    dag=dag
    )



file_read >> combine_files >> hdfs_transfer >> create_feature_hivetable >> feature_processing >> read_hivetable >> hive_to_mysql
    
    
