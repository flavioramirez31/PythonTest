import airflow
import urllib
import sys
from airflow import DAG
import csv
import numpy as np
import pandas as pdDF
from io import open
import dateutil.relativedelta
from datetime import datetime, timedelta, date
from airflow.models import Variable

import requests
from requests.auth import HTTPBasicAuth
import json

# Operators
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator, DagRunOrder
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator

# Utils
from airflow.utils.db import provide_session
from airflow.models import XCom
# Non-Airflow libraries
from contextlib import closing

#Constantes
DAG_NAME = 'DriversRotacionConnSQL'
path = '/mx_simulador_rotacion'

timestr = datetime.now().strftime("%Y%m%d%H%M%S")
year = int(datetime.now().strftime("%Y"))
month = int(datetime.now().strftime("%m"))
day = int(datetime.now().strftime("%d"))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['flavio.ramirez@walmart.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout':timedelta(hours=6)
}

with DAG(
    DAG_NAME,
    default_args=default_args,
    description="DriversRotacionConnSQL",
    start_date=datetime(2022, 3, 22),
    schedule_interval=None,
    catchup=False
) as dag:

    # Functions

    #Funcion para liberar memoria
    @provide_session
    def cleanup_xcom(session=None):
        session.query(XCom).filter(XCom.dag_id == DAG_NAME).delete()

    #Funcion para realizar consulta a la Base de Datos
    def ApiTest(**op_args):
        url = "https://api8preview.sapsf.com/odata/v2/FOCompany?$fromDate=1900-01-01&$format=JSON"
        username = "SFAPI@walmartstoT2"
        password = 'MXec2021!'
        #x=1
        res=requests.get(url, auth=(username, password))
        #print(res)

    #Dummy
    start_task = DummyOperator(task_id="Start", dag=dag)

    #Delete XCOM
    delete_xcom_task  = PythonOperator(
        task_id='Delete_Xcom',
        python_callable=cleanup_xcom,
        trigger_rule="all_done",
        dag=dag
    )
   
    ExtractData = PythonOperator(
        task_id="ExtractData",
        python_callable=ApiTest,
        provide_context=True,        
        execution_timeout=timedelta(hours=5),
        do_xcom_push=True,
        dag=dag
    )

    start_task >> ExtractData >> delete_xcom_task   

