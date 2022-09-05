from airflow import DAG
from airflow.utils.db import provide_session
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.hooks.jdbc_hook import JdbcHook
from airflow.models import XCom
from airflow.models import Variable

from google.oauth2 import service_account
import pandas as pd
import sys
import pandas_gbq
import pendulum
import numpy as np

DAG_NAME = 'DAG_ECO_P17'

def select_P17():
    conn_db2 = JdbcHook(jdbc_conn_id='cartaporte_jdbc_presto_prod17')
    query_sql =    f'''
                  select
                  year_nbr,
                  month_nbr,
                  day_nbr,
                  banner_cd,
                  banner_desc,
                  tribe_nbr,
                  tribe_desc,
                  squad_nbr,
                  squad_desc,
                  dept_desc,
                  dept_nbr,
                  cat_nbr,
                  cat_desc,
                  ship_cost_cy_amt,
                  ship_cost_ly_amt,
                  ship_rtl_cy_amt,
                  ship_rtl_ly_amt,
                  sum(onhand_cost_ly_amt + intran_cost_ly_amt + inwh_cost_ly_amt) as Total_cadena_LY,--last year
                  sum(onhand_cost_cy_amt + intran_cost_cy_amt + inwh_cost_cy_amt) as Total_cadena_CY,--current year
                  visit_comp_ly_dt,--fecha comparativa last year
                  visit_cal_ly_dt,--fecha de cálculo last year
                  visit_cy_dt--fecha de cálculo current year
                  from mx_reporting_tables.mx_reporting_wmt_tot_sales_trib
                  where year_nbr = 2020
                  and month_nbr in (6,5,4,3,2,1)
                  group by 
                  year_nbr,
                  month_nbr,
                  day_nbr,
                  banner_cd,
                  banner_desc,
                  tribe_nbr,
                  tribe_desc,
                  squad_nbr,
                  squad_desc,
                  dept_desc,
                  dept_nbr,
                  cat_nbr,
                  cat_desc,
                  ship_cost_cy_amt,
                  ship_cost_ly_amt,
                  ship_rtl_cy_amt,
                  ship_rtl_ly_amt,
                  visit_comp_ly_dt,
                  visit_cal_ly_dt,
                  visit_cy_dt
                  '''
    dataf = conn_db2.get_pandas_df(sql=query_sql)
    return dataf

def insert_BQ(**context):
    credentials = service_account.Credentials.from_service_account_file(Variable.get("AIRFLOW_ECO_GBQ_KEY_WRITE"),)
    data_ = context['ti'].xcom_pull(task_ids='select_P17')
    data_.columns=["year","month","day","banner_cd","banner_desc","tribe_nbr","tribe_desc","squad_nbr","squad_desc","dept_nbr","dept_desc","cat_nbr","cat_desc","ship_cost_cy_amt","ship_cost_ly_amt","ship_rtl_cy_amt","ship_rtl_ly_amt", "total_cadena_ly","total_cadena_cy","fecha_comp_ly","fecha_calc_ly","fecha_calc_cy"] 
    project_id = "wmt-intl-dasc-gcp-dev"
    table_id = 'DATA_ESTADO_DE_COSTOS.raw_AS_Inventario'
    pandas_gbq.to_gbq(data_, table_id, project_id=project_id, credentials = credentials, if_exists = 'append')

@provide_session
def cleanup_xcom(session=None):
    session.query(XCom).filter(XCom.dag_id == DAG_NAME).delete()


default_args = {
    'owner': 'Estado_de_Costos_Omnicanal',
    #'email': Variable.get("AIRFLOW_ECO_EMAILS_ON_FAILURE").split(','),
    #'email':['jacobo.navarrete@walmart.com'],
    'email':['carlos.olvera@walmart.com'],
    'email_on_failure': bool(int(Variable.get("Centralizacion_Email_Flag")))
}


with DAG(DAG_NAME,
         default_args=default_args,
         schedule_interval=None,
         start_date=pendulum.datetime(2022, 6, 1, tzinfo="America/Mexico_City"),
         description="Prd17-BQ_ECO",
         catchup=False,
         ) as dag:


    select_P17 = PythonOperator(
        task_id='select_P17',
        python_callable=select_P17,
        dag=dag)

    insert_BQ = PythonOperator(
        task_id='insert_BQ',
        python_callable=insert_BQ,
        provide_context=True,
        dag=dag)

    cleanup_xcom = PythonOperator(
        task_id="cleanup_xcom",
        python_callable=cleanup_xcom)

    select_P17 >> insert_BQ >> cleanup_xcom