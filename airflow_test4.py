import airflow
import os
import pandas as pdDF
import csv
import gzip
import numpy as np
import shutil
import calendar
import re
from io import open
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from calendar import monthrange
from time import time
from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.jdbc_hook import JdbcHook
from airflow.hooks import MsSqlHook
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine
from contextlib import closing
from builtins import str

"""
---------------------------------------------------------------------------------------
Global Variables
parallel_task_num = variable which is used to determinate the number of simultaneous tasks. Default value is 10

---------------------------------------------------------------------------------------
"""
parallel_task_num=10
SRC_CONN = 'EA_Oracle_OMS_JDBC'
TGT_CONN = 'WISDOMADHOC_MSSQL_EVENTS'
SRC_TABLE_NAME = "oms.yfs_item"
TGT_TABLE_NAME = "raw_mx_oms_tables_yfs_item"
DAG_NAME ="z_WISDOM_EA_ORACLE_MX_OMS_YFS_ITEM_INC"


def getFastMSQLConn(msql_conn_id):
    conn_meta = BaseHook.get_connection(msql_conn_id)
    user = str(conn_meta.login)
    pwd = str(conn_meta.password)
    host = str(conn_meta.host)
    port = str(conn_meta.port)
    schema = str(conn_meta.schema)
    conn_str = 'mssql+pymssql://{0}:{1}@{2}:{3}/{4}'.format(user, pwd, host, port, schema)
    conn = create_engine(conn_str, pool_recycle=1, pool_timeout=57600).connect()
    return conn



def runSrcSQL(connectionID, SQL):
    """
    runSrcSQL
    Function to run a query in the source connection.
    """
    print('runSrcSQL() ------ ')
    print('--------------- SQL : ' + SQL )
    print('--------------- connectionID: ' + connectionID )
    start_time_per = time()
    
    jdbc_hook = JdbcHook(jdbc_conn_id=connectionID)
    jdbc_hook.autocommit = True    
    start_time_dly = time()
    src_df = jdbc_hook.get_pandas_df(SQL)
    elapsed_time_dly = time() - start_time_dly
    print("runSrcSQL() Query executed in: %.10f seconds." % elapsed_time_dly)
    return src_df
    
      
def runTrgtSQL(connectionID, SQL):
    """
    runTrgtSQL
    Function to run a query in the target connection.
    """
    print('runTrgtSQL() ------ ')
    print('--------------- SQL : ' + SQL )
    print('--------------- connectionID: ' + connectionID )
    start_time_per = time()
    mssql_hook = MsSqlHook(mssql_conn_id=connectionID)
    mssql_hook.autocommit = True
    start_time_dly = time()
    df = mssql_hook.get_pandas_df(SQL)
    elapsed_time_dly = time() - start_time_dly
    print("runTrgtSQL() Query executed in: %.10f seconds." % elapsed_time_dly)
    return df



def dataToExtract(*op_args,**context):
    """
    dataToExtract

    Function to get the partitions we will get from the source. 
    Ex. Get the dates to process. 
    """
    print('dataToExtract() ------ ')
    SQL_GetDate = "SELECT \
                    FORMAT(COALESCE(MAX(b.FROM_DATETIME),MAX(a.FROM_DATETIME),getdate()),'yyyy-MM-dd') AS FROM_DATETIME,\
                    FORMAT(COALESCE(MAX(b.TO_DATETIME),MAX(a.TO_DATETIME),getdate()),'yyyy-MM-dd') AS TO_DATETIME\
                    FROM dbo.load_control_EA a \
                    LEFT JOIN [dbo].[load_control_Master] b ON 1 = b.Flag \
                    WHERE TABLE_NAME = '{0}' AND b.Banner = 'EA'\
                    ".format(TGT_TABLE_NAME)
    df_date = runTrgtSQL(TGT_CONN, SQL_GetDate)
    startDate = (df_date['FROM_DATETIME'].to_list())[0]
    finishDate =(df_date['TO_DATETIME'].to_list())[0]

    
    SQL = "SELECT distinct CASE WHEN extn_dept_id = 'null' OR extn_dept_id IS null then 'sin_dept'\
                                else extn_dept_id end as EXTN_DEPT_ID FROM {0}\
            WHERE ORGANIZATION_CODE  = 'WM_BU'\
         ".format(SRC_TABLE_NAME)
    
    print(SQL)
    df = runSrcSQL(SRC_CONN, SQL)
    partsToProcess = df['EXTN_DEPT_ID'].to_list()
    print('-------- partsToProcess')
    print('FROM_DATETIME = ' + str(startDate))
    print('TO_DATETIME   = ' + str(finishDate))
    return partsToProcess,startDate,finishDate    

def getParallelTaskParams(**context):
    """
    getParallelTaskParams

    Divide the partitions between the workers. 
    """
    print('------ getParallelTaskParams ')
    return_dataToExtract = context['ti'].xcom_pull(task_ids='dataToExtract')
    partsToProcess = return_dataToExtract[0]
    parallelTaskParams = []
    leen = len(partsToProcess)
    result_uts = len(partsToProcess)
    tasks_toexec = int(result_uts / parallel_task_num)     
    extra_uts = result_uts % parallel_task_num
    extra_utss = result_uts % parallel_task_num
    emptyParams = False
    maxNumOfTask = parallel_task_num
    accum = 0
    init_index = 0
    iteration_num = 0

    if extra_uts > (tasks_toexec * parallel_task_num  ):
        maxNumOfTask = extra_uts
        emptyParams = True
        
    for t in range (parallel_task_num):
        init_index = 0
        iteration_num = 0
        if extra_uts > 0:
            extra_uts = extra_uts - 1
            init_index = ((tasks_toexec +1)*t)
            iteration_num = tasks_toexec + 1
        elif not emptyParams:
            init_index = (tasks_toexec*t) + extra_utss
            iteration_num = tasks_toexec
        parallelTaskParams.append([init_index,iteration_num])
    
    parallelTaskParams.append(maxNumOfTask)
    print('parallelTaskParams = ' + str(parallelTaskParams))
    print('------ getParallelTaskParams ')
    return parallelTaskParams

    

def loadTGData(*op_args, **context):
    """
    loadTGData
    This method creates the query to run in the source. 
    """
    print('loadTGData() task {}------------------------------'.format(op_args[0]))
    
    return_dataToExtract = context['ti'].xcom_pull(task_ids='dataToExtract')
    partsToProcess = return_dataToExtract[0]
    startDate = return_dataToExtract[1]
    finishDate = return_dataToExtract[2]
    parallelTaskParams = context['ti'].xcom_pull(task_ids='getParallelTaskParams')
    print('partsToProcess = ' + str(partsToProcess))
    print('parallelTaskParams = ' + str(parallelTaskParams))
    
    params = parallelTaskParams[op_args[0]]
    totalPartitions = parallelTaskParams[-1]
    iteration_num = params[1]
    part_to_process_index = params[0]
    
    print('iteration_num = {0}, part_to_process_index = {1}'.format(iteration_num,part_to_process_index))
  
    first_partition = partsToProcess[part_to_process_index]
    last_partition = partsToProcess[part_to_process_index+iteration_num-1]
    start_time_per = time()
    
    partitions = []
    
    for p in range(iteration_num):
        buPartition = partsToProcess[part_to_process_index]
        partitions.append(buPartition)
        part_to_process_index += 1

    print("partitons " + str(partitions))
    partitions_list = re.sub('(\]|\[)','',str(partitions))
    srcQuery = "SELECT coalesce(item_id, 'null_value') as item_id,\
                coalesce(organization_code, 'null_value') as organization_code,\
                coalesce(item_key, '0') as item_key,\
                coalesce(extn_source, 'null_value') as extn_source,\
                coalesce(extn_upc, 'null_value') as extn_upc,\
                coalesce(extn_offer_seller_id, '0') as extn_offer_seller_id,\
                coalesce(extn_item_id, '0') as extn_item_id,\
                coalesce(extn_dept_id, '0') as extn_dept_id,\
                coalesce(extn_offer_type, 'null_value') as extn_offer_type,\
                coalesce(extn_big_item, 'null_value') as extn_big_item,\
                coalesce(extn_offer_length, '0') as extn_offer_length,\
                coalesce(unit_length, 0.0) as unit_length,\
                coalesce(extn_offer_height, '0') as extn_offer_height,\
                coalesce(extn_offer_width, '0') as extn_offer_width,\
                coalesce(unit_height, 0.0) as unit_height,\
                coalesce(unit_width, 0.0) as unit_width,\
                coalesce(extn_offer_weight, '0') as extn_offer_weight,\
                coalesce(unit_weight, 0.0) as unit_weight,\
                coalesce(extn_offer_seller_name, 'null_value') as extn_offer_seller_name,\
                createts, modifyts\
                FROM {0} \
                WHERE organization_code = 'WM_BU'\
                and EXTN_DEPT_ID in ({1}) and to_char(CREATETS, 'YYYYMMDDHH24MI') >= '{2}'".format(SRC_TABLE_NAME,partitions_list,startDate)
    print(srcQuery)
    start_time_dly = time()
    passThroughData(srcQuery)
    elapsed_time_per = time() - start_time_per
    print('Total time process for partitions %.10f seconds.' % elapsed_time_per)


def passThroughData(srcQuery):
    """
    passThroughData
    Read data from SRC_TABLE_NAME table and write it to TGT_TABLE_NAME.
    """
   
    print('passThroughData() task {}------------------------------')
    tgf = ['item_id','organization_code','item_key','extn_source','extn_upc','extn_offer_seller_id','extn_item_id','extn_dept_id','extn_offer_type','extn_big_item','extn_offer_length','unit_length','extn_offer_height','extn_offer_width','unit_height','unit_width','extn_offer_weight','unit_weight','extn_offer_seller_name','createts','modifyts']
    start_time_per = time()
    mssql_hook = MsSqlHook(mssql_conn_id=TGT_CONN)
    mssql_hook.autocommit = True
    
    start_time_day = time()
    print('Running query : ' + srcQuery)
    
    start_time_dly = time()
    df = runSrcSQL(SRC_CONN, srcQuery)
    elapsed_time_dly = time() - start_time_dly
    
    print("Source query executed in: %.10f seconds." % elapsed_time_dly)
    rownum = len(df.index)
    print("Records got from the query: " + str(rownum))
   
    if rownum > 0:
        print('Writing read {0} rows to {1}'.format(rownum, TGT_TABLE_NAME))
        start_time_dly = time()
        try:
            """
            mssql_hook.insert_rows(table=TGT_TABLE_NAME, rows=df.to_records(index=False), target_fields=tgf, commit_every=10000, replace=False)
            elapsed_time_dly = time() - start_time_dly
            print("Data writed in: %.10f seconds." % elapsed_time_dly)
            """
            
            tgt_fast_conn = getFastMSQLConn(TGT_CONN)
            with closing(tgt_fast_conn) as conn:              
                df.to_sql(con=conn, name=TGT_TABLE_NAME, if_exists='append',chunksize=1000,method='multi',index=False)

            elapsed_time_dly = time() - start_time_dly
            print("Data writed in: %.10f seconds." % elapsed_time_dly)
            
        except Exception as e:
            print("Error while inserting data: " + str(e))
            print(df)
    else:
        print("No data for query " + srcQuery)        
    
    elapsed_time_per = time() - start_time_per
    
    print('uploadTopassThroughDataADLS() has taken %.10f seconds.' % elapsed_time_per)


    
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['jorge.garcia9@walmart.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'wait_for_downstream': False
}

dag = DAG(
    DAG_NAME,
    default_args=default_args,
    description='Dag for pass through data between ORACLE to MSSS',
    start_date=datetime(2021,1,4),
    schedule_interval='45 6 * * *',
    catchup=False,
    tags=['z_Wisdom_EA_AdHoc']
)

t1 = DummyOperator(task_id="Start", dag=dag)

t2 = PythonOperator(
    task_id='dataToExtract',
    python_callable=dataToExtract,
    provide_context=True,
    dag=dag
)

t3 = PythonOperator(
    task_id='getParallelTaskParams',
    python_callable=getParallelTaskParams,
    provide_context=True,
    dag=dag
)

for t in range (parallel_task_num): 
    t4 = PythonOperator(
        task_id='loadTGData{}'.format(t+1),
        python_callable=loadTGData,
        op_args=[t],
        provide_context=True,
        dag=dag,
    )
    t1 >> t2 >> t3 >> t4