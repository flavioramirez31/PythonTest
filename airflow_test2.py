import logging
from datetime import timedelta, datetime
import base64

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from contextlib import closing
from airflow.contrib.hooks.azure_data_lake_hook import AzureDataLakeHook
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings
from airflow.hooks.mssql_hook import MsSqlHook
from airflow.models import XCom
from airflow.utils.db import provide_session
from airflow.operators.dagrun_operator  import TriggerDagRunOperator
from airflow.hooks.jdbc_hook import JdbcHook

import vn51zxm_cartaporte_libs.configuration_file as cf
from vn51zxm_cartaporte_libs.zoom_notification import ZoomNotification
from vn51zxm_cartaporte_libs.airflow.wasb_hook import WasbHook
from vn51zxm_cartaporte_libs.airflow.pyodbc_hook import PyODBCConnection

# Importing base classes that we need to derive
import os, sys

sys.path.append(os.path.join(os.path.dirname(__file__), "vn51zxm_cartaporte_libs"))

#Other imports
from sqlalchemy import create_engine
from contextlib import closing
import pandas as pd
from time import time, sleep
from datetime import datetime, timedelta, date, tzinfo
import json
from io import BytesIO, StringIO, open
import gzip
import traceback
import html
import re
from dateutil import tz


# Variable principal settings
settings = Variable.get("cartaporte_dags_settings", deserialize_json=True)

DAG_NAME = 'Cartaporte_adls_PlantaCarnes'
PROCESO = 'planta-carnes'
ENVIRONMENT = settings['environment']
ORIGIN_WASB_CONN_ID = settings['adls'][PROCESO]['origin_wasb_conn_id']
ORIGIN_CONTAINER_NAME = settings['adls'][PROCESO]['origin_container_name']
ORIGIN_CONTAINER_PATH = settings['adls'][PROCESO]['origin_container_path']
DESTINATION_WASB_CONN_ID = settings['adls'][PROCESO]['destination_wasb_conn_id']
DESTINATION_CONTAINER_NAME = settings['adls'][PROCESO]['destination_container_name']
DESTINATION_CONTAINER_PATH = settings['adls'][PROCESO]['destination_container_path']
DESTINATION_CONTAINER_PATH_ERROR = settings['adls'][PROCESO]['destination_container_path_error']
DAGRUN_TIMEOUT = settings['adls'][PROCESO]['dagrun_timeout']
SCHEDULE_INTERVAL = settings['adls'][PROCESO]['schedule_interval']
TASK_RETRIES = settings['adls'][PROCESO]['task_retries']
EMAIL = settings['notifications']['email']['destination']
EMAIL_ON_FAILURE = settings['notifications']['email']['email_on_failure']
EMAIL_ON_FAILURE_WITH_EXCEPTION = settings['notifications']['email']['email_on_failure_with_exception']
EMAIL_WITH_EXCEPTION = EMAIL if EMAIL_ON_FAILURE_WITH_EXCEPTION else None
EMAIL_ON_RETRY = settings['notifications']['email']['email_on_retry']
ZOOM_ON_FAILURE = settings['notifications']['zoom']['zoom_on_failure']
ZOOM_ENDPOINT_WEBHOOK = settings['notifications']['zoom']['zoom_endpoint_webhook']
ZOOM_VERIFICATION_TOKEN = settings['notifications']['zoom']['zoom_verification_token']

CPORTE_COLS = ['ID_PC','RFC','NUMTOTALMERCANCIAS','BIENESTRANSP','DESCRIPCION','CANTIDAD_TIPO','CLAVEUNIDAD','MATERIALPELIGROSO','CVEMATERIAL_PELIGROSO','EMBALAJE','EMBALAJE_DESC','PESOENKG','CANTIDAD_ITEM','UNIDADPESO','CEDIS_DESTINO_FINAL','ID_WTMS']
TARGET_TZ="America/Mexico_City"
to_zone = tz.gettz(TARGET_TZ)
todaydtStr = datetime.strftime(datetime.now().astimezone(to_zone), '%Y-%m-%d %H:%M:%S')
START_TIME_DAG = days_ago(1)
IGNORED_BLOBS = ['processed', 'no_processed']

adls_origin = WasbHook(wasb_conn_id=ORIGIN_WASB_CONN_ID)
adls_destination = WasbHook(wasb_conn_id=DESTINATION_WASB_CONN_ID)

#Notificaciones vía Zoom
zoom_notifications = ZoomNotification(ZOOM_ENDPOINT_WEBHOOK, ZOOM_VERIFICATION_TOKEN) if ZOOM_ON_FAILURE else None


def send_success_message(message):
    def send_zoom_success_notification(context):
        zoom_notifications.send_success_message(context, message)

    return send_zoom_success_notification


def send_failure_alert(message=None, env=None, email=None):
    def failure_alert(context):
        if zoom_notifications is not None:
            msg = message if message is not None else "no_message"
            logging.info("Se envia notificacion Zoom...")
            zoom_notifications.send_failure_message(context, msg)
        if email is not None:
            logging.info("Se envia notificacion email...")
            task_id = context.get('task_instance').task_id
            dag_id = context.get("dag").dag_id
            execution_time = context.get("execution_date")
            raw_exception = context.get("exception")
            raw_exception_lines = traceback.TracebackException.from_exception(raw_exception).format()
            exception_lines = [s.encode('ascii', 'ignore').decode('ascii') for s in raw_exception_lines]
            reason = (
                html.escape("_BR_TAG_".join(exception_lines))).replace(
                '_BR_TAG_', '<br />')

            dag_failure_html_body = f"""<html>
            <header><title>The below DAG has failed!</title></header>
            <body>
            <b>DAG Name</b>: {dag_id}<br/>
            <b>Task Id</b>: {task_id}<br/>
            <b>Execution Time (UTC)</b>: {execution_time}<br/>
            <b>Reason for Failure</b>: {reason}<br/>
            </body>
            </html>
            """

            subject = f"Airflow alert: <DagInstance: {dag_id} - {execution_time} [failed]"

            if env is not None:
                upper_env = env.upper()
                subject = f"{upper_env} | Airflow alert: <DagInstance: {dag_id} - {execution_time} [failed]"

            try:
                send_email(
                    to=email,
                    subject=subject,
                    html_content=dag_failure_html_body,
                )
            except Exception as e:
                logging.error(e, exc_info=True)
                logging.error(f'Error in sending email to address {email}')

    return failure_alert


# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'cartaporte',
    'depends_on_past': False,
    'email': EMAIL,
    'email_on_failure': EMAIL_ON_FAILURE,
    'email_on_retry': EMAIL_ON_RETRY,
    'on_failure_callback': send_failure_alert(env=ENVIRONMENT, email=EMAIL_WITH_EXCEPTION),
    'start_date': START_TIME_DAG,
    'retries': TASK_RETRIES,
    'retry_delay': timedelta(seconds=15)
}

@provide_session

def cleanup_xcom(session=None):
    session.query(XCom).filter(XCom.dag_id == DAG_NAME).delete()
    
with DAG(
    DAG_NAME,
    default_args=default_args,
    description='DAG for moving files from one ADLS (SAS-KEY) to MX-DO ADLS',
    schedule_interval=None if not SCHEDULE_INTERVAL else SCHEDULE_INTERVAL,
    catchup=False,
    on_success_callback=cleanup_xcom,
    dagrun_timeout=None if DAGRUN_TIMEOUT == 0 else timedelta(minutes=DAGRUN_TIMEOUT),
    max_active_runs=1
) as dag:
    def move_file(origin_container, blob, destination_container, destination_path):
        filename = blob.split('/')[-1]
        stream_blob = adls_origin.get_blob_to_bytes(container_name=origin_container, blob_name=blob)
        logging.info("Uploading the blob {}...".format(blob))
        adls_destination.create_blob_from_bytes(container_name=destination_container,
                                                blob_name="{0}{1}".format(destination_path, filename),
                                                blob=stream_blob.content)
        logging.info("Deleting the blob {}...".format(blob))
        adls_origin.delete_file(container_name=origin_container, blob_name=blob, is_prefix=False, ignore_if_missing=False)


    def insert_not_processed_file(blob, container, error):
        filename = blob.split('/')[-1]
        archivo_leido = False
        df = pd.DataFrame()
        try:
            logging.info('Intentando leer como CSV...')
            df = read_file_csv(blob, container, only_utf8=False)
        except Exception as e:
            logging.error('Error al intentar leer como CSV.')
            try:
                logging.info('Intentando leer como TXT...')
                df = read_file_csv(blob, container, sep='\t', only_utf8=False)
            except Exception as e:
                logging.error('Error al intentar leer como TXT.')
                try:
                    logging.info('Intentando leer como EXCEL...')
                    df = read_file_excel(blob, container)
                except Exception as e:
                    logging.error('Error al intentar leer como EXCEL.')

        if len(df) != 0:
            archivo_leido = True

        if archivo_leido:
            try:
                df['archivo'] = filename
                df['error'] = error
                df['contenedor'] = container
                df['fecha_proc'] = todaydtStr
                tokens = filename.split("_")
                if len(tokens) > 4 and tokens[4].isnumeric():
                    df['cedis_origen'] = tokens[4]
                else:
                    df['cedis_origen'] = None
                odbc_conn = PyODBCConnection('cartaporte_azuresql_odbc_gls').get_conn()
                logging.info('connected to {}'.format(type(odbc_conn)))
                cursor = odbc_conn.cursor()
                cursor.fast_executemany = True
                insert_np = 'INSERT INTO TT_NoProcesados VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
                cursor.executemany(insert_np, df.values.tolist())
                cursor.commit()
                cursor.close()
                odbc_conn.close()
                logging.info('Finaliza insert en la base TT_NoProcesados')
            except Exception as e:
                logging.error('Error al insertar informacion carga en tabla TT_NoProcesados...')
                logging.error('Se registra informacion basica...')

                odbc_conn = PyODBCConnection('cartaporte_azuresql_odbc_gls').get_conn()
                logging.info('connected to {}'.format(type(odbc_conn)))
                cursor = odbc_conn.cursor()
                cursor.fast_executemany = True
                insert_np = 'INSERT INTO TT_NoProcesados (archivo, error, contenedor, fecha_proc) VALUES (?,?,?,?)'
                cursor.execute(insert_np, (filename, error + ' -> ' + str(e), container, todaydtStr))
                cursor.commit()
                cursor.close()
                odbc_conn.close()
                logging.info('Finaliza insert en la base TT_NoProcesados')
        else:
            odbc_conn = PyODBCConnection('cartaporte_azuresql_odbc_gls').get_conn()
            logging.info('connected to {}'.format(type(odbc_conn)))
            cursor = odbc_conn.cursor()
            cursor.fast_executemany = True
            insert_np = 'INSERT INTO TT_NoProcesados (archivo, error, contenedor, fecha_proc) VALUES (?,?,?,?)'
            cursor.execute(insert_np, (filename, error, container, todaydtStr))
            cursor.commit()
            cursor.close()
            odbc_conn.close()
            logging.info('Finaliza insert en la base TT_NoProcesados')

        odbc_conn = PyODBCConnection('cartaporte_azuresql_odbc_gls').get_conn()
        logging.info('connected to {}'.format(type(odbc_conn)))
        cursor = odbc_conn.cursor()
        cursor.fast_executemany = True
        insert_np = 'INSERT INTO TT_NoProcesados (archivo, error, contenedor, fecha_proc) VALUES (?,?,?,?)'
        cursor.execute(insert_np, (filename, error, container, todaydtStr))
        cursor.commit()
        cursor.close()
        odbc_conn.close()
        logging.info('Finaliza insert en la base TT_NoProcesados')

    def cleanDF(df):
        # Pendiente por implementar limpieza de dataframe

        # Remove \r in all columns
        #newDf = df.replace({r'\\r': ''}, regex=True)

        # Remove row with ALL empty columns
        # Reference: https://stackoverflow.com/questions/61964116/delete-rows-from-pandas-dataframe-if-all-its-columns-have-empty-string/
        #newDf = newDf[~newDf.eq('').all(1)]

        #return newDf
        return df

    def read_file_csv(blob, container_name, sep=',', only_utf8=True):
        df = pd.DataFrame()
        try:
            logging.info('Intentando lectura de archivo con encoding utf-8')
            file_ = adls_origin.read_file_to_bytes(container_name=container_name, blob_name=blob)
            df = pd.read_csv(BytesIO(file_), names=CPORTE_COLS, sep=sep, skiprows=1, lineterminator='\n',index_col=False, skip_blank_lines=True)
        except Exception as e:
            if (only_utf8):
                raise Exception(e)
            logging.error('Falla en lectura de archivo con encoding utf-8. Intentando lectura con encoding ISO-8859-1')
            file_ = adls_origin.read_file(container_name=container_name, blob_name=blob, encoding='ISO-8859-1')
            df = pd.read_csv(StringIO(file_), names=CPORTE_COLS, sep=sep, skiprows=1, lineterminator='\n', index_col=False, skip_blank_lines=True, encoding='ISO-8859-1')

        return cleanDF(df)

    def read_file_excel(blob, container_name):
        df = pd.DataFrame()
        logging.info('Intentando lectura de archivo con encoding utf-8')
        file_ = adls_origin.read_file_to_bytes(container_name=container_name, blob_name=blob)
        df = pd.read_excel(BytesIO(file_), names=CPORTE_COLS, skiprows=1, index_col=None, header=None, usecols=CPORTE_COLS)
        return cleanDF(df)

    def insert_file(blob, container_name):
        filename = blob.split('/')[-1]
        df = pd.DataFrame()
        if filename.lower().endswith('csv'):
            logging.info('Archivo CSV')
            df = read_file_csv(blob, container_name)
        elif filename.lower().endswith('txt'):
            logging.info('Archivo TXT')
            df = read_file_csv(blob, container_name, sep='\t')
        else:
            logging.info('Archivo EXCEL')
            df = read_file_excel(blob, container_name)

        if filename.lower().startswith('pc_mx_'):
            logging.info('>>> Flujo: PLANTA DE CARNES')
            df['MATERIALPELIGROSO'] = df['MATERIALPELIGROSO'].astype(str)
            df['CVEMATERIAL_PELIGROSO'] = df['CVEMATERIAL_PELIGROSO'].astype(str)
            df['EMBALAJE'] = df['EMBALAJE'].astype(str)
            df['MATERIALPELIGROSO'] = df['MATERIALPELIGROSO'].replace('nan', '0')
            df['CVEMATERIAL_PELIGROSO'] = df['CVEMATERIAL_PELIGROSO'].replace('nan', 'NULL')
            df['EMBALAJE'] = df['EMBALAJE'].replace('nan', '')
            df['EMBALAJE_DESC'] = df['EMBALAJE_DESC'].replace('nan', '')
            df['CEDIS_ORIGEN'] = blob.split('_')[4]
            df['archivo'] = filename
            df['F_Proc'] = todaydtStr
            df['procesado'] = 0

            # inserta en Base de datos
            odbc_conn = PyODBCConnection('cartaporte_azuresql_odbc_gls').get_conn()
            logging.info('connected to {}'.format(type(odbc_conn)))
            cursor = odbc_conn.cursor()
            cursor.fast_executemany = True
            insert_bh = 'INSERT INTO TT_PlantaCarnes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
            cursor.executemany(insert_bh, df.values.tolist())
            cursor.commit()
            cursor.close()
            odbc_conn.close()
            logging.info('Finaliza insert en la base TT_PlantaCarnes')
        else:
            logging.info('>>> Flujo: PLANTA DE CARNES RECOLECCION')
            df['MATERIALPELIGROSO'] = df['MATERIALPELIGROSO'].astype(str)
            df['CVEMATERIAL_PELIGROSO'] = df['CVEMATERIAL_PELIGROSO'].astype(str)
            df['EMBALAJE'] = df['EMBALAJE'].astype(str)
            df['MATERIALPELIGROSO'] = df['MATERIALPELIGROSO'].replace('nan', '0')
            df['CVEMATERIAL_PELIGROSO'] = df['CVEMATERIAL_PELIGROSO'].replace('nan', 'NULL')
            df['EMBALAJE'] = df['EMBALAJE'].replace('nan', '')
            df['EMBALAJE_DESC'] = df['EMBALAJE_DESC'].replace('nan', '')
            df['CEDIS_ORIGEN'] = blob.split('_')[5]
            df['archivo'] = filename
            df['F_Proc'] = todaydtStr
            df['procesado'] = 0

            # inserta en Base de datos
            odbc_conn = PyODBCConnection('cartaporte_azuresql_odbc_gls').get_conn()
            logging.info('connected to {}'.format(type(odbc_conn)))
            cursor = odbc_conn.cursor()
            cursor.fast_executemany = True
            insert_bh = 'INSERT INTO TT_PlantaCarnes_Recoleccion VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
            cursor.executemany(insert_bh, df.values.tolist())
            cursor.commit()
            cursor.close()
            odbc_conn.close()
            logging.info('Finaliza insert en la base TT_PlantaCarnes_Recoleccion')

    def get_list_blobs(container, prefix):
        blobs = adls_origin.list_blobs(container, prefix)
        lfinal = []
        for i in blobs:
            valid = True
            for j in IGNORED_BLOBS:
                if i.lower() == f'{prefix}{j}'.lower() or i.lower().startswith(f'{prefix}{j}/'.lower()):
                    valid = False
                    break
            if valid:
                lfinal.append(i)
        return lfinal

    def upload_blobs(**context):
        prefix = ORIGIN_CONTAINER_PATH
        pattern = r'^' + prefix + '(pc_mx_|pc_r_mx_).*\.(csv|txt|xls|xlsx)$'
        blobs = get_list_blobs(ORIGIN_CONTAINER_NAME, prefix)
        for blob in blobs:
            result = re.match(pattern, blob, re.IGNORECASE)
            if not result:
                msg_error = f'Archivo {blob} no cumple con nombre establecido...'
                logging.info(msg_error)
                insert_not_processed_file(blob, ORIGIN_CONTAINER_NAME, msg_error)
                move_file(ORIGIN_CONTAINER_NAME, blob, DESTINATION_CONTAINER_NAME, DESTINATION_CONTAINER_PATH_ERROR)
                continue
            tokens = blob.split("_")
            filename = blob.split('/')[-1]
            if not ((filename.lower().startswith('pc_mx_') and len(tokens) > 4 and tokens[4].isnumeric())
                    or (filename.lower().startswith('pc_r_mx_') and len(tokens) > 5 and tokens[5].isnumeric())):
                msg_error = f'Archivo {blob} no contiene CEDIS origen...'
                logging.info(msg_error)
                insert_not_processed_file(blob, ORIGIN_CONTAINER_NAME, msg_error)
                move_file(ORIGIN_CONTAINER_NAME, blob, DESTINATION_CONTAINER_NAME, DESTINATION_CONTAINER_PATH_ERROR)
                continue
            try:
                logging.info(f'Archivo {blob} considerado para lectura de contenido...')
                insert_file(blob, ORIGIN_CONTAINER_NAME)
                move_file(ORIGIN_CONTAINER_NAME, blob, DESTINATION_CONTAINER_NAME, DESTINATION_CONTAINER_PATH)
            except Exception as e:
                msg_error = f'Error al procesar archivo {blob}: {str(e)}'
                logging.error(e, exc_info=True)
                logging.error('Moviendo archivo a no procesados...')
                insert_not_processed_file(blob, ORIGIN_CONTAINER_NAME, msg_error)
                move_file(ORIGIN_CONTAINER_NAME, blob, DESTINATION_CONTAINER_NAME, DESTINATION_CONTAINER_PATH_ERROR)

    ##### Inicia carga de datos de WTMS #####
    # Selección de información proveniente de DB2 hacia azure sql
    def factura(**context):
        #Conexión a Azure
        odbc_conn = PyODBCConnection('cartaporte_azuresql_odbc_gls').get_conn()
        loadid = '''
        SELECT DISTINCT id_wtms
        FROM [dbo].[TT_PlantaCarnes] AS A WITH(NOLOCK)
        WHERE NOT EXISTS (
                            SELECT *
                            FROM tt_wtms_inbound WITH(NOLOCK)
                            WHERE A.id_wtms = load_id
                          )
        AND A.procesado = 0
        '''

        load_id = pd.read_sql_query(loadid, odbc_conn)
        logging.info("Extracción de AzureSQL exitosa!")

        # Selección de información proveniente de DB2 hacia azure sql
        loadid_ = load_id['id_wtms'].tolist()
        if len(loadid_) == 0:
            logging.info("No hay informacion por consultar en WTMS")
            return

        loadid_ = str(loadid_).replace('[','').replace(']','')

        #Conexión a Informix
        JdbcConn = JdbcHook(jdbc_conn_id='cartaporte_informix_wtms')
        querySql = f"""
            SELECT l.load_id,
                   l.orig_loc_id AS idOrigen,
                   p.location_id AS idDestino,
                   l.ready_ts AS FechaCargaOrigen,
                   l.trailer_id AS Placa,
                   CASE
                       WHEN c.arrival_ts IS NULL THEN
                              (SELECT max(must_arrive_ts)
                               FROM load_stop lp
                               WHERE lp.load_id = l.load_id
                               GROUP BY lp.load_id)
                       ELSE c.arrival_ts
                   END AS FechaCitaDestino,
                   p.dist_frm_prev_stop AS Distancia,
                   O.address_txt AS calle_O,
                   CASE O.state_prov_code
                       WHEN 'AG' THEN 'AGU'
                       WHEN 'BC' THEN 'BCN'
                       WHEN 'BS' THEN 'BCS'
                       WHEN 'CM' THEN 'CAM'
                       WHEN 'CS' THEN 'CHP'
                       WHEN 'CH' THEN 'CHH'
                       WHEN 'CO' THEN 'COA'
                       WHEN 'CL' THEN 'COL'
                       WHEN 'CX' THEN 'DIF'
                       WHEN 'DG' THEN 'DUR'
                       WHEN 'GT' THEN 'GUA'
                       WHEN 'GR' THEN 'GRO'
                       WHEN 'HG' THEN 'HID'
                       WHEN 'JA' THEN 'JAL'
                       WHEN 'EM' THEN 'MEX'
                       WHEN 'MI' THEN 'MIC'
                       WHEN 'MO' THEN 'MOR'
                       WHEN 'NA' THEN 'NAY'
                       WHEN 'NL' THEN 'NLE'
                       WHEN 'OA' THEN 'OAX'
                       WHEN 'PU' THEN 'PUE'
                       WHEN 'QT' THEN 'QUE'
                       WHEN 'QR' THEN 'ROO'
                       WHEN 'SL' THEN 'SLP'
                       WHEN 'SI' THEN 'SIN'
                       WHEN 'SO' THEN 'SON'
                       WHEN 'TB' THEN 'TAB'
                       WHEN 'TM' THEN 'TAM'
                       WHEN 'TL' THEN 'TLA'
                       WHEN 'VE' THEN 'VER'
                       WHEN 'YU' THEN 'YUC'
                       WHEN 'ZA' THEN 'ZAC'
                   END AS edo_O,
                   CASE O.country_code
                        WHEN 'MX' THEN 'MEX'
                        WHEN 'US' THEN 'USA'
                   END AS pais_O,
                   O.postal_code AS cp_O,
                   O.location_type_id,
                   O.city_name AS cd_O,
                   O.ref_field2_txt,
                   D.address_txt AS calle_D,
                   CASE D.state_prov_code
                       WHEN 'AG' THEN 'AGU'
                       WHEN 'BC' THEN 'BCN'
                       WHEN 'BS' THEN 'BCS'
                       WHEN 'CM' THEN 'CAM'
                       WHEN 'CS' THEN 'CHP'
                       WHEN 'CH' THEN 'CHH'
                       WHEN 'CO' THEN 'COA'
                       WHEN 'CL' THEN 'COL'
                       WHEN 'CX' THEN 'DIF'
                       WHEN 'DG' THEN 'DUR'
                       WHEN 'GT' THEN 'GUA'
                       WHEN 'GR' THEN 'GRO'
                       WHEN 'HG' THEN 'HID'
                       WHEN 'JA' THEN 'JAL'
                       WHEN 'EM' THEN 'MEX'
                       WHEN 'MI' THEN 'MIC'
                       WHEN 'MO' THEN 'MOR'
                       WHEN 'NA' THEN 'NAY'
                       WHEN 'NL' THEN 'NLE'
                       WHEN 'OA' THEN 'OAX'
                       WHEN 'PU' THEN 'PUE'
                       WHEN 'QT' THEN 'QUE'
                       WHEN 'QR' THEN 'ROO'
                       WHEN 'SL' THEN 'SLP'
                       WHEN 'SI' THEN 'SIN'
                       WHEN 'SO' THEN 'SON'
                       WHEN 'TB' THEN 'TAB'
                       WHEN 'TM' THEN 'TAM'
                       WHEN 'TL' THEN 'TLA'
                       WHEN 'VE' THEN 'VER'
                       WHEN 'YU' THEN 'YUC'
                       WHEN 'ZA' THEN 'ZAC'
                   END AS edo_D,
                   CASE D.country_code
                        WHEN 'MX' THEN 'MEX'
                        WHEN 'US' THEN 'USA'
                   END AS pais_D,
                   D.postal_code AS cp_D,
                   D.location_type_id,
                   D.city_name AS cd_D,
                   D.ref_field2_txt,
                   --Campos auxiliaras para revisar cruces
            
                   l.dest_loc_id,
                   l.orig_loc_type,
                   l.dest_loc_type,
                   l.curr_load_status,
                   l.frt_move_catg_code,
                   p.stop_seq_nbr,
                   p.must_arrive_ts ,
                   f.shipment_type,
                   Co.costo,
                   concat ("OR00",l.orig_loc_id) AS IdOrigen_I,
                   concat ("DE00",p.location_id) AS IdDestino_I
            FROM LOAD l --Cargas
            
            LEFT JOIN load_stop p ON l.load_id=p.load_id
            AND l.dest_loc_id =p.location_id --Paradas
            
            LEFT JOIN freight_shipment f ON l.load_id=f.current_load_id
            AND l.dest_loc_id =f.dest_location_id --Envios
            
            LEFT JOIN carrier_expect c ON l.load_id=c.load_id
            AND c.last_change_ts IN
              (SELECT max(last_change_ts)
               FROM carrier_expect
               GROUP BY load_id) --Ultimo status de los envios
            
            LEFT JOIN facility_xref O ON l.orig_loc_id=O.location_id
            AND l.orig_loc_type=O.location_type_code AND O.country_code = 'MX'-- match Dirección Origen
            
            LEFT JOIN facility_xref D ON p.location_id=D.location_id
            AND p.location_type_code=D.location_type_code AND D.country_code = 'MX' -- match Dirección Destino
            
            LEFT JOIN
              (SELECT load_id,
                      sum(charge_amt) AS costo
               FROM load_std_charge
               GROUP BY load_id) Co ON Co.load_id = l.load_id--match para costo
            
            WHERE l.load_id IN ({loadid_})
            AND l.frt_move_catg_code NOT IN ('BDH','BKHDHD','DBLBDH','DBLDHD','DHD','PLTDHD')
            AND year(l.ready_ts)>1753 
            and year (l.last_change_ts)>1753 
            and (p.must_arrive_ts is null or year(p.must_arrive_ts)>1753)
            and year (c.arrival_ts)>1753
        """
        data_ = JdbcConn.get_pandas_df(sql= querySql)
        logging.info("Extracción de informix exitosa!")

        if len(data_) == 0:
            logging.info("No hay informacion recuperada de WTMS")
            return

        odbc_conn = PyODBCConnection('cartaporte_azuresql_odbc_gls').get_conn()
        logging.info('connected to {}'.format(type(odbc_conn)))
        cursor = odbc_conn.cursor()
        cursor.fast_executemany = True
        insert_wtms = 'INSERT INTO TT_WTMS_INBOUND VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
        cursor.executemany(insert_wtms, data_.values.tolist())
        cursor.commit()
        cursor.close()
        odbc_conn.close()
        logging.info('Finaliza insert en la base TT_WTMS_INBOUND')


    def get_tt_wtms_updates_inbound(**context):
        # Conexión a Azure
        odbc_conn = PyODBCConnection('cartaporte_azuresql_odbc_gls').get_conn()
        loadid = '''
                    SELECT MAX(u.last_change_ts) AS fechacambio
                    FROM tt_wtms_updates_inbound u WITH(NOLOCK)
                    WHERE u.frt_move_catg_code IN ('CP', 'HUB')
                    AND EXISTS (
                        SELECT *
                        FROM TT_PlantaCarnes tt WITH(NOLOCK)
                        WHERE tt.id_wtms = u.load_id 
                        AND tt.procesado = 0
                    )
                '''
        load_id = pd.read_sql_query(loadid, odbc_conn)
        logging.info("Extracción de AzureSQL exitosa!")

        # Selección de información de WTMS
        loadid_ = load_id['fechacambio'].tolist()
        if len(loadid_) == 0 or loadid_[0] is None:
            logging.info("No hay informacion por consultar en WTMS")
            return

        loadid_ = str(loadid_).replace('[', '').replace(']', '')
        loadid_ = str(loadid_).replace('Timestamp(', '').replace(')', '')

        # Conexión a Informix
        JdbcConn = JdbcHook(jdbc_conn_id='cartaporte_informix_wtms')
        querySql = f"""
            SELECT l.load_id,
                   l.orig_loc_id AS idOrigen,
                   p.location_id AS idDestino,
                   l.ready_ts AS FechaCargaOrigen,
                   l.trailer_id AS Placa,
                   CASE
                       WHEN c.arrival_ts IS NULL THEN
                              (SELECT max(must_arrive_ts)
                               FROM load_stop lp
                               WHERE lp.load_id = l.load_id
                               GROUP BY lp.load_id)
                       ELSE c.arrival_ts
                   END AS FechaCitaDestino,
                   p.dist_frm_prev_stop AS Distancia,
                   costo,
                   l.last_change_ts,
                   O.address_txt AS calle_O,
                   CASE O.state_prov_code
                       WHEN 'AG' THEN 'AGU'
                       WHEN 'BC' THEN 'BCN'
                       WHEN 'BS' THEN 'BCS'
                       WHEN 'CM' THEN 'CAM'
                       WHEN 'CS' THEN 'CHP'
                       WHEN 'CH' THEN 'CHH'
                       WHEN 'CO' THEN 'COA'
                       WHEN 'CL' THEN 'COL'
                       WHEN 'CX' THEN 'DIF'
                       WHEN 'DG' THEN 'DUR'
                       WHEN 'GT' THEN 'GUA'
                       WHEN 'GR' THEN 'GRO'
                       WHEN 'HG' THEN 'HID'
                       WHEN 'JA' THEN 'JAL'
                       WHEN 'EM' THEN 'MEX'
                       WHEN 'MI' THEN 'MIC'
                       WHEN 'MO' THEN 'MOR'
                       WHEN 'NA' THEN 'NAY'
                       WHEN 'NL' THEN 'NLE'
                       WHEN 'OA' THEN 'OAX'
                       WHEN 'PU' THEN 'PUE'
                       WHEN 'QT' THEN 'QUE'
                       WHEN 'QR' THEN 'ROO'
                       WHEN 'SL' THEN 'SLP'
                       WHEN 'SI' THEN 'SIN'
                       WHEN 'SO' THEN 'SON'
                       WHEN 'TB' THEN 'TAB'
                       WHEN 'TM' THEN 'TAM'
                       WHEN 'TL' THEN 'TLA'
                       WHEN 'VE' THEN 'VER'
                       WHEN 'YU' THEN 'YUC'
                       WHEN 'ZA' THEN 'ZAC'
                   END AS edo_O,
                   CASE O.country_code
                        WHEN 'MX' THEN 'MEX'
                        WHEN 'US' THEN 'USA'
                   END AS pais_O,
                   O.postal_code AS cp_O,
                   O.location_type_id,
                   O.city_name AS cd_O,
                   O.ref_field2_txt,
                   D.address_txt AS calle_D,
                   CASE D.state_prov_code
                       WHEN 'AG' THEN 'AGU'
                       WHEN 'BC' THEN 'BCN'
                       WHEN 'BS' THEN 'BCS'
                       WHEN 'CM' THEN 'CAM'
                       WHEN 'CS' THEN 'CHP'
                       WHEN 'CH' THEN 'CHH'
                       WHEN 'CO' THEN 'COA'
                       WHEN 'CL' THEN 'COL'
                       WHEN 'CX' THEN 'DIF'
                       WHEN 'DG' THEN 'DUR'
                       WHEN 'GT' THEN 'GUA'
                       WHEN 'GR' THEN 'GRO'
                       WHEN 'HG' THEN 'HID'
                       WHEN 'JA' THEN 'JAL'
                       WHEN 'EM' THEN 'MEX'
                       WHEN 'MI' THEN 'MIC'
                       WHEN 'MO' THEN 'MOR'
                       WHEN 'NA' THEN 'NAY'
                       WHEN 'NL' THEN 'NLE'
                       WHEN 'OA' THEN 'OAX'
                       WHEN 'PU' THEN 'PUE'
                       WHEN 'QT' THEN 'QUE'
                       WHEN 'QR' THEN 'ROO'
                       WHEN 'SL' THEN 'SLP'
                       WHEN 'SI' THEN 'SIN'
                       WHEN 'SO' THEN 'SON'
                       WHEN 'TB' THEN 'TAB'
                       WHEN 'TM' THEN 'TAM'
                       WHEN 'TL' THEN 'TLA'
                       WHEN 'VE' THEN 'VER'
                       WHEN 'YU' THEN 'YUC'
                       WHEN 'ZA' THEN 'ZAC'
                   END AS edo_D,
                   CASE D.country_code
                        WHEN 'MX' THEN 'MEX'
                        WHEN 'US' THEN 'USA'
                   END AS pais_D,
                   D.postal_code AS cp_D,
                   D.location_type_id,
                   D.city_name AS cd_D,
                   D.ref_field2_txt,
                   --Campos auxiliaras para revisar cruces

                   l.dest_loc_id,
                   l.orig_loc_type,
                   l.dest_loc_type,
                   l.curr_load_status,
                   l.frt_move_catg_code,
                   p.stop_seq_nbr,
                   p.must_arrive_ts ,
                   f.shipment_type,
                   total_case_cnt,
                   total_cubic_feet,
                   total_linear_feet,
                   total_pallet_qty,
                   total_slip_sht_qty,
                   total_weight_lbs,
                   pallet_stack_count
            FROM LOAD l --Cargas

            LEFT JOIN freight_shipment f ON l.load_id=f.current_load_id
            AND l.orig_loc_id =f.orig_location_id --l.dest_loc_id =f.dest_location_id --Envios
             --left join load_stop p on l.load_id=p.load_id and l.dest_loc_id =p.location_id --Paradas

            LEFT JOIN load_stop p ON p.load_id=f.current_load_id
            AND f.dest_location_id=p.location_id
            AND f.dest_location_type= p.location_type_code --Paradas

            LEFT JOIN carrier_expect c ON l.load_id=c.load_id
            AND c.last_change_ts IN
              ( SELECT max(last_change_ts)
               FROM carrier_expect
               GROUP BY load_id) --Ultimo status de los envios

            LEFT JOIN facility_xref O ON l.orig_loc_id=O.location_id
            AND l.orig_loc_type=O.location_type_code
            AND O.country_code='MX' -- match Dirección Origen

            LEFT JOIN facility_xref D ON p.location_id=D.location_id
            AND p.location_type_code=D.location_type_code
            AND D.country_code='MX' -- match Dirección Destino

            LEFT JOIN
              (SELECT load_id,
                      sum(charge_amt) AS costo
               FROM load_std_charge
               GROUP BY load_id) Co ON Co.load_id=l.load_id
            WHERE l.last_change_ts > ({loadid_})
            AND l.frt_move_catg_code NOT IN ('BDH','BKHDHD','DBLBDH','DBLDHD','DHD','PLTDHD')
            AND year(l.ready_ts)>1753 
            and year (l.last_change_ts)>1753 
            and (p.must_arrive_ts is null or year(p.must_arrive_ts)>1753)
            and year (c.arrival_ts)>1753
            -- AND shipment_type = 'INBND' -- Para Planta de carnes, en WTMS se guarda como OUTBND
            """

        logging.info(querySql)

        data_ = JdbcConn.get_pandas_df(sql=querySql)
        logging.info("Extracción de informix exitosa!")

        if len(data_) == 0:
            logging.info("No hay informacion recuperada de WTMS")
            return

        data_['process_date'] = todaydtStr

        odbc_conn = PyODBCConnection('cartaporte_azuresql_odbc_gls').get_conn()
        logging.info('connected to {}'.format(type(odbc_conn)))
        cursor = odbc_conn.cursor()
        cursor.fast_executemany = True
        insert_wtms = 'INSERT INTO TT_WTMS_UPDATES_INBOUND VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
        cursor.executemany(insert_wtms, data_.values.tolist())
        cursor.commit()
        cursor.close()
        odbc_conn.close()
        logging.info('Finaliza insert en la base TT_WTMS_UPDATES_INBOUND')


    def corre_sp(**context):
        odbc_conn = PyODBCConnection('cartaporte_azuresql_odbc_gls').get_conn()
        logging.info('connected to {}'.format(type(odbc_conn)))
        cursor = odbc_conn.cursor()
        sql_command = """ 
                        EXEC [sp_plantacarnes_inbound] 
                        EXEC [sp_plantacarnes_recoleccion_inbound]
                    """
        cursor.execute(sql_command)
        cursor.commit()
        cursor.close()
        odbc_conn.close()

    def update_mv_out_inb(**context):
        odbc_conn = PyODBCConnection('cartaporte_azuresql_odbc_gls').get_conn()
        logging.info('connected to {}'.format(type(odbc_conn)))
        cursor = odbc_conn.cursor()
        sql_command = """ 
                        EXEC [sp_delta_cartaporte_inbound_plantacarnes]
                        EXEC [sp_delta_cartaporte_inbound_plantacarnes_rec] 
                    """
        cursor.execute(sql_command)
        cursor.commit()
        cursor.close()
        odbc_conn.close()

    ##### Fin carga de datos de WTMS #####

    #Sección de lectura de archivo y escritura de base de datos

    upload_blobs = PythonOperator(
        task_id="{0}_upload_blobs".format(ORIGIN_CONTAINER_NAME),
        python_callable=upload_blobs,
        trigger_rule='none_skipped',
        provide_context=True,
        dag=dag
    )

    # Sección para importar y guardar datos de wtms

    factura = PythonOperator(
        task_id='factura',
        trigger_rule='none_skipped',
        python_callable=factura,
        provide_context=True,
        dag=dag
    )

    get_tt_wtms_updates_inbound = PythonOperator(
        task_id='get_tt_wtms_updates_inbound',
        python_callable=get_tt_wtms_updates_inbound,
        trigger_rule='none_skipped',
        provide_context=True,
        dag=dag
    )

    corre_sp = PythonOperator(
        task_id = 'run_sp_plantacarnes_inbound',
        trigger_rule='none_skipped',
        python_callable=corre_sp,
        provide_context=True,
        dag=dag
    )

    update_mv_out_inb = PythonOperator(
        task_id = 'update_mv_out_inb',
        trigger_rule='none_skipped',
        python_callable=update_mv_out_inb,
        provide_context=True,
        dag=dag
    )

    clean_xcom = PythonOperator(
        task_id="clean_xcom",
        python_callable=cleanup_xcom,
        dag=dag
    )

    upload_blobs >> factura >> get_tt_wtms_updates_inbound >> corre_sp >>update_mv_out_inb >> clean_xcom