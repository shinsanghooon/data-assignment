from datetime import datetime, timedelta
import json
import pendulum
import psycopg2
import gspread
import pandas as pd

from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.sensors.filesystem import FileSensor

local_tz = pendulum.timezone("Asia/Seoul")

order_file_path = '/opt/airflow/dags/data/orders.csv'
order_contents_file_path='/opt/airflow/dags/data/order_contents_information.csv'

# CSV 파일을 PostgreSQL에 로드하는 PythonOperator
def load_csv_to_postgres(file_path):
    hook = PostgresHook()
    table_name = file_path.split('/')[-1].split('.')[0]
    hook.copy_expert(f'COPY "torder".{table_name} FROM stdin WITH CSV HEADER', file_path)

# 전처리한 테이블에 대한 유효성을 검증
def validate():
    print('validate input data.')

# 작업 실패시 알람 처리
def alarm_failure():
    print('fail task. please check.')

# 작업 성공 시 구글 시트에 작성
def write_to_googlespread(data_type):

    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    hook = GoogleBaseHook()
    credentials = hook._get_credentials()
    google_credentials = gspread.Client(auth=credentials)

    sheet = google_credentials.open('torder')
    worksheet = sheet.worksheet(f'{data_type}_history')
    list_of_lists = worksheet.get_all_values()
    
    if(data_type=='daily'):
        data_type_kr = '일간'
    else:
        data_type_kr = '월간'

    if(len(list_of_lists)==0):
        use_columns = ['일자', '데이터 타입', '성공 여부', '메시지']
    else: 
        use_columns = list_of_lists[0]

    data = list_of_lists[1:]
    new_value = [now, data_type, '성공', f'{data_type_kr} 통계 정보 생성이 완료되었습니다.']

    data.append(new_value)
    worksheet.update([use_columns] + data)

with DAG(
    "torder",
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=3),
    },
    description="torder assignment",
    start_date=datetime(2023, 10, 28, 5, 0, 0, tzinfo=local_tz),
    schedule_interval=timedelta(days=1),
    on_failure_callback=alarm_failure,
    tags=["DAG"],
) as dag:

    initialize_all_tables = PostgresOperator(
        task_id="initialize_all_tables",
        sql="sql/initialize_all_tables.sql"
    )

    order_file_sensor = FileSensor(
        task_id="order_file_sensor",
        filepath=order_file_path,
        poke_interval=60, # 파일 생성까지 확인 주기(초)
        timeout=600,  # 파일이 생성까지 최대 대기 시간(초)
    )

    order_contents_file_sensor = FileSensor(
        task_id="order_contents_file_sensor",
        filepath=order_contents_file_path,
        poke_interval=60, # 파일 생성까지 확인 주기(초)
        timeout=600,  # 파일이 생성까지 최대 대기 시간(초)
    )

    create_order_table = PostgresOperator(
        task_id="create_order_table",
        sql="sql/create_order_table.sql"
    )

    create_order_contents_table = PostgresOperator(
        task_id="create_order_contents_table",
        sql="sql/create_order_contents_table.sql"
    )

    load_order_csv = PythonOperator(
        task_id='load_order_csv_to_postgres',
        python_callable=load_csv_to_postgres,
        op_kwargs={"file_path": order_file_path},
    )

    load_order_contents_csv = PythonOperator(
        task_id='load_order_contents_csv_to_postgres',
        python_callable=load_csv_to_postgres,
        op_kwargs={"file_path": order_contents_file_path},
    )

    validate_daily_data = PythonOperator(
        task_id='validate_daily_data',
        python_callable=validate
    )

    validate_monthly_data = PythonOperator(
        task_id='validate_monthly_data',
        python_callable=validate
    )

    create_base_daily_table = PostgresOperator(
        task_id="base_daily_table",
        sql="sql/analysis/create_base_daily_table.sql"
        
    )

    insert_daily_stat_overview = PostgresOperator(
        task_id="daily_stat_overview",
        sql="sql/analysis/create_daily_stat_overview.sql"
    )

    insert_daily_stat_by_store = PostgresOperator(
        task_id="daily_stat_by_store",
        sql="sql/analysis/create_daily_stat_by_store.sql"
    ) 

    insert_monthly_stat_overview = PostgresOperator(
        task_id="monthly_stat_overview",
        sql="sql/analysis/create_monthly_stat_overview.sql"
    )

    insert_monthly_stat_by_store = PostgresOperator(
        task_id="monthly_stat_by_store",
        sql="sql/analysis/create_monthly_stat_by_store.sql"
    )

    daily_data_to_google_sheet = PythonOperator(
        task_id="daily_data_to_google_sheet",
        python_callable=write_to_googlespread,
        op_kwargs={'data_type': 'daily'}
    )

    monthly_data_to_google_sheet = PythonOperator(
        task_id="monthly_data_to_google_sheet",
        python_callable=write_to_googlespread,
        op_kwargs={'data_type': 'monthly'}
    )

    initialize_all_tables >> [order_file_sensor, order_contents_file_sensor]
    order_file_sensor >> create_order_table >> load_order_csv >> create_base_daily_table
    order_contents_file_sensor >> create_order_contents_table >> load_order_contents_csv >> create_base_daily_table
    create_base_daily_table >> [insert_daily_stat_overview, insert_daily_stat_by_store, insert_monthly_stat_overview, insert_monthly_stat_by_store]
    [insert_daily_stat_overview, insert_daily_stat_by_store] >> validate_daily_data >> daily_data_to_google_sheet
    [insert_monthly_stat_overview, insert_monthly_stat_by_store] >> validate_monthly_data >> monthly_data_to_google_sheet