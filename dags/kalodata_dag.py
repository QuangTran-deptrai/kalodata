from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum

local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

default_args = {
    'owner': 'quangtran',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='kalodata_crawler_weekly',
    default_args=default_args,
    description='Cào dữ liệu Kalodata định kỳ 7 ngày/lần',
    start_date=datetime(2023, 1, 1, tzinfo=local_tz),
    schedule_interval='0 0 * * 0', # Chạy lúc 00:00 Chủ Nhật hàng tuần
    catchup=False, # Không chạy bù các ngày quá khứ
    tags=['kalodata', 'selenium'],
) as dag:

    
    run_scraper = BashOperator(
        task_id='run_crawler',
        bash_command='cd /opt/airflow/scripts && xvfb-run --auto-servernum --server-args="-screen 0 1280x1024x24" python kalodata.py'
    )

    run_scraper