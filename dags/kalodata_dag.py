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
    
    dag_id='kalodata_crawler_daily', 
    default_args=default_args,
    description='Cào dữ liệu Kalodata định kỳ mỗi ngày một lần',
    
    start_date=datetime(2023, 1, 1, tzinfo=local_tz),
   
    # '0 0 * * *' nghĩa là: Phút 0, Giờ 0, Mọi ngày, Mọi tháng, Mọi thứ
    # Tức là chạy vào 00:00 hàng ngày
    schedule_interval='0 0 * * *', 
   
    catchup=False, # Không chạy bù quá khứ
    tags=['kalodata', 'selenium', 'daily'],
) as dag:

    run_scraper = BashOperator(
        task_id='run_crawler_daily',
        
        bash_command='cd /opt/airflow/scripts && xvfb-run --auto-servernum --server-args="-screen 0 1280x1024x24" python -u kalodata.py'
    )

    run_scraper