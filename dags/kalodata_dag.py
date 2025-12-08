from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum
import sys
import os

# --- 1. THÊM ĐƯỜNG DẪN ĐỂ ĐỌC FILE CONFIG ---
# Vì file config.py nằm trong thư mục scripts, ta cần trỏ đường dẫn vào đó
sys.path.append("/opt/airflow/scripts")
import config  # Import file config của bạn

local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

# --- 2. XỬ LÝ NGÀY THÁNG TỪ CONFIG ---
# Đọc ngày bắt đầu và kết thúc từ file config.py
try:
    start_dt = datetime.strptime(config.HISTORY_START_DATE, "%Y-%m-%d")
    end_dt = datetime.strptime(config.HISTORY_END_DATE, "%Y-%m-%d")
except Exception as e:
    # Phòng trường hợp quên cấu hình, dùng ngày mặc định
    print(f"Lỗi đọc ngày từ config: {e}. Dùng ngày mặc định.")
    start_dt = datetime(2025, 10, 1)
    end_dt = datetime(2025, 11, 30)

# Chuyển đổi sang múi giờ VN (quan trọng để Airflow chạy đúng giờ)
start_date_aware = pendulum.instance(start_dt).astimezone(local_tz)
end_date_aware = pendulum.instance(end_dt).astimezone(local_tz)

default_args = {
    'owner': 'quangtran',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    # Đổi tên ID để Airflow nhận diện đây là quy trình mới
    dag_id='kalodata_run_by_config', 
    
    default_args=default_args,
    description='Chạy dữ liệu theo khoảng thời gian cấu hình trong config.py',
    
    # --- 3. CẤU HÌNH QUAN TRỌNG ---
    # Lấy ngày Start/End động từ file config
    start_date=start_date_aware,
    end_date=end_date_aware,
    
    # Chạy mỗi ngày một lần (theo lịch sử)
    schedule_interval='@daily', 
    
    # BẮT BUỘC = True: Để nó tự động chạy bù những ngày trong quá khứ (từ 1/10 đến nay)
    catchup=True,  
    
    # Chỉ chạy 1 task tại 1 thời điểm (tránh sập RAM)
    max_active_runs=1, 
    
    tags=['kalodata', 'config-driven', 'backfill'],
) as dag:

    run_scraper = BashOperator(
        task_id='run_daily_script',
        
       
        bash_command='cd /opt/airflow/scripts && xvfb-run --auto-servernum --server-args="-screen 0 1280x1024x24" python -u kalodata.py {{ ds }}'
    )

    run_scraper