import os
from dotenv import load_dotenv


load_dotenv()


FILE_NAME = "kalodata_master.xlsx"
BASE_URL = "https://www.kalodata.com/shop"

FILTER_DATE_START = "2025-11-10"
FILTER_DATE_END = "2025-11-11"


MAX_SHOPS = 2


LOGIN_PHONE = os.getenv("KALO_PHONE")
LOGIN_PASSWORD = os.getenv("KALO_PASSWORD")


if not LOGIN_PHONE or not LOGIN_PASSWORD:
    raise ValueError(" Chưa cấu hình KALO_PHONE hoặc KALO_PASSWORD trong file .env")