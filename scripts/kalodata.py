import time
import os
import undetected_chromedriver as uc
import config
from sqlalchemy import create_engine, text
import pandas as pd
import json
import shutil
from DrissionPage import ChromiumPage, ChromiumOptions

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from urllib.parse import urlparse, parse_qs 
from selenium.common.exceptions import (
    NoSuchElementException, TimeoutException, WebDriverException, 
    StaleElementReferenceException, ElementClickInterceptedException
)
import sys

if len(sys.argv) > 1:
    
    current_run_date = sys.argv[1]
    
    print(f"\n>>> [AUTO] Đang thay đổi cấu hình filter sang ngày: {current_run_date}")
    
    
    config.FILTER_DATE_START = current_run_date
    config.FILTER_DATE_END = current_run_date
else:
    print(f">>> [MANUAL] Chạy theo cấu hình cứng trong file config: {config.FILTER_DATE_START}")

print(">>> [DEBUG] Script bat dau chay! Dang khoi dong Chrome...", flush=True)

DB_USER = os.getenv('DB_USER', 'root')
DB_PASS = os.getenv('DB_PASSWORD', '')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_NAME = os.getenv('DB_NAME', 'kalodata_db')

try:
    DB_CONNECTION_STR = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:3306/{DB_NAME}"
    db_engine = create_engine(DB_CONNECTION_STR)
except Exception as e:
    print(f" Lỗi cấu hình DB: {e}")
    db_engine = None



def scroll_to_and_click(driver, wait, xpath_selector):
    try:
        element = wait.until(EC.presence_of_element_located((By.XPATH, xpath_selector)))
        driver.execute_script("arguments[0].scrollIntoView(true);", element)
        time.sleep(0.5)
        clickable_element = wait.until(EC.element_to_be_clickable((By.XPATH, xpath_selector)))
        clickable_element.click()
    except Exception as e:
        print(f"    -> [Helper] Lỗi click: {xpath_selector}")
        raise e

def focus_on_tab(driver, text_name):
   
    try:
        # XPath tìm div tab đang active hoặc chưa active
        xpath = f"//div[contains(@class, 'cursor-pointer') and contains(text(), '{text_name}')]"
        tab = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, xpath)))
        tab.click()
        time.sleep(1.5) # Đợi web tự cuộn
        return True
    except:
        return False

def click_next_page_in_tab(driver, tab_name="all"):
   
    try:
        if tab_name.lower() == "creator":
            specific_xpath = "//div[contains(@class, 'labelCreator Info')]/ancestor::div[contains(@class, 'ant-table-wrapper')]//li[contains(@class, 'ant-pagination-next')]"
        elif tab_name.lower() == "video":
            specific_xpath = "//div[contains(@class, 'labelVideo Content')]/ancestor::div[contains(@class, 'ant-table-wrapper')]//li[contains(@class, 'ant-pagination-next')]"
        elif tab_name.lower() == "live":
            specific_xpath = "//div[contains(@class, 'labelLivestream Content')]/ancestor::div[contains(@class, 'ant-table-wrapper')]//li[contains(@class, 'ant-pagination-next')]"
        elif tab_name.lower() == "product_list": # Thêm case mới cho phân trang danh sách sản phẩm
            # Tìm nút Next trong container của bảng sản phẩm (trong tab Product cấp shop)
           
            # hoặc tiêu đề cột là 'labelProduct Info'
            specific_xpath = "//div[contains(@class, 'labelProduct Info')]/ancestor::div[contains(@class, 'ant-table-wrapper')]//li[contains(@class, 'ant-pagination-next')]"
        else: 
            specific_xpath = "//li[contains(@class, 'ant-pagination-next')]"

        btns = driver.find_elements(By.XPATH, specific_xpath)
        target_btn = None
        
        # Duyệt ngược từ cuối lên trong kết quả tìm được
        for btn in reversed(btns):
            if btn.is_displayed():
                # Kiểm tra xem có bị disable không
                classes = btn.get_attribute("class") or ""
                aria_disabled = btn.get_attribute("aria-disabled")
                if "disabled" not in classes and aria_disabled != "true":
                    target_btn = btn
                    break   
        
        if target_btn:
            # Scroll nhẹ để căn giữa nút
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", target_btn)
            time.sleep(0.5)
            try: 
                target_btn.click()
            except ElementClickInterceptedException:
                # Nếu bị che khuất, thử click bằng script
                driver.execute_script("arguments[0].click();", target_btn)
            
            print(f"          [Pagination - {tab_name}] >> Đang chuyển trang...")
            time.sleep(5) # Tăng từ 4 lên 5 hoặc hơn nếu cần
            
            return True
        
        return False 
    except Exception as e:
        print(f"          [Pagination - {tab_name}] Lỗi khi click Next: {e}")
        return False

def get_video_id_from_url(url):
    try: return parse_qs(urlparse(url).query)['id'][0]
    except: return None

def clean_shop_url(url):
    
    try:
        if not url: return url
        parsed = urlparse(url)
        
        shop_id = parse_qs(parsed.query).get('id', [None])[0]
        if shop_id:
            
            return f"https://www.kalodata.com/shop/detail?id={shop_id}"
    except:
        pass
    return url


def get_core_metrics(driver, wait):
    metrics_data = {}
    try:
        container_xpath = "//div[contains(@class, 'Layout-CoreMetrics')]"
        
    
        try: 
            wait.until(EC.visibility_of_element_located((By.XPATH, container_xpath)))
        except: 
            # Fallback scroll nhẹ nếu chưa thấy khung
            driver.execute_script("window.scrollBy(0, 300);")
            time.sleep(1)
            try: wait.until(EC.presence_of_element_located((By.XPATH, container_xpath)))
            except: return metrics_data

     
        try:
            def has_metric_data(d):
                # Tìm tất cả các thẻ value trong container
                vals = d.find_elements(By.XPATH, f"{container_xpath}//div[contains(@class, 'value')]")
                # Quét qua các thẻ value, nếu có thẻ nào có text (không rỗng) thì trả về True
                for v in vals:
                    # Lấy text ưu tiên hiển thị hoặc text ẩn
                    txt = v.text.strip() or v.get_attribute("textContent").strip()
                    # Nếu có text và text không phải dấu gạch ngang chờ (placeholder)
                    if txt and txt != "-" and txt != "N/A": 
                        return True
                return False
            
            # Đợi tối đa 10s cho điều kiện trên xảy ra
            wait.until(has_metric_data)
        except:
            
            time.sleep(2)

        
        time.sleep(2) 

        
        items = driver.find_elements(By.XPATH, f"{container_xpath}//div[contains(@class, 'item')]")
        for item in items:
            try:
                # Scroll tới từng item để đảm bảo nó nằm trong viewport (tránh lazy load)
                driver.execute_script("arguments[0].scrollIntoView({block: 'nearest', inline: 'start'});", item)
                
                label_el = item.find_element(By.XPATH, ".//div[contains(@class, 'label')]//div[contains(@class, 'line-clamp-2')]")
                value_el = item.find_element(By.XPATH, ".//div[contains(@class, 'value')]")
                
                label = label_el.get_attribute("textContent").strip()
                
                # Ưu tiên lấy text hiển thị, nếu rỗng thì lấy text ẩn trong DOM
                value = value_el.text.replace('\n', '').strip()
                if not value:
                    value = value_el.get_attribute("textContent").replace('\n', '').strip()
                
                if label: metrics_data[label] = value
            except: continue
    except Exception as e: 
        pass
    return metrics_data



data_store = {
    "Shop_Metrics": [], "Product_Metrics": [], "Creators": [], "Videos": [], "Lives": [], "creator_dim_shop": []            
}

sql_sync_cursors = {
    "Shop_Metrics": 0, "Product_Metrics": 0, "Creators": 0, 
    "Videos": 0, "Lives": 0, "creator_dim_shop": 0
}
processed_shops = set() 
done_names_creators = set() # (shop_link, product_name, creator_name)
done_links_creators = set() # (shop_link, product_name, tiktok_link)
done_names_videos = set() # (shop_link, product_name, item_title)
done_links_videos = set() # (shop_link, product_name, link)
done_names_lives = set() # (shop_link, product_name, item_title)
done_links_lives = set() # (shop_link, product_name, link)
done_shop_creators = set()



processed_creator_pages = set()  # (shop_link, product_name, page_number)
processed_video_pages = set()    # (shop_link, product_name, page_number)
processed_live_pages = set()     # (shop_link, product_name, page_number)
processed_product_pages = set()  # (shop_link, page_number) - Thêm dòng này
processed_shop_creator_pages = set()

def load_checkpoint():
    global processed_shops, data_store
    global done_names_creators, done_links_creators
    global done_names_videos, done_links_videos
    global done_names_lives, done_links_lives
    global done_shop_creators
    
    print(f"--- ĐANG KHỞI TẠO VÀ NẠP DỮ LIỆU CŨ... ---")
    
    # --- ƯU TIÊN 1: NẠP TỪ MYSQL ---
    if 'db_engine' in globals() and db_engine:
        try:
            print("    -> Đang kiểm tra dữ liệu từ Database...")
            date_filter_val = f"{config.FILTER_DATE_START} ~ {config.FILTER_DATE_END}"
            with db_engine.connect() as conn:
                # 1. Load SHOP đã xong 
                try:
                    query = text("SELECT `Shop Link` FROM shop_metrics WHERE `Date Filter` = :df")
                    result = conn.execute(query, {"df": date_filter_val}).fetchall()
                    for row in result:
                        if row[0]: 
                           
                            processed_shops.add(clean_shop_url(str(row[0])))
                    print(f"       + Đã nạp {len(result)} Shop từ DB.")
                except: pass

                # 2. Load SHOP CREATORS đã xong
                try:
                    query = text("SELECT `Shop Link`, `Creator Name` FROM shop_creators WHERE `Date Filter` = :df")
                    result = conn.execute(query, {"df": date_filter_val}).fetchall()
                    for row in result:
                        if row[0] and row[1]: 
                            
                            done_shop_creators.add((clean_shop_url(str(row[0])), str(row[1])))
                    print(f"       + Đã nạp {len(result)} Shop Creators từ DB.")
                except: pass

                # 3. Load PRODUCT CREATORS
                try:
                    query = text("SELECT `Shop Link`, `Product Name`, `Creator Name`, `TikTok Link` FROM product_creators WHERE `Date Filter` = :df")
                    result = conn.execute(query, {"df": date_filter_val}).fetchall()
                    for row in result:
                        
                        clean_link = clean_shop_url(str(row[0]))
                        done_names_creators.add((clean_link, str(row[1]), str(row[2])))
                        if row[3] and row[3] != 'N/A': 
                            done_links_creators.add((clean_link, str(row[1]), str(row[3])))
                except: pass

                # 4. Load VIDEOS
                try:
                    query = text("SELECT `Shop Link`, `Product Name`, `Item Title`, `Link` FROM videos WHERE `Date Filter` = :df")
                    result = conn.execute(query, {"df": date_filter_val}).fetchall()
                    for row in result:
                        
                        clean_link = clean_shop_url(str(row[0]))
                        if row[2]: done_names_videos.add((clean_link, str(row[1]), str(row[2])))
                        if row[3] and row[3] != 'N/A': done_links_videos.add((clean_link, str(row[1]), str(row[3])))
                except: pass

                # 5. Load LIVES
                try:
                    query = text("SELECT `Shop Link`, `Product Name`, `Item Title`, `Link` FROM lives WHERE `Date Filter` = :df")
                    result = conn.execute(query, {"df": date_filter_val}).fetchall()
                    for row in result:
                        
                        clean_link = clean_shop_url(str(row[0]))
                        if row[2]: done_names_lives.add((clean_link, str(row[1]), str(row[2])))
                        if row[3] and row[3] != 'N/A': done_links_lives.add((clean_link, str(row[1]), str(row[3])))
                except: pass
                
            print("    -> Hoàn tất nạp từ Database.")
            return # Nếu nạp DB thành công thì không cần đọc Excel nữa
        except Exception as e:
            print(f"    -> Lỗi đọc Database ({e}). Chuyển sang đọc file Excel...")

    # --- ƯU TIÊN 2: NẠP TỪ EXCEL  ---
    if os.path.exists(config.FILE_NAME):
        try:
            # Load Shop
            try:
                df_shop = pd.read_excel(config.FILE_NAME, sheet_name="Shop_Metrics")
                for _, row in df_shop.iterrows(): 
                    shop_link = row.get('Shop Link', row.get('Shop Name', 'N/A'))
                    if pd.notna(shop_link) and shop_link != 'nan': 
                        
                        processed_shops.add(clean_shop_url(str(shop_link)))
                data_store["Shop_Metrics"] = df_shop.to_dict('records')
            except: pass

            # Load Product
            try:
                df_prod = pd.read_excel(config.FILE_NAME, sheet_name="Product_Metrics")
                data_store["Product_Metrics"] = df_prod.to_dict('records')
            except: pass
            
            # Load Shop Creators
            try:
                df_sc = pd.read_excel(config.FILE_NAME, sheet_name="creator_dim_shop")
                data_store["creator_dim_shop"] = df_sc.to_dict('records')
                for _, row in df_sc.iterrows():
                    s_link = row.get('Shop Link', 'N/A')
                    c_name = row.get('Creator Name', 'N/A')
                    if pd.notna(c_name) and str(c_name) != 'nan':
                        
                        done_shop_creators.add((clean_shop_url(str(s_link)), str(c_name)))
            except: pass
            
            # Load Product Creators
            try:
                df_c = pd.read_excel(config.FILE_NAME, sheet_name="Creators")
                data_store["Creators"] = df_c.to_dict('records')
                for _, row in df_c.iterrows():
                    s_link = row.get('Shop Link', 'N/A')
                    p_name = row.get('Product Name', 'N/A') 
                    c_name = row.get('Creator Name', 'N/A')
                    t_link = row.get('TikTok Link', 'N/A')
                    
                    # [MODIFIED] Clean URL
                    clean_s_link = clean_shop_url(str(s_link))
                    done_names_creators.add((clean_s_link, str(p_name), str(c_name)))
                    if pd.notna(t_link) and str(t_link) != "N/A":
                        done_links_creators.add((clean_s_link, str(p_name), str(t_link)))
            except: pass
            
            # Load Videos
            try:
                df_v = pd.read_excel(config.FILE_NAME, sheet_name="Videos")
                data_store["Videos"] = df_v.to_dict('records')
                for _, row in df_v.iterrows():
                    s_link = row.get('Shop Link', 'N/A')
                    p_name = row.get('Product Name', 'N/A') 
                    title = row.get('Item Title', 'N/A')
                    link = row.get('Link', 'N/A')
                    
                    
                    clean_s_link = clean_shop_url(str(s_link))
                    if pd.notna(title): done_names_videos.add((clean_s_link, str(p_name), str(title)))
                    if pd.notna(link) and str(link) != "N/A": done_links_videos.add((clean_s_link, str(p_name), str(link)))
            except: pass

            # Load Lives
            try:
                df_l = pd.read_excel(config.FILE_NAME, sheet_name="Lives")
                data_store["Lives"] = df_l.to_dict('records')
                for _, row in df_l.iterrows():
                    s_link = row.get('Shop Link', 'N/A')
                    p_name = row.get('Product Name', 'N/A') 
                    title = row.get('Item Title', 'N/A')
                    link = row.get('Link', 'N/A')

                    
                    clean_s_link = clean_shop_url(str(s_link))
                    if pd.notna(title): done_names_lives.add((clean_s_link, str(p_name), str(title)))
                    if pd.notna(link) and str(link) != "N/A": done_links_lives.add((clean_s_link, str(p_name), str(link)))
            except: pass
            
            print(f"    -> Đã nạp dữ liệu từ file Excel.")
        except Exception as e: print(f"--- LỖI ĐỌC FILE EXCEL ({e}). ---")
    for key in sql_sync_cursors:
        if key in data_store:
            sql_sync_cursors[key] = len(data_store[key])
    print(f"    -> Đã đồng bộ vị trí con trỏ DB: {sql_sync_cursors}")

def save_checkpoint():
    try:
        # Kiểm tra xem có dữ liệu nào để lưu không
        has_data = any(data_store.values())
        if not has_data:
            print("    [System] Không có dữ liệu để lưu.")
            # Vẫn ghi file với tiêu đề cột nếu không có dữ liệu
            with pd.ExcelWriter(config.FILE_NAME, engine='openpyxl') as writer:
                pd.DataFrame(columns=['Shop Link', 'Date Filter']).to_excel(writer, sheet_name="Shop_Metrics", index=False)
                pd.DataFrame(columns=['Shop Link', 'Product Name', 'TikTok Product Link', 'Date Filter']).to_excel(writer, sheet_name="Product_Metrics", index=False)
                pd.DataFrame(columns=['Shop Link', 'Product Name', 'Product Link', 'Creator Name', 'TikTok Link', 'Date Filter']).to_excel(writer, sheet_name="Creators", index=False)
                pd.DataFrame(columns=['Shop Link', 'Product Name', 'Product Link', 'Creator Link', 'Link', 'Type', 'Revenue', 'Item Title', 'Date Filter']).to_excel(writer, sheet_name="Videos", index=False)
                pd.DataFrame(columns=['Shop Link', 'Product Name', 'Product Link', 'Creator Link', 'Revenue', 'Link', 'Item Title', 'Livestream Time', 'Duration', 'Avg Online Viewer', 'Views', 'Item Sold', 'Avg Unit Price', 'Date Filter']).to_excel(writer, sheet_name="Lives", index=False)
                pd.DataFrame(columns=['Shop Link', 'Creator Name', 'Account Type', 'TikTok Link', 'MCN', 'Debut Time', 'Creator Bio', 'Target Sexual', 'Target Age', 'Followers', 'Date Filter']).to_excel(writer, sheet_name="creator_dim_shop", index=False)
                pd.DataFrame(columns=['Shop Link', 'Product Name', 'Product Link']).to_excel(writer, sheet_name="product_Dim", index=False)
            return

        # --- ÁNH XẠ CREATOR LINK NGAY TRƯỚC KHI LƯU ---
        print("    [System] Bắt đầu ánh xạ Creator Link cho Videos và Lives trước khi lưu...")
        creator_link_map_final = {}
        for record in data_store.get("Creators", []):
            shop_link = str(record.get("Shop Link", "N/A")).strip().lower()
            prod_name = str(record.get("Product Name", "N/A")).strip().lower()
            creator_name_raw = str(record.get("Creator Name", "N/A")).strip().lower()
            creator_name_normalized = creator_name_raw.lstrip('@') 
            creator_link = record.get("TikTok Link", "N/A")
            key = (shop_link, prod_name, creator_name_normalized)
            if key not in creator_link_map_final:
                creator_link_map_final[key] = creator_link

        updated_videos_count = 0
        for record in data_store.get("Videos", []):
            shop_link = str(record.get("Shop Link", "N/A")).strip().lower()
            prod_name = str(record.get("Product Name", "N/A")).strip().lower()
            creator_name = str(record.get("Creator Name", "N/A")).strip().lower()
            creator_name_normalized = creator_name.lstrip('@')
            lookup_key = (shop_link, prod_name, creator_name_normalized)
            found_link = creator_link_map_final.get(lookup_key, "N/A")
            if found_link != "N/A":
                record["Creator Link"] = found_link
                updated_videos_count += 1

        updated_lives_count = 0
        for record in data_store.get("Lives", []):
            shop_link = str(record.get("Shop Link", "N/A")).strip().lower()
            prod_name = str(record.get("Product Name", "N/A")).strip().lower()
            creator_name = str(record.get("Creator Name", "N/A")).strip().lower()
            creator_name_normalized = creator_name.lstrip('@')
            lookup_key = (shop_link, prod_name, creator_name_normalized)
            found_link = creator_link_map_final.get(lookup_key, "N/A")
            if found_link != "N/A":
                record["Creator Link"] = found_link
                updated_lives_count += 1
        print(f"    [System] Hoàn tất ánh xạ. Cập nhật {updated_videos_count} Creator Link trong Videos và {updated_lives_count} Creator Link trong Lives.")
       

        # ==============================================================================
        # PHẦN 1: LƯU FILE EXCEL 
        # ==============================================================================
        try:
            with pd.ExcelWriter(config.FILE_NAME, engine='openpyxl') as writer:
                # --- Shop Metrics ---
                if data_store["Shop_Metrics"]: 
                    df_to_save = pd.DataFrame(data_store["Shop_Metrics"])
                    if 'Date Filter' not in df_to_save.columns: df_to_save['Date Filter'] = f"{config.FILTER_DATE_START} ~ {config.FILTER_DATE_END}"
                    df_to_save.drop_duplicates(subset=['Shop Link']).to_excel(writer, sheet_name="Shop_Metrics", index=False)
                else: pd.DataFrame(columns=['Shop Link', 'Date Filter']).to_excel(writer, sheet_name="Shop_Metrics", index=False)
                
                # --- Product Metrics ---
                if data_store["Product_Metrics"]: 
                    df_to_save = pd.DataFrame(data_store["Product_Metrics"])
                    if 'Date Filter' not in df_to_save.columns: df_to_save['Date Filter'] = f"{config.FILTER_DATE_START} ~ {config.FILTER_DATE_END}"
                    df_to_save.drop_duplicates(subset=['Shop Link', 'Product Name']).to_excel(writer, sheet_name="Product_Metrics", index=False)
                else: pd.DataFrame(columns=['Shop Link', 'Product Name', 'TikTok Product Link', 'Rating', 'Number of Reviews', 'Product SKUs', 'Date Filter']).to_excel(writer, sheet_name="Product_Metrics", index=False)
                
                # --- Creators ---
                if data_store["Creators"]: 
                    df_to_save = pd.DataFrame(data_store["Creators"])
                    if 'Date Filter' not in df_to_save.columns: df_to_save['Date Filter'] = f"{config.FILTER_DATE_START} ~ {config.FILTER_DATE_END}"
                    df_to_save.to_excel(writer, sheet_name="Creators", index=False)
                else: pd.DataFrame(columns=['Shop Link', 'Product Name', 'Product Link', 'Creator Name', 'TikTok Link', 'MCN', 'Debut Time', 'Creator Bio', 'Target Sexual', 'Target Age', 'Followers', 'Date Filter']).to_excel(writer, sheet_name="Creators", index=False)
                
                # --- Videos ---
                if data_store["Videos"]:
                    df_to_save = pd.DataFrame(data_store["Videos"])
                    if 'Date Filter' not in df_to_save.columns: df_to_save['Date Filter'] = f"{config.FILTER_DATE_START} ~ {config.FILTER_DATE_END}"
                    df_to_save.to_excel(writer, sheet_name="Videos", index=False)
                else: pd.DataFrame(columns=['Shop Link', 'Product Name', 'Product Link', 'Creator Link', 'Link', 'Type', 'Revenue', 'Item Title', 'Date Filter', 'Video Duration', 'Publish Date', 'Advertising Period (Days)', 'Views', 'Item Sold', 'New Followers Generated', 'Ad View Ratio', 'Ad Revenue Ratio', 'Ads Spending', 'Ad ROAS']).to_excel(writer, sheet_name="Videos", index=False)
                
                # --- Lives ---
                if data_store["Lives"]: 
                    df_to_save = pd.DataFrame(data_store["Lives"])
                    if 'Date Filter' not in df_to_save.columns: df_to_save['Date Filter'] = f"{config.FILTER_DATE_START} ~ {config.FILTER_DATE_END}"
                    df_to_save.to_excel(writer, sheet_name="Lives", index=False)
                else: pd.DataFrame(columns=['Shop Link', 'Product Name', 'Product Link', 'Creator Link', 'Revenue', 'Link', 'Item Title', 'Livestream Time', 'Duration', 'Avg Online Viewer', 'Views', 'Item Sold', 'Avg Unit Price', 'Date Filter']).to_excel(writer, sheet_name="Lives", index=False)

                # --- creator_dim_shop ---
                if data_store["creator_dim_shop"]:
                    df_to_save = pd.DataFrame(data_store["creator_dim_shop"])
                    if 'Date Filter' not in df_to_save.columns: df_to_save['Date Filter'] = f"{config.FILTER_DATE_START} ~ {config.FILTER_DATE_END}"
                    df_to_save.to_excel(writer, sheet_name="creator_dim_shop", index=False)
                else: pd.DataFrame(columns=['Shop Link', 'Creator Name', 'Account Type', 'TikTok Link', 'MCN', 'Debut Time', 'Creator Bio', 'Target Sexual', 'Target Age', 'Followers', 'Date Filter']).to_excel(writer, sheet_name="creator_dim_shop", index=False)

                # --- TẠO SHEET PRODUCT_DIM ---
                print("    [System] Bắt đầu tạo sheet product_Dim...")
                all_product_records = data_store.get("Product_Metrics", [])
                seen_combinations = set()
                unique_product_records = []
                for record in all_product_records:
                    shop_link = record.get("Shop Link", "N/A")
                    prod_name = record.get("Product Name", "N/A")
                    combination = (shop_link, prod_name)
                    if combination not in seen_combinations:
                        seen_combinations.add(combination)
                        unique_product_records.append(record)

                product_dim_data = []
                for record in unique_product_records: 
                    shop_link = record.get("Shop Link", "N/A")
                    prod_name = record.get("Product Name", "N/A")
                    prod_link = record.get("TikTok Product Link", "N/A")
                    product_dim_data.append({"Shop Link": shop_link, "Product Name": prod_name, "Product Link": prod_link})

                df_product_dim = pd.DataFrame(product_dim_data)
                if not df_product_dim.empty:
                    df_product_dim = df_product_dim[['Shop Link', 'Product Name', 'Product Link']]
                    
                df_product_dim.to_excel(writer, sheet_name="product_Dim", index=False)
                print(f"    [System] Hoàn tất tạo sheet product_Dim với {len(df_product_dim)} bản ghi duy nhất.")
        except Exception as e: 
            print(f"    [System] Lỗi lưu Excel: {e}")

        # ==============================================================================
        # PHẦN 2: [BƯỚC 1.2] LƯU VÀO MYSQL (INCREMENTAL SAVE)
        # ==============================================================================
        if 'db_engine' in globals() and db_engine:
            print("    [System] Đang đồng bộ dữ liệu MỚI sang MySQL...")
            
            # Mapping tên bảng
            table_map = {
                "Shop_Metrics": "shop_metrics",
                "creator_dim_shop": "shop_creators",
                "Product_Metrics": "product_metrics",
                "Creators": "product_creators",
                "Videos": "videos",
                "Lives": "lives"
            }

            for key, table_name in table_map.items():
                current_data_list = data_store.get(key, [])
                current_len = len(current_data_list)
                last_saved_idx = sql_sync_cursors.get(key, 0)

                # Chỉ lưu nếu có dữ liệu mới (len hiện tại > len lần lưu trước)
                if current_len > last_saved_idx:
                    # Cắt lấy phần dữ liệu mới chưa được lưu
                    new_records = current_data_list[last_saved_idx:]
                    
                    df = pd.DataFrame(new_records)
                    if 'Date Filter' not in df.columns:
                        df['Date Filter'] = f"{config.FILTER_DATE_START} ~ {config.FILTER_DATE_END}"
                    
                    # Chuyển đổi kiểu object thành string
                    for col in df.columns:
                        if df[col].dtype == object:
                            df[col] = df[col].astype(str)

                    try:
                        # Lưu vào DB (append phần mới)
                        df.to_sql(table_name, db_engine, if_exists='append', index=False, chunksize=500)
                        
                        
                        sql_sync_cursors[key] = current_len
                        print(f"      + Đã thêm {len(new_records)} dòng mới vào bảng {table_name}.")
                    except Exception as ex:
                        print(f"      - Lỗi lưu bảng {table_name}: {ex}")

           
            if 'product_dim_data' in locals() and product_dim_data:
                try:
                    df_dim = pd.DataFrame(product_dim_data)
                    if not df_dim.empty:
                        for col in df_dim.columns:
                            if df_dim[col].dtype == object: df_dim[col] = df_dim[col].astype(str)
                        
                        
                        df_dim.to_sql('product_dim', db_engine, if_exists='append', index=False)
                except Exception as ex:
                    
                    pass 
            
            print("    [System] Hoàn tất lưu Database.")

    except Exception as e: print(f"    [System] LỖI TỔNG SAVE CHECKPOINT: {e}")


def build_product_link_map():
    
    map_dict = {}
    for record in data_store.get("Product_Metrics", []):
        shop_link = record.get("Shop Link", "N/A")
        prod_name = record.get("Product Name", "N/A")
        prod_link = record.get("TikTok Product Link", "N/A")
       
        if (shop_link, prod_name) not in map_dict:
            map_dict[(shop_link, prod_name)] = prod_link
    return map_dict


def build_creator_link_map():
  
    map_dict = {}
    for record in data_store.get("Creators", []):
        shop_link = record.get("Shop Link", "N/A")
        prod_name = record.get("Product Name", "N/A")
        creator_name = record.get("Creator Name", "N/A")
        creator_link = record.get("TikTok Link", "N/A")
        
        if (shop_link, prod_name, creator_name) not in map_dict:
            map_dict[(shop_link, prod_name, creator_name)] = creator_link
    return map_dict


load_checkpoint()

def find_chrome_executable():
    """Tìm đường dẫn Chrome/Chromium executable"""
    # Các tên phổ biến trên Linux/Windows/Mac
    candidates = [
        "google-chrome", "chrome", "chromium", "chromium-browser",
        "google-chrome-stable", "/usr/bin/google-chrome", "/usr/bin/chromium",
        "C:/Program Files/Google/Chrome/Application/chrome.exe",
        "C:/Program Files (x86)/Google/Chrome/Application/chrome.exe"
    ]
    
    for candidate in candidates:
        path = shutil.which(candidate) or candidate
        if os.path.exists(path) and os.access(path, os.X_OK):
            return path
            
    # Nếu không tìm thấy, trả về None để DrissionPage tự xử (hoặc fail)
    return None

# --- DRISSION PAGE CLOUDFLARE SOLVER ---
def solve_cloudflare_with_drission():
    print(">>> [DrissionPage] Đang khởi động để bypass Cloudflare...")
    
    # Cấu hình DrissionPage
    co = ChromiumOptions()
    co.set_argument('--no-sandbox')
    co.set_argument('--disable-gpu')
    # [REVERT] Không fake UA Windows trên Linux nữa vì sẽ gây mismatch (OS fingerprint != UA)
    # co.set_argument('--user-agent=...') 
    
    # [FIX] Tìm và set đường dẫn browser cụ thể
    browser_path = find_chrome_executable()
    if browser_path:
        print(f"    -> Tìm thấy browser tại: {browser_path}")
        co.set_paths(browser_path=browser_path)
    else:
        print("    -> Không tìm thấy browser path cụ thể, dùng mặc định 'chrome'...")

    # co.headless(True) # Có thể bật headless nếu chạy trên server linux không có GUI
    
    try:
        page = ChromiumPage(co)
        
        target_url = "https://www.kalodata.com/shop"
        print(f">>> [DrissionPage] Truy cập: {target_url}")
        page.get(target_url)
        
        # Đợi Cloudflare (tự động xử lý bởi DrissionPage trong hầu hết trường hợp)
        print("    -> Đang kiểm tra Cloudflare...")
        
        # Loop check để click checkbox liên tục nếu cần
        for i in range(20): # Thử trong 60 giây (20 * 3s)
            if "Just a moment" not in page.title and "Cloudflare" not in page.title:
                print("    -> Đã vượt qua Cloudflare (Title OK).")
                break
                
            print(f"    -> [{i+1}/20] Vẫn còn Cloudflare, thử click...")
            
            try:
                # --- CHIẾN THUẬT TURNSTILE (USER SUGGESTION) ---
                # Tìm tất cả iframe
                iframes = page.eles("tag:iframe")
                for iframe in iframes:
                    # Lấy thuộc tính để nhận diện
                    title = (iframe.attr("title") or "").lower()
                    src = (iframe.attr("src") or "").lower()
                    
                    if "challenge" in title or "cloudflare" in title or "turnstile" in src:
                        print("      -> Tìm thấy Iframe Cloudflare/Turnstile! Đang xử lý...")
                        
                        # --- PHƯƠNG ÁN 1: Click trực diện (Nếu may mắn Shadow DOM mở) ---
                        try:
                            # Tìm checkbox trong iframe (DrissionPage tự xuyên iframe)
                            # Lưu ý: DrissionPage không cần switch_to.frame như Selenium
                            # Ta tìm trong body của iframe đó
                            iframe_body = iframe.ele("tag:body", timeout=1)
                            if iframe_body:
                                cb = iframe_body.ele("css:input[type='checkbox']", timeout=1)
                                if cb:
                                    print("      => [Cách 1] Đã click checkbox thành công!")
                                    cb.click()
                                    break
                        except: pass
                        
                        # --- PHƯƠNG ÁN 2: Click theo tọa độ (Blind Click) ---
                        try:
                            print("      => [Cách 2] Thử click theo tọa độ (20, 20) trong iframe...")
                            # Click vào body của iframe với offset
                            if iframe_body:
                                # DrissionPage click offset tính từ tâm element, nên cần tính toán lại hoặc dùng action chain
                                # Ở đây ta click vào góc trái trên của iframe body
                                # page.actions.move_to(iframe_body, offset_x=20, offset_y=20).click() -> Logic này khác Selenium
                                # Selenium (0,0) là góc trái trên. DrissionPage (0,0) là tâm.
                                # Để click góc trái trên (20,20), ta cần move tới top-left rồi offset
                                page.actions.move_to(iframe_body, offset_x=-iframe_body.rect.size[0]/2 + 20, offset_y=-iframe_body.rect.size[1]/2 + 20).click()
                        except Exception as e:
                            # print(f"Lỗi click tọa độ: {e}")
                            pass
                            
                        # --- PHƯƠNG ÁN 3: Dùng phím (Tab + Space) ---
                        try:
                            print("      => [Cách 3] Thử dùng phím TAB + SPACE...")
                            # Focus vào iframe trước
                            page.actions.move_to(iframe).click() 
                            time.sleep(0.2)
                            page.actions.key_down('Tab').key_up('Tab')
                            time.sleep(0.2)
                            page.actions.key_down('Space').key_up('Space')
                        except: pass
                        
                        time.sleep(1)

            except Exception as e: 
                # print(f"    -> Lỗi xử lý: {e}")
                pass
            
            time.sleep(3)
            
        print(f"    -> Tiêu đề sau khi xử lý: {page.title}")
        
        # [DEBUG] Nếu vẫn thất bại, in ra một phần HTML để debug
        if "Just a moment" in page.title or "Cloudflare" in page.title:
            print("!!! Vẫn chưa qua được Cloudflare. HTML dump (500 chars):")
            try: print(page.html[:500])
            except: pass
        
        # [FIX ROBUST] Lấy cookies an toàn
        cookies_dict = {}
        try:
            # Cách 1: DrissionPage v4 chuẩn (property)
            if hasattr(page, 'cookies') and hasattr(page.cookies, 'as_dict'):
                cookies_dict = page.cookies.as_dict()
            # Cách 2: DrissionPage cũ hoặc biến thể (method)
            elif callable(getattr(page, 'cookies', None)):
                try: cookies_dict = page.cookies(as_dict=True)
                except: cookies_dict = dict(page.cookies())
            # Cách 3: Property trả về list/dict trực tiếp
            elif isinstance(page.cookies, dict):
                cookies_dict = page.cookies
            elif isinstance(page.cookies, list):
                for c in page.cookies:
                    if isinstance(c, dict): cookies_dict[c.get('name')] = c.get('value')
            # Cách 4: Fallback cuối cùng
            else:
                cookies_dict = dict(page.cookies)
        except Exception as e:
            print(f"    -> Lỗi lấy cookie (thử fallback): {e}")
            try: 
                # [FIX] Sửa lỗi dictionary update sequence element #0 has length 3
                # Nếu page.cookies trả về list các object cookie, ta cần lấy name/value từ object đó
                cookies_dict = {}
                for c in page.cookies:
                    if hasattr(c, 'name') and hasattr(c, 'value'):
                        cookies_dict[c.name] = c.value
                    elif isinstance(c, dict):
                        cookies_dict[c.get('name')] = c.get('value')
            except: pass

        if not cookies_dict:
            print("!!! Không lấy được cookie nào. Có thể bypass thất bại.")
            print(">>> [DrissionPage] KHÔNG GHI ĐÈ FILE COOKIES.JSON. Giữ lại cookie cũ.")
            page.quit()
            return False
        
        # Chuyển đổi sang format của Selenium (list of dicts)
        selenium_cookies = []
        for name, value in cookies_dict.items():
            selenium_cookies.append({
                "name": name,
                "value": value,
                "domain": ".kalodata.com", # Set chung domain
                "path": "/"
            })
            
        # Lưu vào file
        with open("cookies.json", "w", encoding="utf-8") as f:
            json.dump(selenium_cookies, f, indent=4)
            
        print(f">>> [DrissionPage] Đã lưu {len(selenium_cookies)} cookies vào cookies.json")
        page.quit()
        return True
        
    except Exception as e:
        print(f"!!! [DrissionPage] Lỗi: {e}")
        try: page.quit()
        except: pass
        return False

# Chạy bypass trước khi vào Selenium
# Logic: Luôn chạy bypass để lấy cookie tươi, hoặc chỉ chạy khi không có cookie
# Ở đây ta chọn: Luôn chạy để đảm bảo 
# [USER REQUEST] Chuyển sang dùng Selenium Undetected hoàn toàn
# # [USER REQUEST] Chuyển sang dùng Selenium Undetected hoàn toàn
# solve_cloudflare_with_drission()

print("Đang khởi động trình duyệt Selenium...")
options = uc.ChromeOptions()


options.add_argument('--start-maximized')
options.add_argument('--no-sandbox') 
options.add_argument('--disable-dev-shm-usage') 
options.add_argument('--disable-gpu') 
# [NEW] Fake User-Agent theo yêu cầu user (Chrome 143)
options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36')
# options.add_argument('--headless=new') # KHÔNG BẬT HEADLESS, chúng ta sẽ dùng màn hình ảo (XVFB) để tránh bị phát hiện

# [NEW] Thêm profile để lưu session (tránh phải login lại nhiều lần và bypass Cloudflare tốt hơn)
script_dir = os.path.dirname(os.path.abspath(__file__))
profile_path = os.path.join(script_dir, "chrome_profile")

# [NEW] Xóa profile cũ để tránh lỗi cache/state bẩn
if os.path.exists(profile_path):
    try:
        print(f">>> [System] Đang xóa profile cũ để clean start: {profile_path}")
        shutil.rmtree(profile_path, ignore_errors=True)
    except Exception as e:
        print(f"!!! Không thể xóa profile cũ: {e}")

options.add_argument(f"--user-data-dir={profile_path}")

prefs = {"credentials_enable_service": False, "profile.password_manager_enabled": False}
options.add_experimental_option("prefs", prefs)

try:
    
    driver = uc.Chrome(options=options, version_main=142)
except Exception as e:
    print(f"Lỗi khởi động Chrome: {e}")
    
    sys.exit(1)

# --- BẮT ĐẦU: LOGIC NẠP COOKIES & ĐĂNG NHẬP THÔNG MINH ---
target_url = "https://www.kalodata.com/shop"

# 1. Truy cập domain để kích hoạt trình duyệt
print(f">>> [Cookies] Đang truy cập domain để nạp Cookies...")
driver.get("https://www.kalodata.com/404") 
time.sleep(3)

# 2. Đọc và nạp Cookies
try:
    cookie_path = "cookies.json"
    if os.path.exists(cookie_path):
        with open(cookie_path, 'r', encoding='utf-8') as f:
            cookies = json.load(f)
        print(f">>> [Cookies] Tìm thấy {len(cookies)} cookies. Đang inject...")
        for cookie in cookies:
            if 'sameSite' in cookie and cookie['sameSite'] not in ["Strict", "Lax", "None"]:
                del cookie['sameSite']
            if 'storeId' in cookie: del cookie['storeId']
            try: driver.add_cookie(cookie)
            except: pass
        print("    -> Đã nạp Cookies thành công!")
    else:
        print("!!! KHÔNG TÌM THẤY FILE cookies.json.")
except Exception as e:
    print(f"!!! LỖI NẠP COOKIES: {e}")

# 3. Vào trang đích
print(f">>> [System] Đang vào trang đích: {target_url}")
driver.get(target_url)

# [NEW] LOGIC BYPASS CLOUDFLARE TURNSTILE (SELENIUM)
print(">>> [Selenium] Đang kiểm tra Cloudflare Turnstile...")
time.sleep(5) # Chờ load

try:
    # Lặp kiểm tra iframe
    iframes = driver.find_elements(By.TAG_NAME, "iframe")
    print(f"    -> Tìm thấy {len(iframes)} iframe(s).")
    
    cf_iframe_found = False
    for idx, iframe in enumerate(iframes):
        iframe_title = str(iframe.get_attribute("title") or "").lower()
        iframe_src = str(iframe.get_attribute("src") or "").lower()
        iframe_id = str(iframe.get_attribute("id") or "").lower()
        
        print(f"       [{idx}] id='{iframe_id[:30]}' title='{iframe_title[:30]}' src='{iframe_src[:50]}'")
        
        # Mở rộng điều kiện detect
        if ("challenge" in iframe_title or "cloudflare" in iframe_title or 
            "turnstile" in iframe_src or "cf-" in iframe_id or "challenges.cloudflare" in iframe_src):
            print("    -> Phát hiện Iframe Cloudflare! Đang xâm nhập...")
            cf_iframe_found = True
            driver.switch_to.frame(iframe)
            time.sleep(1)
            
            # --- PHƯƠNG ÁN 1: Click trực diện ---
            try:
                checkbox = driver.find_element(By.CSS_SELECTOR, "input[type='checkbox']")
                checkbox.click()
                print("    => [Cách 1] Đã click checkbox thành công!")
                driver.switch_to.default_content()
                break
            except: pass
            
            # --- PHƯƠNG ÁN 2: Click theo tọa độ (Blind Click) ---
            try:
                print("    => [Cách 2] Thử click theo tọa độ (30, 30)...")
                action = ActionChains(driver)
                action.move_to_element_with_offset(driver.find_element(By.TAG_NAME, "body"), 30, 30).click().perform()
            except Exception as e: 
                print(f"       Lỗi click tọa độ: {e}")

            # --- PHƯƠNG ÁN 3: Dùng phím (Tab + Space) ---
            try:
                print("    => [Cách 3] Thử dùng phím TAB + SPACE...")
                action = ActionChains(driver)
                action.send_keys(Keys.TAB).send_keys(Keys.SPACE).perform()
            except: pass

            driver.switch_to.default_content()
            time.sleep(3)
            break # Thoát sau khi xử lý iframe đầu tiên
    
    if not cf_iframe_found and len(iframes) > 0:
        print("    -> Không tìm thấy iframe CF cụ thể. Thử click iframe đầu tiên có src...")
        for iframe in iframes:
            if iframe.get_attribute("src"):
                driver.switch_to.frame(iframe)
                try:
                    action = ActionChains(driver)
                    action.move_to_element_with_offset(driver.find_element(By.TAG_NAME, "body"), 30, 30).click().perform()
                    time.sleep(0.5)
                    action = ActionChains(driver)
                    action.send_keys(Keys.TAB).send_keys(Keys.SPACE).perform()
                except: pass
                driver.switch_to.default_content()
                time.sleep(2)
                break
            
except Exception as e:
    print(f"    -> Lỗi xử lý iframe: {e}")

# Check lại title
if "Just a moment" in driver.title or "Cloudflare" in driver.title:
    print("\n!!! PHÁT HIỆN CLOUDFLARE (Bypass chưa thành công) !!!")
    print(">>> Cookie không hợp lệ hoặc bị chặn IP. Đang chờ thêm 30s...")
    print(">>> Đang thử chờ thêm 30s xem có tự qua không...")
    time.sleep(30)
    
    if "Just a moment" in driver.title or "Cloudflare" in driver.title:
        print("!!! Vẫn bị kẹt ở Cloudflare. Dừng script để tránh treo.")
        # Chụp ảnh màn hình để debug
        try: driver.save_screenshot("cloudflare_stuck_final.png")
        except: pass
        sys.exit(1)

print(">>> Đã vượt qua Cloudflare (hoặc không bị chặn).")

# 4. QUYẾT ĐỊNH: CÓ CẦN ĐĂNG NHẬP KHÔNG?
# Nếu URL KHÔNG chứa chữ 'login' nghĩa là đã vào được Dashboard -> BỎ QUA ĐĂNG NHẬP
if "login" not in driver.current_url:
    print(">>> TUYỆT VỜI! ĐÃ VÀO THẲNG DASHBOARD (Bỏ qua bước Login)!")
    
else:
    # Chỉ chạy dòng này nếu Cookies hỏng và bị văng ra trang Login
    print("\n--- COOKIES HẾT HẠN - BẮT ĐẦU ĐĂNG NHẬP THỦ CÔNG ---")
    try:
       
        try:
            login_popup_btn = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//span[contains(text(), 'Log in / Sign up')]")))
            login_popup_btn.click()
        except:
            try: 
                login_popup_btn = driver.find_element(By.XPATH, "//div[contains(@class, 'V2-Components-Button')]")
                login_popup_btn.click()
            except: pass
            
        time.sleep(3)
        try: WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, "//div[text()='Log in']"))).click(); time.sleep(1)
        except: pass 

        phone_input = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.ID, "register_phone")))
        phone_input.clear()
        phone_input.send_keys(config.LOGIN_PHONE) 
        time.sleep(1)
        
        pass_input = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.ID, "register_password")))
        pass_input.clear()
        pass_input.send_keys(config.LOGIN_PASSWORD) 
        time.sleep(1)
        
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[@type='submit']"))).click()
        time.sleep(10)
        
        # Kiểm tra lại lần cuối
        if "login" in driver.current_url:
            raise Exception("Đăng nhập thất bại, vẫn ở trang login.")
            
    except Exception as e: 
        print(f"!!! LỖI ĐĂNG NHẬP NGHIÊM TRỌNG: {e}")
        driver.save_screenshot("login_fatal_error.png")
        sys.exit(1)

# --- KẾT THÚC LOGIC ĐĂNG NHẬP ---

wait = WebDriverWait(driver, 30)

# --- CLEAN POPUP ---
short_wait = WebDriverWait(driver, 5)
try: short_wait.until(EC.element_to_be_clickable((By.XPATH, "//div[starts-with(text(), 'Language')]/../a[contains(@class, 'close-icon')]"))).click(); time.sleep(1)
except: pass
try: short_wait.until(EC.element_to_be_clickable((By.XPATH, "//div[text()='Dark Mode is Now Available']/ancestor::div[contains(@class, 'ant-modal-root')]//button[contains(@class, 'ant-modal-close')]"))).click(); time.sleep(1)
except: pass
try: short_wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'tip-close')]"))).click(); time.sleep(1)
except: pass
try: short_wait.until(EC.element_to_be_clickable((By.XPATH, "//*[local-name()='path' and contains(@d, 'M28.2508')]/.."))).click(); time.sleep(1)
except: pass
try: short_wait.until(EC.element_to_be_clickable((By.XPATH, "//div[contains(@class, 'absolute') and .//*[local-name()='path' and @d='M5 5L19 19M5 19L19 5']]"))).click(); time.sleep(1)
except: pass

# --- FILTER DATA ---
print("Bắt đầu quá trình lọc...")
try:
    # 1. Click Last 30 Days
    wait.until(EC.element_to_be_clickable((By.XPATH, "//div[starts-with(text(), 'Last 30 Days')]"))).click()
    time.sleep(2) 
    
    # 2. Click Customize
    wait.until(EC.element_to_be_clickable((By.XPATH, "//div[text()='Customize']"))).click()
    time.sleep(2) 
    
    # 3. Click ngày bắt đầu (Dùng f-string để đưa biến config vào XPath)
    wait.until(EC.element_to_be_clickable((By.XPATH, f"//td[@title='{config.FILTER_DATE_START}']"))).click()
    time.sleep(2) 
    
    # 4. Click ngày kết thúc
    wait.until(EC.element_to_be_clickable((By.XPATH, f"//td[@title='{config.FILTER_DATE_END}']"))).click()
    time.sleep(2)
    
    # 5. Nhấn Escape để đóng picker
    ActionChains(driver).send_keys(Keys.ESCAPE).perform()
    time.sleep(2)
    
    # 6. Click vào Category để mở cascader
    category_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//div[text()='Category' and preceding-sibling::div[contains(@class, 'dark-filter')]]")))
    category_button.click()
    time.sleep(2) # Chờ menu mở ra

    # 7. Chọn các cấp trong cascader menu
    # Cấp 1: Baby & Maternity
    # Sử dụng XPath dự phòng ngay lập tức
    fallback_xpath1 = "(//div[contains(@class, 'ant-cascader-menu-item-content') and contains(text(), 'Baby & Maternity')])[1]"
    try:
        level1_item = wait.until(EC.element_to_be_clickable((By.XPATH, fallback_xpath1)))
        driver.execute_script("arguments[0].scrollIntoView(true);", level1_item)
        time.sleep(0.5)
        level1_item.click()
        print("    -> [Filter] Đã click 'Baby & Maternity' (Cấp 1) với XPath dự phòng.")
    except TimeoutException:
        print("    -> [Filter] Không tìm thấy 'Baby & Maternity' ở cấp 1 với XPath dự phòng.")
        raise
    time.sleep(3) # Chờ menu cấp 2 tải - tăng thời gian chờ

   
    print("    -> [Filter] Đang đợi menu cấp 2 xuất hiện...")
    
    wait.until(lambda d: len(d.find_elements(By.XPATH, "//div[contains(@class, 'ant-cascader-menu')]")) > 1)
    print("    -> [Filter] Đã phát hiện menu cấp 2.")

    # Cấp 2: Baby Care & Health
    # Sử dụng phương pháp tìm kiếm chung (dự phòng) ngay lập tức
    try:
        print("    -> [Filter] Đang đợi 'Baby Care & Health' trong menu cấp 2...")
        # Fallback: Có thể menu cấp 2 chưa hoàn toàn ổn định.
        # Thử đợi thêm một chút và kiểm tra lại.
        time.sleep(2)
        
        all_level2_candidates = driver.find_elements(By.XPATH, "//div[contains(@class, 'ant-cascader-menu-item-content') and contains(text(), 'Baby Care & Health')]")
        if all_level2_candidates:
            
            level2_item = all_level2_candidates[0]
            driver.execute_script("arguments[0].scrollIntoView(true);", level2_item)
            time.sleep(0.5)
            level2_item.click()
            print("    -> [Filter] Đã click 'Baby Care & Health' (Cấp 2) với cách dự phòng.")
        else:
            raise TimeoutException("Không tìm thấy phần tử 'Baby Care & Health' trong menu cấp 2.")
    except TimeoutException:
        print("    -> [Filter] Không tìm thấy 'Baby Care & Health' ở cấp 2 với phương pháp dự phòng.")
        raise

    time.sleep(3) 

    
    print("    -> [Filter] Đang đợi menu cấp 3 xuất hiện...")
    
    wait.until(lambda d: len(d.find_elements(By.XPATH, "//div[contains(@class, 'ant-cascader-menu')]")) > 2)
    print("    -> [Filter] Đã phát hiện menu cấp 3.")

    # Cấp 3: Baby Diapering Products
    try:
        print("    -> [Filter] Đang đợi 'Baby Diapering Products' trong menu cấp 3...")
        # Fallback: Tương tự cấp 2
        time.sleep(2)
        all_level3_candidates = driver.find_elements(By.XPATH, "//div[contains(@class, 'ant-cascader-menu-item-content') and contains(text(), 'Baby Diapering Products')]")
        if all_level3_candidates:
            level3_item = all_level3_candidates[0]
            driver.execute_script("arguments[0].scrollIntoView(true);", level3_item)
            time.sleep(0.5)
            level3_item.click()
            print("    -> [Filter] Đã click 'Baby Diapering Products' (Cấp 3) với cách dự phòng.")
        else:
            raise TimeoutException("Không tìm thấy phần tử 'Baby Diapering Products' trong menu cấp 3.")
    except TimeoutException:
        print("    -> [Filter] Không tìm thấy 'Baby Diapering Products' ở cấp 3 với phương pháp dự phòng.")
        raise

    time.sleep(2) 

    # 8. Click Apply trong cascader
    apply_btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//div[contains(@class, 'V2-Components-Button') and contains(text(), 'Apply')]")))
    apply_btn.click()
    time.sleep(2) 

    # 9. Click Submit
    submit_btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//span[text()='Submit']")))
    submit_btn.click()
    print(">>> ĐÃ NHẤP 'SUBMIT'. ĐANG TẢI DỮ LIỆU...")
    time.sleep(5) 
    
except Exception as e:
    print(f"!!! LỖI FILTER DATA: {type(e).__name__}: {e}")
    
    raise e 


try: 
    
    # [LOOP A] SHOP 
    total_shops_processed = 0
    shop_xpath = "//tbody[contains(@class, 'ant-table-tbody')]//tr/td[2]//div[contains(@class, 'line-clamp-1')]/span"

    while total_shops_processed < config.MAX_SHOPS:
        print(f"\n--- [Loop A] Đang tải danh sách Shop... ---")
        first_shop_name = None 
        try:
            shops_on_page = wait.until(EC.visibility_of_all_elements_located((By.XPATH, shop_xpath)))
            if shops_on_page: first_shop_name = shops_on_page[0].text 
            else: break 
        except: break 

        for i in range(len(shops_on_page)):
            if total_shops_processed >= config.MAX_SHOPS: break
            main_window = driver.current_window_handle
            shop_detail_window = None 

            try:
                # Lấy lại danh sách hàng để tránh Stale
                shops = wait.until(EC.presence_of_all_elements_located((By.XPATH, shop_xpath)))
                if i >= len(shops): break # Kiểm tra index an toàn
                
                curr_shop_el = shops[i]
                shop_name = curr_shop_el.text
                
                # --- CƠ CHẾ MỚI: Scroll và Click an toàn ---
                driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'nearest'});", curr_shop_el)
                time.sleep(0.5)
                
                print(f"\n--- [Shop {total_shops_processed + 1}] Đang xử lý: '{shop_name}' ---")
                try:
                    curr_shop_el.click()
                except ElementClickInterceptedException:
                    driver.execute_script("arguments[0].click();", curr_shop_el)
                # -------------------------------------------
            except Exception as e: 
                print(f"    [Loop A] Lỗi click shop: {e}")
                continue
            
            try:
                wait.until(EC.number_of_windows_to_be(2))
                shop_detail_window = [w for w in driver.window_handles if w != main_window][0]
                driver.switch_to.window(shop_detail_window)
                time.sleep(3) 
                
                # 1. Lấy Shop Link và Metrics Shop
                shop_link = clean_shop_url(driver.current_url)
                if shop_link not in processed_shops:
                    shop_metrics = get_core_metrics(driver, WebDriverWait(driver, 10))
                    shop_record = {"Shop Link": shop_link} 
                    shop_record.update({f"Shop_{k}": v for k, v in shop_metrics.items()})
                    shop_record["Date Filter"] = f"{config.FILTER_DATE_START} ~ {config.FILTER_DATE_END}"
                    data_store["Shop_Metrics"].append(shop_record)
                    processed_shops.add(shop_link)
                    save_checkpoint()
                
                
                
                # ==================================================================================
                # [BLOCK 0] SHOP CREATORS 
                # ==================================================================================
                print(f"    --- [Shop Level] Bắt đầu xử lý danh sách Creator của Shop... ---")
                
                if focus_on_tab(driver, "Creator"):
                    time.sleep(3)
                    shop_creator_wait = WebDriverWait(driver, 20)
                    current_shop_creator_page = 1
                    
                    while True:
                        print(f"    [Shop Creator - Page {current_shop_creator_page}] Đang tải danh sách...")
                        
                        if (shop_link, current_shop_creator_page) in processed_shop_creator_pages:
                            print(f"    [Shop Creator - Page {current_shop_creator_page}] đã xử lý (page check), bỏ qua...")
                            current_shop_creator_page += 1
                            if not click_next_page_in_tab(driver, "Creator"): break
                            continue

                        sc_row_xpath = "//div[contains(@class, 'labelCreator')]/ancestor::div[contains(@class, 'ant-table-wrapper')]//tbody[contains(@class, 'ant-table-tbody')]/tr[@data-row-key]"
                        sc_rows = []
                        try: sc_rows = shop_creator_wait.until(EC.presence_of_all_elements_located((By.XPATH, sc_row_xpath)))
                        except: 
                            print("    [Shop Creator] Không tìm thấy dữ liệu.")
                            break

                        for idx in range(len(sc_rows)):
                            try:
                                sc_rows = shop_creator_wait.until(EC.presence_of_all_elements_located((By.XPATH, sc_row_xpath)))
                                if idx >= len(sc_rows): break
                                curr_row = sc_rows[idx]
                                
                                driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'nearest'});", curr_row)
                                time.sleep(0.5)

                                # 1. Lấy Tên
                                sc_name_el = None
                                try:
                                    sc_name_el = curr_row.find_element(By.XPATH, ".//td[2]//div[contains(@class, 'line-clamp-1')]")
                                    sc_name = sc_name_el.text
                                except: continue 
                                
                                # Check Resume
                                if (str(shop_link), str(sc_name)) in done_shop_creators:
                                    continue

                                # 2. Lấy Account Type
                                sc_type = "N/A"
                                try: sc_type = curr_row.find_element(By.XPATH, "./td[3]").text.strip()
                                except: pass

                                # 3. Click
                                try: sc_name_el.click()
                                except: driver.execute_script("arguments[0].click();", sc_name_el)
                                
                                wait.until(EC.number_of_windows_to_be(3))
                                sc_tab = [w for w in driver.window_handles if w != main_window and w != shop_detail_window][0]
                                driver.switch_to.window(sc_tab)
                                time.sleep(2)

                                # --- LẤY CHI TIẾT (Bio, MCN, Followers...) ---
                                sc_tiktok_link = "N/A"; sc_mcn = "N/A"; sc_debut = "N/A"; sc_bio = "N/A"
                                sc_sex = "N/A"; sc_age = "N/A"; sc_fol = "N/A"
                                try:
                                    try: sc_tiktok_link = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, "//a[contains(@href, 'tiktok.com/@')]"))).get_attribute("href")
                                    except: pass
                                    try: sc_mcn = driver.find_element(By.XPATH, "//div[contains(@class, 'mcn')]//div[contains(@class, 'text-base-666')]").text
                                    except: pass
                                    try: 
                                        dt_full = driver.find_element(By.XPATH, "//div[contains(@class, 'text-base-666') and contains(., 'Debut Time : ')]").text
                                        sc_debut = dt_full.split(': ', 1)[1].strip() if ': ' in dt_full else dt_full
                                    except: pass
                                    
                                    try: # Bio
                                        bio_div = driver.find_element(By.XPATH, "//div[contains(text(), 'Creator Bio:')]")
                                        bio_content = bio_div.find_element(By.XPATH, "./following-sibling::div[1] | ../div[contains(@class, 'flex-1')]")
                                        sc_bio = bio_content.text.replace('\n', ' ')
                                    except:
                                        try: # Bio Fallback
                                            bio_con = driver.find_element(By.XPATH, "//div[contains(@class, 'flex') and contains(@class, 'items-center') and contains(@class, 'gap-[2px]')]//div[contains(@class, 'flex-1') and contains(@class, 'text-base-666')]")
                                            sc_bio = bio_con.text.replace('\n', ' ')
                                        except: pass

                                    try: sc_sex = driver.find_element(By.XPATH, "//span[contains(text(), 'Majority') or contains(text(), 'Gender Balance')]").text
                                    except: pass
                                    try: sc_age = driver.find_element(By.XPATH, "//div[contains(@class, 'flex') and contains(@class, 'gap-[4px]')]//span[contains(text(), '-')]").text
                                    except: pass
                                    
                                    
                                    try: 
                                        # Tìm hàng chứa chữ Followers
                                        fol_row = driver.find_element(By.XPATH, "//div[contains(text(), 'Followers')]/ancestor::div[contains(@class, 'table-row')]")
                                        row_text = fol_row.text.replace('\n', ' ') # Followers 500
                                        
                                        import re
                                        # Regex tìm số (có thể có phẩy/chấm) và optional (k/m)
                                        # Tìm cụm số xuất hiện sau chữ Followers
                                        match = re.search(r'Followers\s*([\d,.]+[kKmM]?)', row_text, re.IGNORECASE)
                                        if match:
                                            sc_fol = match.group(1)
                                        else:
                                            # Fallback: Tìm cụm số bất kỳ trong dòng đó
                                            match_any = re.search(r'([\d,.]+[kKmM]?)', row_text.replace('Followers', ''))
                                            if match_any: sc_fol = match_any.group(1)
                                    except: pass
                                except: pass

                                # 4. Lưu & Thêm vào danh sách đã làm
                                data_store["creator_dim_shop"].append({
                                    "Shop Link": shop_link, "Creator Name": sc_name, "Account Type": sc_type,
                                    "TikTok Link": sc_tiktok_link, "MCN": sc_mcn, "Debut Time": sc_debut,
                                    "Creator Bio": sc_bio, "Target Sexual": sc_sex, "Target Age": sc_age,
                                    "Followers": sc_fol, "Date Filter": f"{config.FILTER_DATE_START} ~ {config.FILTER_DATE_END}"
                                })
                                
                                done_shop_creators.add((str(shop_link), str(sc_name)))
                                print(f"    + Shop Creator: {sc_name} | Type: {sc_type} | Fol: {sc_fol}")

                                driver.close()
                                driver.switch_to.window(shop_detail_window)
                                
                            except Exception as e:
                                if len(driver.window_handles) > 2: 
                                    driver.close()
                                    driver.switch_to.window(shop_detail_window)

                        processed_shop_creator_pages.add((shop_link, current_shop_creator_page))
                        save_checkpoint()

                        if not click_next_page_in_tab(driver, "Creator"): 
                            print("    [Shop Creator] Hết danh sách.")
                            break
                        current_shop_creator_page += 1
                        time.sleep(3)
                else:
                    print("    [Shop Creator] Không tìm thấy tab Creator hoặc lỗi chuyển tab.")

                shop_wait = WebDriverWait(driver, 20)
                focus_on_tab(driver, "Product")
                time.sleep(5)
                
                # [LOOP B] PRODUCT LIST PAGINATION 
                
                # 1. Định nghĩa XPath
                prod_table_xpath = "//div[contains(@class, 'labelProduct Info')]/ancestor::div[contains(@class, 'ant-table-wrapper')]//tbody[contains(@class, 'ant-table-tbody')]"
                prod_row_xpath = f"{prod_table_xpath}/tr[@data-row-key]"
                # XPath dùng để check chuyển trang (tìm tên sản phẩm trong bảng)
                prod_name_check_xpath = f"{prod_row_xpath}//td[2]//div[contains(@class, 'line-clamp-2')]"
                
                current_product_page = 1
                while True: 
                    print(f"    [Tab 2 - Product Page {current_product_page}] Đang tải danh sách sản phẩm... ---")
                   
                    if (shop_link, current_product_page) in processed_product_pages: 
                        print(f"    [Tab 2 - Product Page {current_product_page}] đã xử lý, chuyển trang tiếp theo...")
                        current_product_page += 1
                        focus_on_tab(driver, "Product") 
                        time.sleep(1)
                        if not click_next_page_in_tab(driver, "product_list"): 
                            print("    [Tab 2] Hết trang Product.")
                            break 
                        else:
                            continue 

                    # 2. Lấy danh sách HÀNG và Tên SP đầu tiên (để check next page)
                    products_rows = []
                    first_prod_name = None # <--- Khai báo biến còn thiếu
                    try:
                        products_rows = shop_wait.until(EC.presence_of_all_elements_located((By.XPATH, prod_row_xpath)))
                        # Lấy tên sản phẩm đầu tiên ngay khi load trang để làm mốc so sánh
                        if products_rows:
                            try:
                                first_el = products_rows[0].find_element(By.XPATH, ".//td[2]//div[contains(@class, 'line-clamp-2')]")
                                first_prod_name = first_el.text.strip()
                            except: pass
                    except:
                        print("    [Tab 2] Không tìm thấy sản phẩm nào trên trang.")
                        break 

                    # --- BẮT ĐẦU LẶP QUA CÁC SẢN PHẨM ---
                    for j in range(len(products_rows)):
                        prod_tab = None
                        try:
                            # Lấy lại rows để tránh lỗi Stale
                            products_rows = shop_wait.until(EC.presence_of_all_elements_located((By.XPATH, prod_row_xpath)))
                            if j >= len(products_rows): break
                            curr_prod_row = products_rows[j]

                            # [CƠ CHẾ MỚI] SCROLL VÀO HÀNG
                            driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'nearest'});", curr_prod_row)
                            time.sleep(0.5)
                            
                            # Lấy tên và Click
                            prod_name_el = curr_prod_row.find_element(By.XPATH, ".//td[2]//div[contains(@class, 'line-clamp-2')]")
                            prod_name = prod_name_el.text.replace("\n", " ").strip()
                            print(f"    --- [Prod {j+1}] Đang cào: '{prod_name[:20]}...' ---")
                            
                            try:
                                prod_name_el.click()
                            except ElementClickInterceptedException:
                                driver.execute_script("arguments[0].click();", prod_name_el)

                        except Exception as e:
                            print(f"    --- [Prod {j+1}] Lỗi click sản phẩm: {e}")
                            continue
                        
                        try:
                            shop_wait.until(EC.number_of_windows_to_be(3))
                            prod_tab = [w for w in driver.window_handles if w != main_window and w != shop_detail_window][0]
                            driver.switch_to.window(prod_tab)
                            time.sleep(3)
                            product_wait = WebDriverWait(driver, 20) 
                            
                            # 1. Lấy Core Metrics
                            prod_metrics = get_core_metrics(driver, WebDriverWait(driver, 10))

                            # 2. Lấy TikTok Link
                            tiktok_product_link = "N/A"
                            try: 
                                tiktok_product_link = product_wait.until(EC.presence_of_element_located((By.XPATH, "//a[.//span[contains(text(), 'View on TikTok')]]"))).get_attribute("href")
                            except: pass

                            # 3. Lấy Rating, Reviews, SKUs
                            p_rating = "N/A"; p_reviews = "N/A"; p_skus = "N/A"
                            import re 
                            try:
                                rating_el = product_wait.until(EC.presence_of_element_located((By.XPATH, "//span[contains(., '/5')]")))
                                p_rating = rating_el.text.split('/')[0].strip()
                                reviews_el = driver.find_element(By.XPATH, "//span[contains(., 'Reviews:')]")
                                rev_match = re.search(r'Reviews:\s*(\d+)', reviews_el.text)
                                if rev_match: p_reviews = rev_match.group(1)
                                sku_el = driver.find_element(By.XPATH, "//div[contains(@class, 'product-sku')]")
                                sku_match = re.search(r'\((\d+)\)', sku_el.text)
                                if sku_match: p_skus = sku_match.group(1)
                                print(f"          >>> Info: Rating {p_rating} | Reviews {p_reviews} | SKUs {p_skus}")
                            except: pass

                            # 4. Tổng hợp và Lưu
                            prod_record = {
                                "Shop Link": shop_link, "Product Name": prod_name, 
                                "TikTok Product Link": tiktok_product_link,
                                "Rating": p_rating, "Number of Reviews": p_reviews, "Product SKUs": p_skus,
                                "Date Filter": f"{config.FILTER_DATE_START} ~ {config.FILTER_DATE_END}"
                            }
                            prod_record.update({f"Product_{k}": v for k, v in prod_metrics.items()})
                            data_store["Product_Metrics"].append(prod_record)
                          
                            link_data = {"Shop Link": shop_link, "Product Name": prod_name, "Date Filter": f"{config.FILTER_DATE_START} ~ {config.FILTER_DATE_END}"} 
                            temp_creator_map = {} 
                           
                            # BLOCK 1: CREATOR (CƠ CHẾ MỚI: AN TOÀN & CHÍNH XÁC)
                            print("        >>> [BLOCK 1] BẮT ĐẦU XỬ LÝ CREATOR...")
                            focus_on_tab(driver, "Creator") 
                            time.sleep(3) 
                            
                            current_page = 1
                            while True: 
                                if (shop_link, prod_name, current_page) in processed_creator_pages: 
                                    print(f"          [Creator] Trang {current_page} đã xử lý, chuyển trang tiếp theo...")
                                    current_page += 1
                                    focus_on_tab(driver, "Creator")
                                    if not click_next_page_in_tab(driver, "Creator"): 
                                        print("          [Creator] Hết trang Creator.")
                                        break
                                    continue

                                # [FIX] XPath chỉ tìm hàng trong bảng 'Creator Info'
                                c_row_xpath = "//div[contains(@class, 'labelCreator')]/ancestor::div[contains(@class, 'ant-table-wrapper')]//tbody[contains(@class, 'ant-table-tbody')]/tr[@data-row-key]"
                                c_rows = []
                                try: 
                                    c_rows = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.XPATH, c_row_xpath)))
                                except: 
                                    # Fallback nếu không tìm thấy (có thể do chưa load kịp hoặc hết data)
                                    focus_on_tab(driver, "Creator") 
                                    time.sleep(1) 
                                    if not click_next_page_in_tab(driver, "Creator"): 
                                        print("          [Creator] Hết trang Creator hoặc không có dữ liệu.")
                                        break
                                    else: 
                                        current_page += 1
                                        continue 

                                for k in range(len(c_rows)):
                                    try:
                                        # Lấy lại rows để tránh Stale
                                        c_rows = product_wait.until(EC.presence_of_all_elements_located((By.XPATH, c_row_xpath)))
                                        if k >= len(c_rows): break
                                        curr = c_rows[k]

                                        # [FIX] Scroll an toàn đến hàng hiện tại
                                        driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'nearest'});", curr)
                                        time.sleep(0.5)
                                        
                                        c_name_el = curr.find_element(By.XPATH, ".//td[2]//div[contains(@class, 'line-clamp-1')]")
                                        c_name = c_name_el.text
                                        
                                        if (str(shop_link), str(prod_name), str(c_name)) in done_names_creators: 
                                            continue
                                        
                                        # Click an toàn
                                        try:
                                            c_name_el.click()
                                        except ElementClickInterceptedException:
                                            driver.execute_script("arguments[0].click();", c_name_el)

                                        product_wait.until(EC.number_of_windows_to_be(4))
                                        creat_tab = [w for w in driver.window_handles if w not in [main_window, shop_detail_window, prod_tab]][0]
                                        driver.switch_to.window(creat_tab); time.sleep(2)
                                        
                                        # --- LẤY THÔNG TIN CHI TIẾT CREATOR (Giữ nguyên logic cũ đã tốt) ---
                                        c_link = "N/A"; c_mcn = "N/A"; c_debut_time = "N/A"; c_bio = "N/A"
                                        c_target_sexual = "N/A"; c_target_age = "N/A"; c_followers = "N/A"

                                        try: 
                                            # 1. TikTok Link
                                            try: c_link = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, "//a[contains(@href, 'tiktok.com/@')]"))).get_attribute("href")
                                            except: pass
                                            # 2. MCN
                                            try: c_mcn = driver.find_element(By.XPATH, "//div[contains(@class, 'mcn')]//div[contains(@class, 'text-base-666')]").text
                                            except: pass
                                            # 3. Debut Time
                                            try: 
                                                dt_full = driver.find_element(By.XPATH, "//div[contains(@class, 'text-base-666') and contains(., 'Debut Time : ')]").text
                                                c_debut_time = dt_full.split(': ', 1)[1].strip() if ': ' in dt_full else dt_full
                                            except: pass
                                            # 4. Bio
                                            try:
                                                bio_div = driver.find_element(By.XPATH, "//div[contains(text(), 'Creator Bio:')]")
                                                bio_content = bio_div.find_element(By.XPATH, "./following-sibling::div[1] | ../div[contains(@class, 'flex-1')]")
                                                c_bio = bio_content.text.replace('\n', ' ')
                                            except: 
                                                # Fallback Bio
                                                try:
                                                    bio_con = driver.find_element(By.XPATH, "//div[contains(@class, 'flex') and contains(@class, 'items-center')]//div[contains(@class, 'flex-1') and contains(@class, 'text-base-666')]")
                                                    c_bio = bio_con.text.replace('\n', ' ')
                                                except: pass
                                            # 5. Target Sex
                                            try: c_target_sexual = driver.find_element(By.XPATH, "//span[contains(text(), 'Majority') or contains(text(), 'Gender Balance')]").text
                                            except: pass
                                            # 6. Target Age
                                            try: c_target_age = driver.find_element(By.XPATH, "//div[contains(@class, 'flex') and contains(@class, 'gap-[4px]')]//span[contains(text(), '-')]").text
                                            except: pass
                                            # 7. Followers
                                            try: 
                                                fol_el = driver.find_element(By.XPATH, "//div[contains(text(), 'Followers')]/ancestor::div[contains(@class, 'table-row')]//div[contains(text(), 'k') or contains(text(), 'm')]")
                                                c_followers = fol_el.text
                                            except: pass
                                        except: pass

                                        # --- LƯU DỮ LIỆU ---
                                        if (str(shop_link), str(prod_name), str(c_link)) in done_links_creators: 
                                            driver.close(); driver.switch_to.window(prod_tab)
                                            done_names_creators.add((str(shop_link), str(prod_name), str(c_name)))
                                            continue

                                        product_link_map = build_product_link_map()
                                        prod_link = product_link_map.get((shop_link, prod_name), "N/A")
                                        temp_creator_map[c_name] = c_link

                                        row_data = link_data.copy()
                                        row_data.update({
                                            "Product Link": prod_link,
                                            "Creator Name": c_name, 
                                            "TikTok Link": c_link,
                                            "MCN": c_mcn,
                                            "Debut Time": c_debut_time,
                                            "Creator Bio": c_bio,
                                            "Target Sexual": c_target_sexual,
                                            "Target Age": c_target_age,
                                            "Followers": c_followers
                                        })
                                        data_store["Creators"].append(row_data)
                                        
                                        done_names_creators.add((str(shop_link), str(prod_name), str(c_name)))
                                        done_links_creators.add((str(shop_link), str(prod_name), str(c_link)))
                                        print(f"          + Creator: {c_name}, MCN: {c_mcn}, Debut: {c_debut_time}, Target: {c_target_sexual}, Age: {c_target_age}, Followers: {c_followers}")
                                        
                                        driver.close(); driver.switch_to.window(prod_tab)
                                    except Exception as e:
                                        if len(driver.window_handles) > 3: driver.close(); driver.switch_to.window(prod_tab)

                                processed_creator_pages.add((shop_link, prod_name, current_page)) 
                                save_checkpoint()

                                if not click_next_page_in_tab(driver, "Creator"): 
                                    print("          [Creator] Hết trang Creator.")
                                    break 
                                current_page += 1
                                time.sleep(5) 
                                try:
                                    
                                    first_row_xpath = "//tbody[contains(@class, 'ant-table-tbody')]/tr[@data-row-key][1]"
                                    
                                    first_row_element = product_wait.until(
                                        EC.presence_of_element_located((By.XPATH, first_row_xpath))
                                    )
                                    
                                    driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", first_row_element)
                                    
                                    time.sleep(1)
                                except:
                                    
                                    print("          [Creator] Không tìm thấy hàng đầu tiên, thử focus tab.")
                                    focus_on_tab(driver, "Creator")
                                    time.sleep(1)


                            
                            # BLOCK 2: VIDEO (CƠ CHẾ MỚI: AN TOÀN & CHÍNH XÁC)
                            print("        >>> [BLOCK 2] BẮT ĐẦU XỬ LÝ VIDEO...")
                            focus_on_tab(driver, "Video & Ad")
                            time.sleep(3)

                            current_page = 1
                            while True:
                                if (shop_link, prod_name, current_page) in processed_video_pages: 
                                    print(f"          [Video] Trang {current_page} đã xử lý, chuyển trang tiếp theo...")
                                    current_page += 1
                                    focus_on_tab(driver, "Video & Ad")
                                    if not click_next_page_in_tab(driver, "Video"): 
                                        print("          [Video] Hết trang Video.")
                                        break
                                    continue

                                # [FIX] XPath chỉ tìm hàng trong bảng 'Video' (bắt được cả 'labelVideo Content')
                                vid_full_xpath = "//div[contains(@class, 'labelVideo')]/ancestor::div[contains(@class, 'ant-table-wrapper')]//tbody[contains(@class, 'ant-table-tbody')]/tr[@data-row-key]"
                                v_rows = []
                                try: 
                                    v_rows = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.XPATH, vid_full_xpath)))
                                except: 
                                    focus_on_tab(driver, "Video & Ad") 
                                    time.sleep(1) 
                                    if not click_next_page_in_tab(driver, "Video"): 
                                        print("          [Video] Hết trang Video hoặc không có dữ liệu.")
                                        break
                                    else: 
                                        current_page += 1
                                        continue

                                for k in range(len(v_rows)):
                                    try:
                                        # Lấy lại rows để tránh Stale
                                        v_rows = product_wait.until(EC.presence_of_all_elements_located((By.XPATH, vid_full_xpath)))
                                        if k >= len(v_rows): break
                                        curr = v_rows[k]

                                        # [FIX] Scroll an toàn
                                        driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'nearest'});", curr)
                                        time.sleep(0.5)
                                        
                                        v_title_element = curr.find_element(By.XPATH, ".//td[2]//span[contains(@class, 'line-clamp-2')]")
                                        v_title_check = v_title_element.text

                                        if (str(shop_link), str(prod_name), str(v_title_check)) in done_names_videos: 
                                            continue

                                        v_type = "AD" if curr.find_elements(By.XPATH, ".//*[local-name()='g' and @id='AD']") else "Normal"

                                        try: 
                                            v_rev_element = curr.find_element(By.XPATH, "./td[3]")
                                            v_rev = v_rev_element.text
                                        except: 
                                            v_rev = "N/A"

                                        # Click an toàn
                                        try:
                                            v_title_element.click()
                                        except ElementClickInterceptedException:
                                            driver.execute_script("arguments[0].click();", v_title_element)

                                        product_wait.until(EC.number_of_windows_to_be(4))
                                        vid_tab = [w for w in driver.window_handles if w not in [main_window, shop_detail_window, prod_tab]][0]
                                        driver.switch_to.window(vid_tab); time.sleep(2)

                                        # --- LẤY THÔ TIN TỪ TRANG CHI TIẾT VIDEO ---
                                        v_duration = "N/A"; v_publish_date = "N/A"; v_ad_period_days = "N/A"
                                        v_views = "N/A"; v_item_sold = "N/A"; v_new_followers = "N/A"
                                        v_ad_view_ratio = "N/A"; v_ad_rev_ratio = "N/A"; v_ads_spending = "N/A"; v_ad_roas = "N/A"

                                        try:
                                            # 1. Duration và Publish Date (Đã dùng bản vá lỗi XPath chính xác)
                                            try:
                                                pub_xpath = "//div[contains(@class, 'text-base-999') and contains(., 'Publish Date')]"
                                                publish_date_label = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, pub_xpath)))
                                                v_publish_date = publish_date_label.find_element(By.XPATH, "./following-sibling::div[1]").text

                                                dur_xpath = "//div[contains(@class, 'text-base-999') and contains(., 'Duration')]"
                                                duration_label = driver.find_element(By.XPATH, dur_xpath)
                                                v_duration = duration_label.find_element(By.XPATH, "./following-sibling::div[1]").text
                                            except: pass

                                            # 2. Ad Period
                                            try:
                                                ad_period_element = driver.find_element(By.XPATH, "//div[contains(@class, 'info') and contains(text(), 'Days')]")
                                                import re
                                                days_match = re.search(r'(\d+)\s*Days', ad_period_element.text, re.IGNORECASE)
                                                if days_match: v_ad_period_days = days_match.group(1)
                                            except: pass

                                            # 3. Metrics (Views, Sales...)
                                            try:
                                                # Views
                                                views_el = driver.find_element(By.XPATH, "//div[contains(@class, 'labelViews')]/following::div[contains(@class, 'value')][1]//div[contains(@class, 'block')]")
                                                v_views_raw = views_el.text
                                                match = re.search(r'([\d,.]+)([kKmM]?)', v_views_raw)
                                                v_views = (match.group(1) + (match.group(2) or "")) if match else v_views_raw
                                            except: pass

                                            try: v_item_sold = driver.find_element(By.XPATH, "//div[contains(@class, 'labelSales')]/following::div[contains(@class, 'value')][1]//div[contains(@class, 'block')]").text
                                            except: pass
                                            try: v_new_followers = driver.find_element(By.XPATH, "//div[contains(@class, 'labelNew Followers')]/following::div[contains(@class, 'value')][1]//div[contains(@class, 'block')]").text
                                            except: pass
                                            try: v_ad_view_ratio = driver.find_element(By.XPATH, "//div[contains(@class, 'labelAd View Ratio')]/following::div[contains(@class, 'value')][1]//div[contains(@class, 'block')]").text
                                            except: pass
                                            try: v_ad_rev_ratio = driver.find_element(By.XPATH, "//div[contains(@class, 'labelAd Revenue Ratio')]/following::div[contains(@class, 'value')][1]//div[contains(@class, 'block')]").text
                                            except: pass
                                            try: v_ads_spending = driver.find_element(By.XPATH, "//div[contains(@class, 'labelAd Spend')]/following::div[contains(@class, 'value')][1]//div[contains(@class, 'block')]").text
                                            except: pass
                                            try: v_ad_roas = driver.find_element(By.XPATH, "//div[contains(@class, 'labelAd ROAS')]/following::div[contains(@class, 'value')][1]//div[contains(@class, 'block')]").text
                                            except: pass
                                        except: pass

                                        # --- LƯU DỮ LIỆU ---
                                        try:
                                            v_creat_element = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'text-ellipsis1') and contains(@class, 'font-medium')]")))
                                            v_creat_raw = v_creat_element.text 
                                            v_creat_normalized = v_creat_raw.lstrip('@') 
                                        except:
                                            v_creat_raw = "N/A"; v_creat_normalized = "N/A"

                                        v_id = get_video_id_from_url(driver.current_url)
                                        v_link = f"https://www.tiktok.com/@{v_creat_raw}/video/{v_id}" if v_id else "N/A" 

                                        if (str(shop_link), str(prod_name), str(v_link)) in done_links_videos and v_link != "N/A": 
                                            driver.close(); driver.switch_to.window(prod_tab)
                                            done_names_videos.add((str(shop_link), str(prod_name), str(v_title_check))) 
                                            continue

                                        product_link_map = build_product_link_map()
                                        prod_link = product_link_map.get((shop_link, prod_name), "N/A")
                                        creator_link = "N/A" 

                                        row_data = link_data.copy()
                                        row_data.update({
                                            "Product Link": prod_link, 
                                            "Creator Name": v_creat_normalized, "Creator Link": creator_link, 
                                            "Link": v_link, "Type": v_type, "Revenue": v_rev, "Item Title": v_title_check,
                                            "Video Duration": v_duration, "Publish Date": v_publish_date,
                                            "Advertising Period (Days)": v_ad_period_days, "Views": v_views, "Item Sold": v_item_sold,
                                            "New Followers Generated": v_new_followers, "Ad View Ratio": v_ad_view_ratio,
                                            "Ad Revenue Ratio": v_ad_rev_ratio, "Ads Spending": v_ads_spending, "Ad ROAS": v_ad_roas
                                        })
                                        data_store["Videos"].append(row_data)

                                        done_names_videos.add((str(shop_link), str(prod_name), str(v_title_check))) 
                                        done_links_videos.add((str(shop_link), str(prod_name), str(v_link))) 
                                        print(f"          + Video: {v_creat_raw} (Normalized: {v_creat_normalized}) - {v_title_check[:30]}...") 

                                        driver.close(); driver.switch_to.window(prod_tab)
                                    except Exception as e:
                                        if len(driver.window_handles) > 3: 
                                            driver.close(); driver.switch_to.window(prod_tab)

                                processed_video_pages.add((shop_link, prod_name, current_page)) 
                                save_checkpoint()

                                if not click_next_page_in_tab(driver, "Video"): 
                                    print("          [Video] Hết trang Video.")
                                    break
                                current_page += 1
                                time.sleep(5) 
                                try:
                                    first_row_xpath = "//tbody[contains(@class, 'ant-table-tbody')]/tr[@data-row-key][1]"
                                    first_row_element = product_wait.until(
                                        EC.presence_of_element_located((By.XPATH, first_row_xpath))
                                    )
                                    driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", first_row_element)
                                    time.sleep(1)
                                except:
                                    print("          [Video] Không tìm thấy hàng đầu tiên, thử focus tab.")
                                    focus_on_tab(driver, "Video & Ad")
                                    time.sleep(1)


                                                        
                            # BLOCK 3: LIVE (CƠ CHẾ MỚI: AN TOÀN & CHÍNH XÁC)
                            print("        >>> [BLOCK 3] BẮT ĐẦU XỬ LÝ LIVE...")
                            focus_on_tab(driver, "Live")
                            time.sleep(3) 
                            
                            current_page = 1
                            while True:
                                if (shop_link, prod_name, current_page) in processed_live_pages: 
                                    print(f"          [Live] Trang {current_page} đã xử lý, chuyển trang tiếp theo...")
                                    current_page += 1
                                    focus_on_tab(driver, "Live")
                                    if not click_next_page_in_tab(driver, "Live"): 
                                        print("          [Live] Hết trang Live.")
                                        break
                                    continue

                                # [FIX] XPath chỉ tìm hàng trong bảng 'Livestream Content'
                                l_full_xpath = "//div[contains(@class, 'labelLivestream')]/ancestor::div[contains(@class, 'ant-table-wrapper')]//tbody[contains(@class, 'ant-table-tbody')]/tr[@data-row-key]"
                                l_rows = []
                                try: 
                                    l_rows = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.XPATH, l_full_xpath)))
                                except: 
                                    focus_on_tab(driver, "Live") 
                                    time.sleep(1) 
                                    if not click_next_page_in_tab(driver, "Live"): 
                                        print("          [Live] Hết trang Live hoặc không có dữ liệu.")
                                        break
                                    else: 
                                        current_page += 1
                                        continue

                                for k in range(len(l_rows)):
                                    try:
                                        # Lấy lại danh sách để tránh lỗi Stale Element
                                        l_rows = product_wait.until(EC.presence_of_all_elements_located((By.XPATH, l_full_xpath)))
                                        if k >= len(l_rows): break
                                        curr = l_rows[k]
                                      
                                        # [FIX] Scroll an toàn
                                        driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'nearest'});", curr)
                                        time.sleep(0.5)

                                        l_title_element = curr.find_element(By.XPATH, ".//div[contains(@class, 'line-clamp-2')]")
                                        l_title_check = l_title_element.text
                                        
                                        try: 
                                            l_rev = curr.find_element(By.XPATH, "./td[4]").text
                                        except: 
                                            l_rev = "N/A"
                                        
                                        # Click an toàn
                                        try:
                                            l_title_element.click()
                                        except ElementClickInterceptedException:
                                            driver.execute_script("arguments[0].click();", l_title_element)

                                        product_wait.until(EC.number_of_windows_to_be(4))
                                        live_tab = [w for w in driver.window_handles if w not in [main_window, shop_detail_window, prod_tab]][0]
                                        driver.switch_to.window(live_tab); time.sleep(2)
                                        
                                        l_wait = WebDriverWait(driver, 10)

                                        # --- GỌI HÀM LẤY METRICS CHI TIẾT ---
                                        live_metrics = get_core_metrics(driver, l_wait)
                                        
                                        l_avg_viewer = live_metrics.get("Avg. Online Viewer", "N/A")
                                        l_views = live_metrics.get("Views", "N/A")
                                        l_item_sold = live_metrics.get("Item Sold", "N/A")
                                        l_avg_unit_price = live_metrics.get("Avg. Unit Price", "N/A")

                                        l_creat_raw = "N/A"; l_time = "N/A"; l_dur = "N/A"
                                        try: 
                                            l_creat_element = l_wait.until(EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'text-ellipsis1') and contains(@class, 'font-medium') and contains(@class, 'mb-1')]")))
                                            l_creat_raw = l_creat_element.text
                                            l_creat_normalized = l_creat_raw.lstrip('@') 
                                        except: 
                                            l_creat_normalized = "N/A"
                                        
                                        # Xử lý Link Live
                                        kalodata_live_url = driver.current_url
                                        l_link = "N/A" 
                                        parsed_kalodata_url = urlparse(kalodata_live_url)
                                        if parsed_kalodata_url.netloc == "www.kalodata.com" and '/livestream/detail' in parsed_kalodata_url.path:
                                            query_params = parse_qs(parsed_kalodata_url.query)
                                            room_id = query_params.get('id', [None])[0] 
                                            if room_id and l_creat_raw != "N/A":
                                                l_link = f"https://www.tiktok.com/@{l_creat_raw}/live?room_id={room_id}"
                                       
                                        # Lấy thông tin thời gian và thời lượng
                                        try: 
                                            l_time_element = l_wait.until(EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'table')]//div[contains(@class, 'table-row')][2]/span")))
                                            l_time = l_time_element.text
                                        except: pass
                                        try: 
                                            l_dur_element = l_wait.until(EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'table')]//div[contains(@class, 'table-row')][2]/div[contains(@class, 'table-cell')]")))
                                            l_dur = l_dur_element.text
                                        except: pass
                                        
                                        product_link_map = build_product_link_map()
                                        prod_link = product_link_map.get((shop_link, prod_name), "N/A")
                                        creator_link = "N/A" 

                                        # --- LƯU DỮ LIỆU ---
                                        row_data = link_data.copy()
                                        row_data.update({
                                            "Product Link": prod_link,
                                            "Creator Name": l_creat_normalized, "Creator Link": creator_link, 
                                            "Revenue": l_rev, "Link": l_link, "Item Title": l_title_check, 
                                            "Livestream Time": l_time, "Duration": l_dur,
                                            "Avg Online Viewer": l_avg_viewer, "Views": l_views,
                                            "Item Sold": l_item_sold, "Avg Unit Price": l_avg_unit_price
                                        })
                                        data_store["Lives"].append(row_data)
                                        
                                        print(f"          + Live: {l_creat_raw} - {l_title_check[:20]}... | View: {l_views} | Sold: {l_item_sold}")
                                        
                                        driver.close(); driver.switch_to.window(prod_tab)
                                    except Exception as e:
                                        if len(driver.window_handles) > 3: 
                                            driver.close(); driver.switch_to.window(prod_tab)

                                processed_live_pages.add((shop_link, prod_name, current_page)) 
                                save_checkpoint()

                                if not click_next_page_in_tab(driver, "Live"): 
                                    print("          [Live] Hết trang Live.")
                                    break
                                current_page += 1
                                time.sleep(5) 
                               
                                try:
                                    
                                    first_row_xpath = "//tbody[contains(@class, 'ant-table-tbody')]/tr[@data-row-key][1]"
                                    first_row_element = product_wait.until(
                                        EC.presence_of_element_located((By.XPATH, first_row_xpath))
                                    )
                                    driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", first_row_element)
                                    time.sleep(1)
                                except:
                                    print("          [Live] Không tìm thấy hàng đầu tiên, thử focus tab.")
                                    focus_on_tab(driver, "Live")
                                    time.sleep(1)

                      
                            save_checkpoint()
                            
                        finally:
                            if len(driver.window_handles) > 2: driver.close()
                            if shop_detail_window: driver.switch_to.window(shop_detail_window)
                    
                    # Checkpoint Product Page
                    processed_product_pages.add((shop_link, current_product_page)) 
                    save_checkpoint()
                    
                    # --- XỬ LÝ CHUYỂN TRANG (NEXT PAGE) ---
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(1)
                    focus_on_tab(driver, "Product") 
                    time.sleep(1)
                    if not click_next_page_in_tab(driver, "product_list"): 
                        print("    [Tab 2] Hết trang Product.")
                        break 
                    else:
                        current_product_page += 1
                        # Kiểm tra xem trang mới đã load chưa bằng cách so sánh tên SP đầu tiên
                        if first_prod_name:
                            try: shop_wait.until(lambda d: d.find_element(By.XPATH, prod_name_check_xpath).text != first_prod_name)
                            except: pass
                        time.sleep(3) 

            finally:
                if len(driver.window_handles) > 1: driver.close()
                driver.switch_to.window(main_window)
                total_shops_processed += 1
        
        if total_shops_processed >= config.MAX_SHOPS: break
        
        
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)
        if not click_next_page_in_tab(driver, "all"): 
            print("--- [Loop A] Hết trang Shop.")
            break
        else:
            if first_shop_name:
                try: wait.until(lambda d: d.find_element(By.XPATH, shop_xpath).text != first_shop_name)
                except: pass
            time.sleep(5)

    print("    >>> Bắt đầu ánh xạ Creator Link cho Videos và Lives...")
    
    creator_link_map_final = {}
    for record in data_store.get("Creators", []):
       
        shop_link = str(record.get("Shop Link", "N/A")).strip().lower() 
        prod_name = str(record.get("Product Name", "N/A")).strip().lower() 
    
        creator_name_raw = str(record.get("Creator Name", "N/A")).strip().lower() 
        creator_name_normalized = creator_name_raw.lstrip('@')
      
        creator_link = record.get("TikTok Link", "N/A")
        key = (shop_link, prod_name, creator_name_normalized) 
        
        if key not in creator_link_map_final:
            creator_link_map_final[key] = creator_link

   
    for record in data_store.get("Videos", []):
       
        shop_link = str(record.get("Shop Link", "N/A")).strip().lower()
        prod_name = str(record.get("Product Name", "N/A")).strip().lower()
        creator_name = str(record.get("Creator Name", "N/A")).strip().lower() 
        
        creator_name_normalized = creator_name.lstrip('@')
        
        lookup_key = (shop_link, prod_name, creator_name_normalized)
        found_link = creator_link_map_final.get(lookup_key, "N/A")
        
        record["Creator Link"] = found_link if found_link != "N/A" else record.get("Creator Link", "N/A")

    
    for record in data_store.get("Lives", []):
       
        shop_link = str(record.get("Shop Link", "N/A")).strip().lower()
        prod_name = str(record.get("Product Name", "N/A")).strip().lower()
        creator_name = str(record.get("Creator Name", "N/A")).strip().lower() 
  
        creator_name_normalized = creator_name.lstrip('@')
      
        lookup_key = (shop_link, prod_name, creator_name_normalized)
        found_link = creator_link_map_final.get(lookup_key, "N/A")
     
        record["Creator Link"] = found_link if found_link != "N/A" else record.get("Creator Link", "N/A")
    print("    >>> Hoàn tất ánh xạ Creator Link.")
  
    print("\n--- ĐÃ HOÀN THÀNH TẤT CẢ ---")     

except Exception as e: 
    print(f"\n!!! LỖI: {e}")
    try: driver.save_screenshot("final_error.png")
    except: pass

finally: 
    save_checkpoint()
    time.sleep(5)
    try: driver.quit()
    except: pass

