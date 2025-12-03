
## 🌟 Tính Năng Nổi Bật 

### 1. 🛡️ Hệ thống Vận hành Ổn định & Thông minh
* **Dockerized:** Toàn bộ hệ thống (Code, Chrome, MySQL, Airflow) chạy trong container cô lập, đảm bảo "chạy là đúng" trên mọi máy.
* **Anti-Crash Chrome:** Cấu hình bộ nhớ chia sẻ (`shm_size: 2gb`) và cửa sổ ảo tối ưu, khắc phục hoàn toàn lỗi sập trình duyệt khi tải trang nặng.
* **Smart Resume:** Tự động ghi nhớ vị trí đã cào. Nếu bị ngắt giữa chừng, lần sau chạy sẽ tiếp tục từ điểm dừng, không bao giờ cào lại từ đầu.

### 2. 💾 Cơ chế Lưu trữ Tối ưu 
* **Không trùng lặp :** Sử dụng thuật toán đồng bộ con trỏ (Cursor Sync), chỉ nạp dữ liệu **mới** vào Database. Database luôn sạch và nhẹ.
* **URL Optimization:** Tự động làm sạch và chuẩn hóa đường dẫn (chỉ giữ lại `shop_id`), giúp tiết kiệm dung lượng lưu trữ và dễ dàng truy vấn.
* **Dual Storage:** Dữ liệu được lưu song song:
    * **MySQL:** Dùng cho phân tích chuyên sâu, làm kho dữ liệu (Data Warehouse).
    * **Excel:** Dùng để báo cáo nhanh và kiểm tra thủ công.

### 3. 📊 Thu thập Dữ liệu Đa chiều
* **Shop Overview:** Doanh thu, sản lượng bán, chỉ số vận hành.
* **Shop Creators:** Danh sách KOL/KOC đang gắn affiliate (kèm thông tin liên hệ, chỉ số Follower, MCN).
* **Product Intelligence:** Chi tiết sản phẩm, Rating, Review, Doanh thu từng SKU.
* **Content Analytics:**
    * **Video:** View, Doanh thu, Ad Spend, ROAS, Thời lượng.
    * **Livestream:** Mắt xem trung bình (Avg View), Doanh thu phiên Live, Thời gian Live.

---

## 📋 Yêu cầu Hệ thống

* **Docker & Docker Compose** (Bắt buộc).
* **RAM:** Khuyến nghị từ 6GB trở lên (Do chạy Chrome + MySQL + Airflow cùng lúc).
* **Disk:** Trống khoảng 10GB.

---

## 🚀 Hướng dẫn Cài đặt & Triển khai

### Bước 1: Tải mã nguồn

git clone [https://github.com/QuangTran-deptrai/kalodata.git](https://github.com/QuangTran-deptrai/kalodata.git)
cd kalodata

### Bước 2: Cấu hình tham số (.env)

Tạo file `.env` tại thư mục gốc và điền thông tin tài khoản Kalodata của bạn:


# --- 1. TÀI KHOẢN KALODATA (BẮT BUỘC) ---
KALO_PHONE=0912345678
KALO_PASSWORD=MatKhauCuaBan

# --- 2. CẤU HÌNH DATABASE  ---
DB_HOST=
DB_NAME=
DB_USER=
DB_PASSWORD=

# --- 3. CẤU HÌNH AIRFLOW ---
AIRFLOW_UID=50000


### Bước 3: Khởi chạy hệ thống

Chạy lệnh sau để Docker tự động cài đặt môi trường và khởi tạo Database:


docker-compose up -d --build


*(Lưu ý: Lần đầu chạy sẽ mất vài phút để tải Image và cài đặt thư viện Python).*

-----

## ▶️ Hướng dẫn Sử dụng

### 1\. Truy cập Airflow

  * **Địa chỉ:** `http://localhost:8080`
  * **Tài khoản:** 
  * **Mật khẩu:** 

### 2\. Kích hoạt Tool

1.  Tìm DAG có tên: **`kalodata_crawler_daily`**.
2.  Bật công tắc **ON** (Góc trái) để kích hoạt lịch chạy tự động (Mặc định 00:00 hàng ngày).
3.  Nếu muốn chạy ngay lập tức: Bấm nút **Play ▶️** (Trigger DAG) bên góc phải.

### 3\. Theo dõi tiến trình

  * Click vào Task đang chạy (màu xanh lá cây nhạt) -\> Chọn **Log** để xem Tool đang làm gì theo thời gian thực.
  * *Lưu ý: Bạn sẽ KHÔNG thấy trình duyệt hiện lên vì nó chạy ngầm (Headless/Virtual Display) trong Docker.*

-----

## 📊 Truy cập Dữ liệu

### Cách 1: Kết nối MySQL (Khuyên dùng)

Dùng DBeaver, Navicat hoặc MySQL Workbench kết nối với thông số:

  * **Host:** `localhost` (hoặc IP VPS)
  * **Port:** `3307` (Docker map port 3306 -\> 3307)
  * **User:** 
  * **Pass:** 
  * **Database:** 

### Cách 2: Lấy file Excel

File Excel tổng hợp nằm tại thư mục `scripts/` của dự án:

  * Tên file: `kalodata_master.xlsx`

-----

## 📂 Cấu trúc Database

Dữ liệu được tổ chức thành các bảng quan hệ chặt chẽ:

| Tên Bảng | Mô tả dữ liệu |
| :--- | :--- |
| **`shop_metrics`** | Chỉ số tổng quan Shop (Doanh thu, Link Shop đã chuẩn hóa). |
| **`shop_creators`** | Danh sách Creator liên kết với Shop. |
| **`product_metrics`** | Chỉ số chi tiết từng sản phẩm. |
| **`product_creators`** | Creator nào bán sản phẩm nào. |
| **`videos`** | Dữ liệu Video TikTok (Ads, Revenue, Views). |
| **`lives`** | Dữ liệu Livestream (Revenue, Duration, Viewer). |
| **`product_dim`** | Bảng danh mục sản phẩm duy nhất (Dùng để map quan hệ). |

-----

## ⚠️ Reset Hệ thống (Quan trọng)

Nếu bạn muốn xóa sạch dữ liệu cũ để chạy lại từ đầu (ví dụ sau khi cập nhật code mới):


# 1. Tắt và xóa toàn bộ dữ liệu cũ (bao gồm Database)
docker-compose down -v

# 2. Chạy lại từ đầu
docker-compose up -d --build


-----

Developed by QuangTran.

