
##  Tính năng nổi bật

### 1. Cơ chế vận hành thông minh
* Đăng nhập tự động: Tự động đăng nhập và duy trì phiên làm việc an toàn.
* Công nghệ Anti-Detection: Sử dụng lõi trình duyệt tùy biến để vượt qua các cơ chế chặn bot của nền tảng.
* Cơ chế Resume : Hệ thống tự động lưu checkpoint sau mỗi tác vụ. Nếu quá trình chạy bị gián đoạn (mất mạng, tắt máy...), lần chạy sau sẽ tự động tiếp tục từ điểm dừng, tránh việc cào lại dữ liệu cũ.

### 2. Thu thập dữ liệu đa chiều
Công cụ đi sâu vào từng ngóc ngách dữ liệu:
* **Shop Overview:** Doanh thu tổng, số lượng bán, số lượng Affiliate đang hoạt động.
* **Shop Creators Database:** Quét toàn bộ danh sách Creator liên kết với Shop, bao gồm thông tin quan trọng như **Account Type** (Seller/Affiliate), MCN, Bio, và Follower.
* **Product Intelligence:** Chỉ số chi tiết từng sản phẩm (Doanh thu, Rating, Reviews, Số lượng SKU, Link gốc TikTok).
* **Content Analytics (Video & Live):**
    * **Video:** View, Doanh thu, Thời lượng, Ngày đăng, và các chỉ số Quảng cáo (Ad Spending, ROAS).
    * **Livestream:** Số người xem trung bình (Avg Viewer), Doanh thu phiên Live, Sản phẩm bán ra.



##  Yêu cầu hệ thống

Để vận hành công cụ, máy tính cần cài đặt:
* **Hệ điều hành:** Windows 10/11 hoặc macOS.
* **Python:** Phiên bản 3.8 trở lên.
* **Google Chrome:** Phiên bản mới nhất.
* **Git:** Để quản lý mã nguồn.



##  Hướng dẫn cài đặt & Cấu hình

### Bước 1: Cài đặt môi trường
Mở Terminal (hoặc PowerShell) và thực hiện các lệnh sau:

# 1. Clone dự án về máy
git clone [https://github.com/QuangTran-deptrai/kalodata.git](https://github.com/QuangTran-deptrai/kalodata.git)
cd kalodata

# 2. Tạo môi trường ảo (Khuyên dùng để tránh xung đột thư viện)
python -m venv venv

# 3. Kích hoạt môi trường ảo
# -> Trên Windows:
.\venv\Scripts\activate
# -> Trên macOS/Linux:
source venv/bin/activate

# 4. Cài đặt các thư viện cần thiết
pip install -r requirements.txt


1. Cấu hình Tài khoản (.env)
Tạo một file mới tên là .env (không có đuôi file) tại thư mục gốc dự án. Điền thông tin đăng nhập Kalodata của bạn vào file này:
# File: .env
KALO_PHONE=0912345678
KALO_PASSWORD=MatKhauCuaBanO_Day


2. Cấu hình Chạy (config.py)
Mở file config.py có sẵn trong dự án để chỉnh sửa các thông số lọc dữ liệu theo nhu cầu:

# File: config.py

# Khoảng thời gian muốn lấy dữ liệu (Định dạng YYYY-MM-DD)
FILTER_DATE_START = "2025-11-10"
FILTER_DATE_END = "2025-11-11"

# Số lượng Shop tối đa muốn quét trong một lần chạy
MAX_SHOPS = 5

Quy trình hoạt động của Tool:

Mở trình duyệt Chrome -> Đăng nhập tự động.

Vào danh sách Shop -> Lọc theo ngày tháng cấu hình.

Vào từng Shop -> Lấy thông tin Shop & Danh sách Creator của Shop.

Vào danh sách Sản phẩm -> Lấy chi tiết Sản phẩm (Rating, Review...).

Vào từng Sản phẩm -> Lấy danh sách Video & Livestream liên quan.

Lưu dữ liệu liên tục vào file Excel.


Tên Sheet,Nội dung chi tiết
Shop_Metrics,"Tổng quan chỉ số kinh doanh của Shop (Revenue, Sold, Affiliates count...)."
creator_dim_shop,Danh sách Creator cấp Shop. Đặc biệt: Có cột Account Type phân loại Affiliate/Seller.
Product_Metrics,"Chi tiết sản phẩm: Doanh thu, Giá, Rating (Đánh giá), Reviews (Số lượng review), Product SKUs."
product_Dim,"Danh mục sản phẩm rút gọn (Link Shop, Tên Sản phẩm, Link TikTok)."
Creators,Danh sách Creator quảng bá cho từng sản phẩm cụ thể.
Videos,"Dữ liệu Video chi tiết: Views, Revenue, Duration, Ad Metrics (ROAS, Spend)."
Lives,"Dữ liệu Livestream: Thời lượng, Doanh thu, Avg Online Viewer, Item Sold."




Lưu ý khi vận hành
Không thao tác chuột: Khi tool đang chạy và mở trình duyệt, vui lòng không tự ý click chuột hoặc tắt cửa sổ trình duyệt, điều này có thể làm gián đoạn quy trình tự động.

Tốc độ mạng: Đảm bảo kết nối mạng ổn định để tool tải trang đầy đủ dữ liệu.

Làm mới dữ liệu: Nếu muốn chạy lại từ đầu (xóa hết dữ liệu cũ), hãy xóa file kalodata_master.xlsx hoặc đổi tên file đầu ra trong config.py.



