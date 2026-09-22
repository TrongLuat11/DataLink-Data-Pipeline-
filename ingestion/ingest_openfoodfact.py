"""
DataLink — Pipeline dlt Ingest Open Food Facts (Multi-table)
Nạp dữ liệu sản phẩm FMCG từ Open Food Facts API vào PostgreSQL.
"""

# 1. KHAI BÁO THƯ VIỆN
import time                                 # Thêm thư viện time để tạo khoảng nghỉ
import dlt                                  # Thư viện chính để xây Data Pipeline tự động
from dlt.sources.helpers import requests    # Module của dlt dùng để gọi API (có tích hợp tự thử lại nếu mạng lỗi)

# 2. CẤU HÌNH KHI GỌI API
# Bắt buộc phải có User-Agent để Server Open Food Facts biết ai đang xin dữ liệu, tránh bị chặn (Block IP).
HEADERS = {"User-Agent": "DataLink-StudentProject/1.0 (university project; contact@student.edu.vn)"}
BASE_URL = "https://world.openfoodfacts.org"

# =====================================================================
# 3. CHIA GIAI ĐOẠN LẤY DỮ LIỆU (TEST vs THẬT)
# =====================================================================
# Thay vì sửa số liên tục, ta tạo một công tắc IS_TEST_MODE.
# Khi code và kiểm tra lỗi -> Để True (chạy mất 10 giây).
# Khi cần lấy số liệu làm báo cáo -> Đổi thành False (chạy mất 10 phút).
IS_TEST_MODE = True  

if IS_TEST_MODE:
    TOP_N_CATEGORIES = 10       # Chỉ tải 10 danh mục
    PRODUCTS_PER_CATEGORY = 50  # Mỗi danh mục lấy đúng 50 sản phẩm
    print("⚠️ ĐANG CHẠY CHẾ ĐỘ TEST (Lấy lượng nhỏ dữ liệu để thử code)...")
else:
    TOP_N_CATEGORIES = 100      # Tải 100 danh mục lớn nhất (chiếm phần lớn thị trường)
    PRODUCTS_PER_CATEGORY = 500 # Lấy 500 sản phẩm / danh mục
    print("🚀 ĐANG CHẠY CHẾ ĐỘ THẬT (Tải lượng lớn dữ liệu để làm báo cáo)...")


def fetch_all_categories(top_n=TOP_N_CATEGORIES):
    """Hàm tự động gọi API để lấy ra top_n danh mục có nhiều sản phẩm nhất"""
    url = f"{BASE_URL}/categories.json"
    try:
        response = requests.get(url, headers=HEADERS) # Gọi API
        time.sleep(1)                                 # Nghỉ 1 giây
        data = response.json()                        # Ép kiểu dữ liệu về JSON (như Dictionary Python)
        all_tags = data.get("tags", [])
  
        # Hàm sorted(): Sắp xếp danh sách danh mục theo tiêu chí 'products' (số lượng hàng hoá) giảm dần
        sorted_tags = sorted(all_tags, key=lambda x: x.get("products", 0), reverse=True)
  
        categories = []
        for tag in sorted_tags[:top_n]:
            # Lệnh replace("en:", "") xoá tiền tố không cần thiết từ API trả về
            # Ví dụ API trả về: {"id": "en:snacks", "name": "Snacks", "products": 100000}
            # Lệnh này sẽ biến "en:snacks" thành "snacks"
            clean_id = tag.get("id", "").replace("en:", "").strip()
            if clean_id:
                categories.append(clean_id)
        return categories
    except Exception as e:
        # Nếu mất mạng hoặc API sập, ta hardcode danh sách cứu viện để không chết chương trình
        return ["snacks", "beverages", "dairies", "meats"]


# CHỌN LỌC CỘT DỮ LIỆU (Giảm băng thông)
FIELDS = ",".join([
    "code", "product_name", "brands", "categories_tags", "quantity",
    "nutriscore_grade", "nova_group", "ecoscore_grade", "nutriments",
    "ingredients_text", "allergens", "countries_tags", "image_url", "last_modified_t"
])

# =====================================================================
# 4. ĐỊNH NGHĨA DLT SOURCE & RESOURCES (PHẦN CỐT LÕI)
# =====================================================================

# @dlt.source: Là một "decorator" của Python. Nó đánh dấu hàm bên dưới 
# đóng vai trò là "Nguồn cấp dữ liệu" (Source). Một Source sẽ chứa nhiều bảng con (Resource).
@dlt.source(name="openfoodfacts")
def openfoodfacts_source(categories: list = None):
  
    if categories is None:
        categories = fetch_all_categories()

    # --- BẢNG 1: PRODUCTS ---
    # @dlt.resource: Đánh dấu hàm bên dưới sẽ sinh ra dữ liệu cho 1 BẢNG.
    # name="products": Bảng trong database sẽ tên là 'products'.
    # write_disposition="merge": Cơ chế UPSERT. Nếu mã code trùng -> Cập nhật; Chưa có -> Thêm mới.
    # primary_key="code": Khai báo cột 'code' làm khoá chính để nhận diện dòng trùng lặp.
    @dlt.resource(
        name="products",
        write_disposition="merge",
        primary_key="code",
    )
    def products_resource(
        # dlt.sources.incremental: Công cụ theo dõi thay đổi.
        # Nó sẽ nhớ mốc thời gian "last_modified_t" của lần chạy trước.
        # Ở lần chạy sau, nó chỉ lấy các sản phẩm có "last_modified_t" mới hơn -> Tiết kiệm cực lớn thời gian.
        last_modified=dlt.sources.incremental("last_modified_t", initial_value=0)
    ):
        for category in categories:
            # Phân trang: Chia số lượng sản phẩm cần lấy thành từng trang (mỗi trang 50 SP)
            # Dùng hàm range(1, (tổng số SP // 50) + 2) để tính ra số trang cần lấy.
            pages = (PRODUCTS_PER_CATEGORY // 50) + 1
            for page in range(1, pages):
                url = f"{BASE_URL}/api/v2/search"
                params = {"categories_tags": category, "fields": FIELDS, "page": page, "page_size": 50}
          
                response = requests.get(url, headers=HEADERS, params=params)
                time.sleep(1) # Nghỉ 1 giây tránh nghẽn API
                if response.status_code != 200:
                    break
          
                items = response.json().get("products", [])
                if not items:
                    break
          
                for item in items:
                    code = item.get("code")
                    if not code: continue

                    # yield: Câu lệnh cực kỳ quan trọng trong Python Generator.
                    # Thay vì hàm 'return' trả về 1 danh sách khổng lồ chứa 10,000 dòng rồi kết thúc, 
                    # 'yield' sẽ "đẩy ra" TỪNG DÒNG MỘT. 
                    # Dlt sẽ hứng từng dòng này, cứ đủ 1 cục (chunk) là đẩy ngay vào DB. Máy không bao giờ bị tràn RAM.
                    yield {
                        "code": code,
                        "product_name": item.get("product_name"),
                        "brands": item.get("brands"),
                        "nutriscore_grade": item.get("nutriscore_grade"),
                        "last_modified_t": item.get("last_modified_t"),
                    }

    # --- BẢNG 2: PRODUCT_NUTRIMENTS (Dinh dưỡng) ---
    @dlt.resource(name="product_nutriments", write_disposition="merge", primary_key="code")
    def nutriments_resource(last_modified=dlt.sources.incremental("last_modified_t", initial_value=0)):
        for category in categories:
            for page in range(1, (PRODUCTS_PER_CATEGORY // 50) + 1):
                url = f"{BASE_URL}/api/v2/search"
                response = requests.get(url, headers=HEADERS, params={"categories_tags": category, "fields": FIELDS, "page": page, "page_size": 50})
                time.sleep(1)
                if response.status_code != 200: break
                items = response.json().get("products", [])
                if not items: break

                for item in items:
                    code = item.get("code")
                    if not code: continue

                    n = item.get("nutriments", {})
                    yield {
                        "code": code,
                        "sugars_100g": n.get("sugars_100g"),
                        "salt_100g": n.get("salt_100g"),
                        "fat_100g": n.get("fat_100g"),
                        "last_modified_t": item.get("last_modified_t"),
                    }

    # --- BẢNG 3: PRODUCT_CATEGORIES (Danh mục 1-N) ---
    # write_disposition="replace": Ở đây ta không dùng 'merge' mà dùng 'replace'.
    # Tức là dlt sẽ XOÁ SẠCH bảng product_categories cũ và INSERT lại từ đầu.
    # Dùng cho các bảng phụ chứa mối quan hệ 1-N (1 sản phẩm nằm trong nhiều danh mục).
    @dlt.resource(name="product_categories", write_disposition="replace")
    def categories_resource():
        for category in categories:
            for page in range(1, (PRODUCTS_PER_CATEGORY // 50) + 1):
                url = f"{BASE_URL}/api/v2/search"
                response = requests.get(url, headers=HEADERS, params={"categories_tags": category, "fields": FIELDS, "page": page, "page_size": 50})
                time.sleep(1)
                if response.status_code != 200: break
                items = response.json().get("products", [])
          
                for item in items:
                    code = item.get("code")
                    if not code: continue

                    # Một sản phẩm có mảng danh mục: ["en:snacks", "en:sweet-snacks"]
                    tags = item.get("categories_tags", [])
                    if isinstance(tags, list):
                        for tag in tags:
                            yield {
                                "code": code,
                                "category_tag": tag.replace("en:", "").strip()
                            }

    # Hàm source sẽ trả về danh sách các Resource (các bảng) để DLT quản lý
    return products_resource, nutriments_resource, categories_resource


# =====================================================================
# 5. HÀM MAIN: KHỞI TẠO VÀ THỰC THI PIPELINE
# =====================================================================
def main():
    # dlt.pipeline: Tạo bộ máy tải dữ liệu.
    # pipeline_name: Tên của tiến trình.
    # destination="postgres": Khai báo kho đến là hệ quản trị CSDL PostgreSQL.
    # dataset_name="raw_data": Database schema chứa các bảng sẽ tên là 'raw_data'.
    pipeline = dlt.pipeline(
        pipeline_name="openfoodfacts_pipeline",
        destination="postgres",
        dataset_name="raw_data",
    )

    # Lệnh pipeline.run() kích hoạt toàn bộ quy trình:
    # Nó gọi source() -> Source tự động gọi fetch_all_categories() -> Đánh thức các resources -> Kéo API -> Lưu DB.
    load_info = pipeline.run(openfoodfacts_source())

    # In ra báo cáo tóm tắt tiến trình sau khi tải xong
    print(load_info)

if __name__ == "__main__":
    main()