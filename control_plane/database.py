"""
DataLink Control Plane — Module Database & Xác thực
"""
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import bcrypt
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv("POSTGRES_USER", "datalink")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "datalink_secret_2024")
# Khi chạy trực tiếp trên máy Mac (host), kết nối qua localhost:5433
# Khi chạy trong container Docker, kết nối qua postgres:5432
raw_host = os.getenv("POSTGRES_HOST", "localhost")
DB_HOST = "localhost" if raw_host in ["postgres", "datalink_postgres"] else raw_host
raw_port = os.getenv("POSTGRES_PORT", "5433")
DB_PORT = "5433" if raw_port == "5432" else raw_port
DB_NAME = os.getenv("POSTGRES_DB", "datalink_warehouse")

def get_db_connection():
    """Tạo kết nối tới PostgreSQL"""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        dbname=DB_NAME,
        cursor_factory=RealDictCursor
    )

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """So khớp mật khẩu nhập vào với mật khẩu băm trong database"""
    if not plain_password or not hashed_password:
        return False
    try:
        hashed_bytes = hashed_password.encode("utf-8") if isinstance(hashed_password, str) else bytes(hashed_password)
        plain_bytes = plain_password.encode("utf-8")[:72]
        return bcrypt.checkpw(plain_bytes, hashed_bytes)
    except Exception:
        return False

def hash_password(password: str) -> str:
    """Băm mật khẩu trước khi lưu vào database"""
    plain_bytes = password.encode("utf-8")[:72]
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(plain_bytes, salt).decode("utf-8")

def authenticate_user(username: str, password: str):
    """Kiểm tra tài khoản đăng nhập"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, username, password_hash, full_name, role FROM control_plane.app_users WHERE username = %s",
                (username,)
            )
            user = cur.fetchone()
            if not user:
                return None
            if not verify_password(password, user["password_hash"]):
                return None
            return user
    finally:
        conn.close()

def log_pipeline_start(pipeline_name: str, triggered_by: str) -> int:
    """Ghi nhận pipeline bắt đầu chạy"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO control_plane.pipeline_runs 
                (pipeline_name, triggered_by, status, started_at)
                VALUES (%s, %s, 'RUNNING', CURRENT_TIMESTAMP)
                RETURNING id;
                """,
                (pipeline_name, triggered_by)
            )
            run_id = cur.fetchone()["id"]
            conn.commit()
            return run_id
    finally:
        conn.close()

def log_pipeline_finish(run_id: int, status: str, log_output: str, duration_seconds: float):
    """Ghi nhận pipeline hoàn thành hoặc thất bại"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE control_plane.pipeline_runs
                SET status = %s, finished_at = CURRENT_TIMESTAMP, 
                    log_output = %s, execution_time_seconds = %s
                WHERE id = %s;
                """,
                (status, log_output, duration_seconds, run_id)
            )
            conn.commit()
    finally:
        conn.close()

def get_recent_runs(limit: int = 200):
    """Lấy danh sách các lần chạy gần nhất (hỗ trợ hiển thị toàn bộ lịch sử)"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            if limit:
                cur.execute(
                    """
                    SELECT id, pipeline_name, triggered_by, status, 
                           started_at, execution_time_seconds, log_output
                    FROM control_plane.pipeline_runs
                    ORDER BY id DESC LIMIT %s;
                    """,
                    (limit,)
                )
            else:
                cur.execute(
                    """
                    SELECT id, pipeline_name, triggered_by, status, 
                           started_at, execution_time_seconds, log_output
                    FROM control_plane.pipeline_runs
                    ORDER BY id DESC;
                    """
                )
            return cur.fetchall()
    finally:
        conn.close()

def get_business_kpis():
    """Lấy số liệu kinh doanh tổng hợp từ mart_data phục vụ báo cáo BI"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Sales KPI
            cur.execute("""
                SELECT 
                    COALESCE(SUM(estimated_revenue), 0) as total_revenue,
                    COALESCE(SUM(quantity_sold), 0) as total_sold,
                    COUNT(DISTINCT product_id) as total_products,
                    COUNT(DISTINCT brand_name) as total_brands,
                    COUNT(DISTINCT category_name) as total_categories
                FROM mart_data.fact_sales;
            """)
            sales = cur.fetchone() or {}

            # Flash Sales KPI
            cur.execute("""
                SELECT 
                    COUNT(DISTINCT deal_id) as total_deals,
                    COALESCE(SUM(deal_estimated_revenue), 0) as flash_revenue,
                    ROUND(AVG(sell_out_rate_percent), 1) as avg_sell_out
                FROM mart_data.fact_flash_sales;
            """)
            flash = cur.fetchone() or {}

            # Reviews KPI
            cur.execute("""
                SELECT 
                    COUNT(*) as total_reviews,
                    ROUND(AVG(rating), 2) as avg_rating,
                    ROUND(100.0 * SUM(CASE WHEN rating >= 4 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) as positive_pct
                FROM mart_data.fact_reviews;
            """)
            reviews = cur.fetchone() or {}

            # Sellers KPI
            cur.execute("""
                SELECT COUNT(*) as total_sellers FROM mart_data.dim_sellers;
            """)
            sellers = cur.fetchone() or {}

            return {
                "total_revenue": float(sales.get("total_revenue") or 0),
                "total_sold": int(sales.get("total_sold") or 0),
                "total_products": int(sales.get("total_products") or 0),
                "total_brands": int(sales.get("total_brands") or 0),
                "total_categories": int(sales.get("total_categories") or 0),
                "total_deals": int(flash.get("total_deals") or 0),
                "flash_revenue": float(flash.get("flash_revenue") or 0),
                "avg_sell_out": float(flash.get("avg_sell_out") or 0),
                "total_reviews": int(reviews.get("total_reviews") or 0),
                "avg_rating": float(reviews.get("avg_rating") or 0),
                "positive_pct": float(reviews.get("positive_pct") or 0),
                "total_sellers": int(sellers.get("total_sellers") or 0)
            }
    except Exception as e:
        return {
            "total_revenue": 0, "total_sold": 0, "total_products": 0, "total_brands": 0,
            "total_categories": 0, "total_deals": 0, "flash_revenue": 0, "avg_sell_out": 0,
            "total_reviews": 0, "avg_rating": 0, "positive_pct": 0, "total_sellers": 0,
            "error": str(e)
        }
    finally:
        conn.close()