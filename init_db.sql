
-- init_db.sql — Chạy tự động khi PostgreSQL khởi động lần đầu

-- 1. Database cho Superset (lưu config, dashboard, user nội bộ của Superset)
CREATE DATABASE superset_meta;

-- 2. Thiết lập Data Warehouse chính
\c datalink_warehouse;

-- 2.1 Các schema cho luồng xử lý dữ liệu (Medallion Architecture)
CREATE SCHEMA IF NOT EXISTS raw_data;       -- Lớp Raw (dlt ghi vào)
CREATE SCHEMA IF NOT EXISTS staging_data;        -- Lớp Staging (dbt)
CREATE SCHEMA IF NOT EXISTS mart_data;          -- Lớp Marts (dbt) — Superset đọc từ đây

-- 2.2 Schema cho ứng dụng Quản trị Web Control Plane
CREATE SCHEMA IF NOT EXISTS control_plane;

-- Bảng tài khoản người dùng đăng nhập hệ thống quản trị
CREATE TABLE IF NOT EXISTS control_plane.app_users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    full_name VARCHAR(100),
    role VARCHAR(20) DEFAULT 'operator', -- 'admin', 'operator', 'viewer'
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Bảng theo dõi lịch sử chạy pipeline và log
CREATE TABLE IF NOT EXISTS control_plane.pipeline_runs (
    id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    triggered_by VARCHAR(50) NOT NULL,   -- 'manual', 'scheduler', hoặc username
    status VARCHAR(20) NOT NULL,         -- 'RUNNING', 'SUCCESS', 'FAILED'
    started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP WITH TIME ZONE,
    execution_time_seconds NUMERIC(10, 2),
    log_output TEXT
);

-- Bảng lưu lịch cấu hình chạy tự động (Scheduler)
CREATE TABLE IF NOT EXISTS control_plane.schedules (
    id SERIAL PRIMARY KEY,
    job_name VARCHAR(100) UNIQUE NOT NULL,
    cron_expression VARCHAR(50) NOT NULL, -- vd: '0 2 * * *' (02:00 sáng mỗi ngày)
    is_active BOOLEAN DEFAULT true,
    last_run_at TIMESTAMP WITH TIME ZONE,
    next_run_at TIMESTAMP WITH TIME ZONE
);

-- Tạo sẵn tài khoản Admin mặc định cho Control Plane (mật khẩu: admin123)
-- Hash bcrypt tương ứng: $2b$12$e8qR... (sẽ được sinh bằng script Python)
INSERT INTO control_plane.app_users (username, password_hash, full_name, role)
VALUES ('admin', '$2b$12$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy', 'Hệ Thống Admin', 'admin')
ON CONFLICT (username) DO NOTHING;

-- Phân quyền truy cập
GRANT USAGE, CREATE ON SCHEMA raw_data TO datalink;
GRANT USAGE, CREATE ON SCHEMA staging TO datalink;
GRANT USAGE, CREATE ON SCHEMA intermediate TO datalink;
GRANT USAGE, CREATE ON SCHEMA marts TO datalink;
GRANT USAGE, CREATE ON SCHEMA control_plane TO datalink;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA control_plane TO datalink;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA control_plane TO datalink;



