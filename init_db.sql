-- init_db.sql — Chạy tự động khi PostgreSQL khởi động lần đầu

-- Database cho Metabase (lưu config riêng của Metabase)
CREATE DATABASE metabase_appdb;

-- Tạo các schema trong data warehouse
\c datalink_warehouse;

CREATE SCHEMA IF NOT EXISTS raw_data;       -- Lớp Raw (dlt ghi vào)
CREATE SCHEMA IF NOT EXISTS staging;        -- Lớp Staging (dbt)
CREATE SCHEMA IF NOT EXISTS intermediate;   -- Lớp Intermediate (dbt)
CREATE SCHEMA IF NOT EXISTS marts;          -- Lớp Marts (dbt) — dashboard đọc từ đây

-- Phân quyền
GRANT USAGE ON SCHEMA raw_data TO datalink;
GRANT USAGE ON SCHEMA staging TO datalink;
GRANT USAGE ON SCHEMA intermediate TO datalink;
GRANT USAGE ON SCHEMA marts TO datalink;
