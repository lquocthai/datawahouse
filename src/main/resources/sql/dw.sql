-- Tạo DB dw
CREATE DATABASE dw CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE dw;
-- Bảng DATE_DIM: lưu thông tin thời gian để phân tích (dim table)
CREATE TABLE date_dim (
                          date_key INT PRIMARY KEY,
                          full_date DATE NOT NULL,
                          day INT,
                          month INT,
                          quarter INT,
                          year INT,
                          day_name VARCHAR(20),
                          month_name VARCHAR(20),
                          is_weekend BOOLEAN
);