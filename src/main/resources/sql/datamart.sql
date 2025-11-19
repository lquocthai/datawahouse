-- Tạo Database datamart
CREATE DATABASE IF NOT EXISTS datamart
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

-- Tạo bảng dm_product_analysis
USE datamart;
CREATE TABLE IF NOT EXISTS dm_product_analysis (
                                                   id BIGINT AUTO_INCREMENT PRIMARY KEY,
                                                   product_name VARCHAR(500),
    category VARCHAR(255),
    discount DECIMAL(5,2),
    price_original DECIMAL(15,2),
    price_sale DECIMAL(15,2),
    product_url VARCHAR(1000),

    full_date DATE,
    year INT,
    month INT,
    day INT,
    quarter INT,

    load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

