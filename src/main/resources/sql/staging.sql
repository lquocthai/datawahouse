-- Tạo DB staging
CREATE DATABASE staging CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE staging;
-- Bảng raw_data: Lưu dữ liệu tạm được load lên từ file được crawl về
CREATE TABLE raw_data (
                          product_name   VARCHAR(500),
                          category       VARCHAR(255),
                          discount       VARCHAR(100),
                          price_original VARCHAR(100),
                          price_sale     VARCHAR(100),
                          product_url    VARCHAR(1000),
                          crawl_date     VARCHAR(100),
                          load_time      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- Bảng date_dim: lưu thông tin thời gian để phân tích (dim table)
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

-- Bảng transformed_data: Dùng để lưu dữ liệu đã được transform từ bảng raw_data
CREATE TABLE transformed_data (
                                  id INT AUTO_INCREMENT PRIMARY KEY,
                                  product_name   VARCHAR(500),
                                  category       VARCHAR(255),
                                  discount       DECIMAL(5,2),      -- tỉ lệ giảm giá, ví dụ 11% → 11.00
                                  price_original DECIMAL(15,2),     -- giá gốc, ví dụ 37.990.000 → 37990000.00
                                  price_sale     DECIMAL(15,2),     -- giá sau giảm
                                  product_url    VARCHAR(1000),
                                  crawl_date     DATE,               -- chỉ lấy phần ngày từ crawl_date
                                  load_time      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
