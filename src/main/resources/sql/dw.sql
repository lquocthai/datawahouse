/* ==========================================================
   3) CREATE DATABASE: DATA WAREHOUSE (DW)
   ========================================================== */
DROP DATABASE IF EXISTS dw;
CREATE DATABASE dw CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE dw;

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
-- Bảng FACT_PRODUCT (dữ liệu cuối cùng)
CREATE TABLE fact_product (
                              product_key BIGINT AUTO_INCREMENT PRIMARY KEY,
                              product_name VARCHAR(500),
                              category VARCHAR(255),
                              discount DECIMAL(5,2),
                              price_original DECIMAL(15,2),
                              price_sale DECIMAL(15,2),
                              product_url VARCHAR(1000),
                              date_key INT,
                              load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                              FOREIGN KEY (date_key) REFERENCES date_dim(date_key)
);

/* ==========================================================
   4) USER & QUYỀN TRUY CẬP
   ========================================================== */
CREATE USER 'user_staging'@'%' IDENTIFIED BY '123456';

GRANT ALL PRIVILEGES ON *.* TO 'user_staging'@'%' IDENTIFIED BY '123456';
FLUSH PRIVILEGES;

GRANT ALL PRIVILEGES ON control.* TO 'user_staging'@'%';
GRANT ALL PRIVILEGES ON staging.* TO 'user_staging'@'%';
GRANT ALL PRIVILEGES ON dw.* TO 'user_staging'@'%';

FLUSH PRIVILEGES;

-- Kiểm tra bảng fact
SELECT COUNT(*) FROM fact_product;
