/* ==========================================================
   3) CREATE DATABASE: DATA WAREHOUSE (DW)
   ========================================================== */
DROP DATABASE IF EXISTS dw;
CREATE DATABASE dw CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE dw;

CREATE TABLE date_dim (
                          date_sk INT PRIMARY KEY,             -- 1
                          full_date DATE,                      -- 2
                          day_since_2005 INT,                  -- 3
                          month_since_2005 INT,                -- 4
                          day_of_week VARCHAR(20),             -- 5
                          calendar_month VARCHAR(20),          -- 6
                          calendar_year INT,                   -- 7
                          calendar_year_month VARCHAR(20),     -- 8
                          day_of_month INT,                    -- 9
                          day_of_year INT,                     -- 10
                          week_of_year_sunday INT,             -- 11
                          year_week_sunday VARCHAR(10),        -- 12
                          week_sunday_start DATE,              -- 13
                          week_of_year_monday INT,             -- 14
                          year_week_monday VARCHAR(10),        -- 15
                          week_monday_start DATE,              -- 16
                          quarter_year VARCHAR(10),            -- 17  (vd: 2005-Q01)
                          quarter_since_2005 INT,              -- 18
                          holiday VARCHAR(20),                 -- 19
                          day_type VARCHAR(20)                 -- 20
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
