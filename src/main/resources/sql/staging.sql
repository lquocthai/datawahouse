/* ==========================================================
   2) CREATE DATABASE: STAGING
   ========================================================== */
DROP DATABASE IF EXISTS staging;
CREATE DATABASE staging CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE staging;

-- Bảng RAW_DATA (LoadRawData.java)
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


-- Bảng DATE_DIM
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
