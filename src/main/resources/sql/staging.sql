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
