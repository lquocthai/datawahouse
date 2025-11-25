/* ==========================================================
   3) CREATE DATABASE: DATA WAREHOUSE (DW)
   ========================================================== */
DROP DATABASE IF EXISTS dw;
CREATE DATABASE dw CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;


use dw;
CREATE TABLE date_dim (
                          id INT AUTO_INCREMENT PRIMARY KEY,       -- cột id tăng tự động, chỉ dùng trong table
                          date_key INT NOT NULL UNIQUE,            -- YYYYMMDD, dùng join fact table
                          full_date DATE NOT NULL,
                          day INT,
                          month INT,
                          quarter INT,
                          year INT,
                          day_name VARCHAR(20),
                          month_name VARCHAR(20),
                          is_weekend BOOLEAN,
                          week_of_month INT,                        -- tuần trong tháng
                          week_of_year INT                          -- tuần trong năm
);
CREATE TABLE product_dim (
                             product_key BIGINT AUTO_INCREMENT PRIMARY KEY,
                             product_name VARCHAR(500),
                             category VARCHAR(255),
                             product_url VARCHAR(1000),
                             UNIQUE KEY uq_product_url (product_url(255))
);


CREATE TABLE fact_product (
                              fact_id BIGINT AUTO_INCREMENT PRIMARY KEY,

                              product_key BIGINT NOT NULL,
                              date_key INT NOT NULL,

                              load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                              discount DECIMAL(5,2),
                              price_original DECIMAL(15,2),
                              price_sale DECIMAL(15,2),

                              FOREIGN KEY (product_key) REFERENCES product_dim(product_key),
                              FOREIGN KEY (date_key) REFERENCES date_dim(date_key),

                              UNIQUE KEY uq_product_date (product_key, date_key)   -- chống trùng
);
