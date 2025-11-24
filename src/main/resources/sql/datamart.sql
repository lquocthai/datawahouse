-- -- Tạo Database datamart
-- CREATE DATABASE IF NOT EXISTS datamart
--     CHARACTER SET utf8mb4
--     COLLATE utf8mb4_unicode_ci;
--
-- -- Tạo bảng dm_product_analysis
-- USE datamart;
-- CREATE TABLE IF NOT EXISTS dm_product_analysis (
--                                                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
--                                                    product_name VARCHAR(500),
--     category VARCHAR(255),
--     discount DECIMAL(5,2),
--     price_original DECIMAL(15,2),
--     price_sale DECIMAL(15,2),
--     product_url VARCHAR(1000),
--
--     full_date DATE,
--     year INT,
--     month INT,
--     day INT,
--     quarter INT,
--
--     load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
--     );
--
CREATE DATABASE IF NOT EXISTS datamart
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;


USE datamart;
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

CREATE TABLE price_daily (
                             date_key INT,
                             total_laptops INT,
                             avg_price_original DECIMAL(15,2),
                             avg_price_sale DECIMAL(15,2),
                             avg_discount DECIMAL(10,2),
                             PRIMARY KEY(date_key)
);

CREATE TABLE top_discount_daily (
                                    date_key INT,
                                    product_key BIGINT,
                                    price_sale DECIMAL(15,2),
                                    discount DECIMAL(10,2),
                                    rank_in_day INT,
                                    PRIMARY KEY(date_key, rank_in_day)
);
