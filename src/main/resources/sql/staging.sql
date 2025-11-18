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

-- Bảng TRANSFORMED_DATA (TransformData.java)
CREATE TABLE transformed_data (
                                  product_name   VARCHAR(500),
                                  category       VARCHAR(255),
                                  discount       DECIMAL(5,2),
                                  price_original DECIMAL(15,2),
                                  price_sale     DECIMAL(15,2),
                                  product_url    VARCHAR(1000),
                                  crawl_date     DATE,
                                  load_time      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bảng DATE_DIM
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
