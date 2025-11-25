CREATE DATABASE IF NOT EXISTS datamart
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE datamart;

-- ============================
-- 1. Bảng price_daily
-- ============================
CREATE TABLE price_daily (
                             full_date DATE PRIMARY KEY,
                             total_laptops INT,
                             avg_price_original DECIMAL(15,2),
                             avg_price_sale DECIMAL(15,2),
                             avg_discount DECIMAL(10,2)
);

-- ============================
-- 2. Bảng top_discount_daily
-- ============================
CREATE TABLE top_discount_daily (
                                    full_date DATE,
                                    product_key BIGINT,
                                    product_name VARCHAR(255),
                                    price_sale DECIMAL(15,2),
                                    discount DECIMAL(10,2),
                                    rank_in_day INT,
                                    PRIMARY KEY(full_date, rank_in_day)
);

-- ============================
-- 3. Bảng top 5 cao nhất
-- ============================
CREATE TABLE top5_highest_price (
                                    id INT AUTO_INCREMENT PRIMARY KEY,
                                    full_date DATE NOT NULL,
                                    product_key INT NOT NULL,
                                    product_name VARCHAR(255) NOT NULL,
                                    category VARCHAR(100),
                                    price_sale DECIMAL(15,2) NOT NULL,
                                    rank_order INT NOT NULL,
                                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================
-- 4. Bảng top 5 thấp nhất
-- ============================
CREATE TABLE top5_lowest_price (
                                   id INT AUTO_INCREMENT PRIMARY KEY,
                                   full_date DATE NOT NULL,
                                   product_key INT NOT NULL,
                                   product_name VARCHAR(255) NOT NULL,
                                   category VARCHAR(100),
                                   price_sale DECIMAL(15,2) NOT NULL,
                                   rank_order INT NOT NULL,
                                   created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
