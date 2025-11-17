-- Tạo DB control
CREATE DATABASE control CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE control;
-- Bảng CONFIG: Cấu hình các process để hỗ trợ crawl và viết log
CREATE TABLE config (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        name VARCHAR(100) NOT NULL,
                        url VARCHAR(500) NOT NULL,
                        location_store VARCHAR(200) DEFAULT NULL,
                        date_run DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Bảng LOG: Viết log các tiến trình đã thực hiện
CREATE TABLE log (
                     id INT AUTO_INCREMENT PRIMARY KEY,
                     id_config INT NOT NULL,
                     date_run DATETIME DEFAULT CURRENT_TIMESTAMP,
                     status VARCHAR(50) DEFAULT &#39;PENDING&#39;,
FOREIGN KEY (id_config) REFERENCES config(id)
);