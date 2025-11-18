/* ==========================================================
   1) CREATE DATABASE: CONTROL
   ========================================================== */
DROP DATABASE IF EXISTS control;
CREATE DATABASE control CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE control;

-- Bảng CONFIG
CREATE TABLE config (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        name VARCHAR(100) NOT NULL,
                        url VARCHAR(500) NOT NULL,
                        location_store VARCHAR(200) DEFAULT NULL
                        location_store VARCHAR(200) DEFAULT NULL,
);

-- Bảng LOG
CREATE TABLE log (
                     id INT AUTO_INCREMENT PRIMARY KEY,
                     id_config INT NOT NULL,
                     date_run DATETIME DEFAULT CURRENT_TIMESTAMP,
                     status VARCHAR(50) DEFAULT 'PENDING',
                     FOREIGN KEY (id_config) REFERENCES config(id)
);

-- INSERT CONFIG SAMPLE
INSERT INTO config(name, url, location_store)
VALUES(
          'Laptop',
          'https://cellphones.com.vn/laptop.html',
          'C:/Folder data_crawl'
      );

INSERT INTO config(name, url, location_store)
VALUES(
          'Transform Data',
          'Transform Raw → Transformed',
          'none'
      );

INSERT INTO config(name, url, location_store)
VALUES(
          'Load To DW',
          'Load staging → dw',
          'none'
      );

-- Xóa config thừa trong trường hợp chạy lại script
DELETE FROM config WHERE name = 'Transform Data';
DELETE FROM config WHERE name = 'Load To DW';

