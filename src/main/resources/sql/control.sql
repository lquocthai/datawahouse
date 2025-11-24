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
insert into control.config(name, url, location_store)
values('Laptop','https://cellphones.com.vn/laptop.html','C:/Folder Chung/Study/All Subject/DataWarehouse/Project/data_csv_daily');
insert into control.config(name, url, location_store)
values('Load Raw Data','Load to raw_data','staging');
insert into control.config(name, url, location_store)
values('Transform Data','Tranform Data From table raw_data','transformed_data');
insert into control.config(name, url, location_store)
values('Load To DW','Load to dw','fact_product');
insert into control.config(name, url, location_store)
values('Load To Datamart','Load to dw','dm_product_analysis');
