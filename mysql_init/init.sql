
CREATE DATABASE IF NOT EXISTS kalodata_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE kalodata_db;


CREATE TABLE IF NOT EXISTS shop_metrics (
    id INT AUTO_INCREMENT PRIMARY KEY,
    `Shop Link` VARCHAR(500),
    `Shop_Revenue` VARCHAR(255),
    `Shop_Self-Operated Account Revenue` VARCHAR(255),
    `Shop_Affiliate Revenue` VARCHAR(255),
    `Shop_Shopping Mall Revenue` VARCHAR(255),
    `Shop_Item Sold` VARCHAR(255),
    `Shop_Avg. Unit Price` VARCHAR(255),
    `Shop_Active Affiliates` VARCHAR(255),
    `Shop_New Videos By Affiliate` VARCHAR(255),
    `Date Filter` VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS shop_creators (
    id INT AUTO_INCREMENT PRIMARY KEY,
    `Shop Link` VARCHAR(500),
    `Creator Name` VARCHAR(255),
    `Account Type` VARCHAR(255),
    `TikTok Link` VARCHAR(500),
    `MCN` VARCHAR(255),
    `Debut Time` VARCHAR(255),
    `Creator Bio` TEXT,
    `Target Sexual` VARCHAR(255),
    `Target Age` VARCHAR(255),
    `Followers` VARCHAR(255),
    `Date Filter` VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS product_metrics (
    id INT AUTO_INCREMENT PRIMARY KEY,
    `Shop Link` VARCHAR(500),
    `Product Name` TEXT,
    `TikTok Product Link` VARCHAR(500),
    `Rating` VARCHAR(50),
    `Number of Reviews` VARCHAR(50),
    `Product SKUs` VARCHAR(50),
    `Product_Revenue` VARCHAR(255),
    `Product_Item Sold` VARCHAR(255),
    `Product_Avg. Unit Price` VARCHAR(255),
    `Product_Live Revenue` VARCHAR(255),
    `Product_Video Revenue` VARCHAR(255),
    `Product_Product Card` VARCHAR(255),
    `Product_Creator Conversion Ratio` VARCHAR(255),
    `Product_Concentration Rate (L30D)` VARCHAR(255),
    `Date Filter` VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS product_creators (
    id INT AUTO_INCREMENT PRIMARY KEY,
    `Shop Link` VARCHAR(500),
    `Product Name` TEXT,
    `Product Link` VARCHAR(500),
    `Creator Name` VARCHAR(255),
    `TikTok Link` VARCHAR(500),
    `MCN` VARCHAR(255),
    `Debut Time` VARCHAR(255),
    `Creator Bio` TEXT,
    `Target Sexual` VARCHAR(255),
    `Target Age` VARCHAR(255),
    `Followers` VARCHAR(255),
    `Date Filter` VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS videos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    `Shop Link` VARCHAR(500),
    `Product Name` TEXT,
    `Product Link` VARCHAR(500),
    `Creator Link` VARCHAR(500),
    `Link` VARCHAR(500),
    `Type` VARCHAR(50),
    `Revenue` VARCHAR(255),
    `Item Title` TEXT,
    `Video Duration` VARCHAR(100),
    `Publish Date` VARCHAR(100),
    `Advertising Period (Days)` VARCHAR(100),
    `Views` VARCHAR(100),
    `Item Sold` VARCHAR(100),
    `New Followers Generated` VARCHAR(100),
    `Ad View Ratio` VARCHAR(100),
    `Ad Revenue Ratio` VARCHAR(100),
    `Ads Spending` VARCHAR(100),
    `Ad ROAS` VARCHAR(100),
    `Date Filter` VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS lives (
    id INT AUTO_INCREMENT PRIMARY KEY,
    `Shop Link` VARCHAR(500),
    `Product Name` TEXT,
    `Product Link` VARCHAR(500),
    `Creator Link` VARCHAR(500),
    `Revenue` VARCHAR(255),
    `Link` VARCHAR(500),
    `Item Title` TEXT,
    `Livestream Time` VARCHAR(255),
    `Duration` VARCHAR(100),
    `Avg Online Viewer` VARCHAR(100),
    `Views` VARCHAR(100),
    `Item Sold` VARCHAR(100),
    `Avg Unit Price` VARCHAR(100),
    `Date Filter` VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS product_dim (
    id INT AUTO_INCREMENT PRIMARY KEY,
    `Shop Link` VARCHAR(500),
    `Product Name` TEXT,
    `Product Link` VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);