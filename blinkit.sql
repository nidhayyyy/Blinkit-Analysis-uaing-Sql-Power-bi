create database Blinkit;
use Blinkit;

CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(100),
    `area` VARCHAR(100),
    pincode VARCHAR(10),
    registration_date DATE,
    customer_segment VARCHAR(50),
    total_orders INT,
    average_order_value DECIMAL(10, 2)
);

CREATE TABLE Products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    brand VARCHAR(50),
    price DECIMAL(10, 2),
    mrp DECIMAL(10, 2),
    discount_percentage DECIMAL(5, 2),
    shelf_life_days INT,
    min_stock_level INT,
    max_stock_level INT
);

CREATE TABLE Orders (
    order_id BIGINT PRIMARY KEY,
    customer_id INT,
    order_date DATE,
    promised_delivery_time DATETIME,
    actual_delivery_time DATETIME,
    delivery_status VARCHAR(50),
    order_total DECIMAL(10, 2),
    payment_method VARCHAR(50),
    delivery_partner_id INT,
    store_id INT,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE Inventory (
    product_id INT,
    `date` DATE,
    stock_received INT,
    damaged_stock INT,
    PRIMARY KEY (product_id, `date`),
    FOREIGN KEY (product_id) REFERENCES Products(product_id)
);

CREATE TABLE Order_items (
    order_id BIGINT,
    product_id INT,
    quantity INT,
    unit_price DECIMAL(10, 2),
    PRIMARY KEY (order_id, product_id),
    FOREIGN KEY (order_id) REFERENCES Orders(order_id),
    FOREIGN KEY (product_id) REFERENCES Products(product_id)
);

CREATE TABLE Delivery_performance (
    order_id BIGINT,
    delivery_partner_id INT,
    promised_time DATETIME,
    actual_time DATETIME,
    delivery_time_minutes INT,
    distance_km DECIMAL(6, 2),
    delivery_status VARCHAR(50),
    reasons_if_delayed TEXT,
    PRIMARY KEY (order_id, delivery_partner_id),
    FOREIGN KEY (order_id) REFERENCES Orders(order_id)
);

CREATE TABLE Customer_feedback (
    feedback_id INT PRIMARY KEY,
    order_id BIGINT,
    customer_id INT,
    rating INT CHECK (rating BETWEEN 1 AND 5),
    feedback_category VARCHAR(50),
    sentiment VARCHAR(50),
    feedback_date DATE,
    FOREIGN KEY (order_id) REFERENCES Orders(order_id),
    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id)
);

CREATE TABLE Marketing_performance (
    campaign_id INT PRIMARY KEY,
    campaign_name VARCHAR(100),
    `date` DATE,
    target_audience VARCHAR(100),
    `channel` VARCHAR(50),
    impressions INT,
    clicks INT,
    conversions INT,
    spend DECIMAL(10, 2),
    revenue_generated DECIMAL(10, 2),
    roas DECIMAL(5, 2)
);

-- CUSTOMER ANALYSIS
-- 1. CUSTOMER SEGMENTS WITH HIGHEST NUMBER OF ORDERS
CREATE VIEW cust_segment_orders AS 
SELECT customer_segment, SUM(total_orders) AS total_orders
FROM customers
GROUP BY customer_segment
ORDER BY total_orders DESC;

-- 2. AVERAGE ORDER VALUE BY CUSTOMER SEGMENT AND AREA
CREATE VIEW avg_value_segment_area AS
SELECT customer_segment, `area`, AVG(average_order_value) AS avg_order_value
FROM customers
GROUP BY customer_segment,`area`
ORDER BY avg_order_value DESC;

-- 3. AREAS WITH MOST ACTIVE CUSTOMERS(BY ORDER VOLUME)
CREATE VIEW active_customers_area AS
SELECT `area`, COUNT(customer_id) AS active_customers, SUM(total_orders) AS total_orders
FROM customers
GROUP BY `area`
ORDER BY total_orders DESC;

-- 4. CUSTOMERS JOINED MONTH-OVER-MONTH
CREATE VIEW new_customers_monthly AS
SELECT DATE_FORMAT(registration_date,'%Y-%m') AS `MONTH`, COUNT(customer_id) AS new_customers
FROM customers
GROUP BY `month`
ORDER BY `month`;

-- 5. CUSTOMER SEGMENTS WITH HIGH FREQUENCY BUT LOW ORDER VALUE
CREATE VIEW high_freq_low_value_customers AS
SELECT customer_segment, AVG(total_orders) AS avg_orders, AVG(average_order_value) AS avg_order_value
FROM customers
GROUP BY customer_segment
HAVING avg_orders > (SELECT AVG(total_orders) FROM CUSTOMERS) 
     AND avg_order_value < (SELECT AVG(average_order_value) FROM customers);

-- ORDER ANALYSIS
-- 1. MONTHLY TREND OF TOTAL ORDERS AND TOTAL ORDER VALUE
CREATE VIEW monthly_orders_trend AS
SELECT DATE_FORMAT(order_date,'%Y-%m') AS `month`, COUNT(order_id) AS total_orders, SUM(order_total) AS total_order_value
FROM orders
GROUP BY `month`
ORDER BY `month`;

-- 2. DISTRIBUTION OF ORDERS BY PAYMENT METHOD
CREATE VIEW orders_by_payment_method AS
SELECT payment_method, COUNT(*) AS total_orders
FROM orders
GROUP BY payment_method
ORDER BY total_orders DESC;

-- 3. ORDERS DELIVERED ON TIME VS DELAYED
CREATE VIEW delivery_status_distribution AS
SELECT delivery_status, COUNT(*) AS total_orders
FROM orders
GROUP BY delivery_status;

-- 4. AVERGE ORDER VALUE PER STORE AND DELIVERY PARTNER
CREATE VIEW avg_order_value_store_partner AS
SELECT store_id, delivery_partner_id, AVG(order_total) AS avg_order_value
FROM orders
GROUP BY store_id, delivery_partner_id;

-- 5. BUSIEST DAY/HOURS IN TERMS OF ORDER VOLUME
CREATE VIEW busiest_order_times AS
SELECT DAYNAME(order_date) AS day_of_week, HOUR(order_date) AS `hour`,COUNT(order_id) AS order_count
FROM orders
GROUP BY day_of_week, `hour`
ORDER BY order_count DESC;

-- ORDER ITEMS AND PRODUCT ANALYSIS
-- 1. MOST PURCHASED PRODUCT CATEGORIES AND BRANDS
CREATE VIEW top_categories_brands AS
SELECT p.category, p.brand, SUM(oi.quantity) AS total_quantity
FROM order_items oi
JOIN products p ON oi.product_id = p.product_id
GROUP BY p.category, p.brand
ORDER BY total_quantity DESC;

-- 2.AVERAGE QUANTITY ORDERED PER PRODUCT CATEGORY
CREATE VIEW avg_quantity_per_category AS
SELECT p.category, AVG(oi.quantity) AS avg_quantity
FROM order_items oi
JOIN products p ON oi.product_id = p.product_id
GROUP BY p.category;

-- 3. ACTUAL REVENUE VS MRP REVENUE PER PRODUCT CATEGORY
CREATE VIEW revenue_vs_mrp_by_category AS
SELECT p.category,
      SUM(oi.quantity * oi.unit_price) AS actual_revenue,
      SUM(oi.quantity * p.mrp) AS mrp_revenue
FROM order_items oi
JOIN products p ON oi.product_id = p.product_id 
GROUP BY p.category;   

-- 4. ARE DISCOUNTS INFLUENCING ORDER QUANTITY ?
CREATE VIEW discount_vs_quantity AS
SELECT p.discount_percentage, AVG(oi.quantity) AS avg_quantity
FROM order_items oi
JOIN products p ON oi.product_id = p.product_id 
GROUP BY  p.discount_percentage
ORDER BY p.discount_percentage;

-- 5. PRODUCTS WITH HIGHEST MARGIN (MRP - PRICE)
CREATE VIEW top_margin_products AS
SELECT product_id, product_name, (mrp - price) AS margin
FROM products
ORDER BY margin
LIMIT 10;

-- INVENTORY PERFORMANCE
-- 1. MONTHLY STOCK VS DAMAGED RATIO
CREATE VIEW stock_damage_ratio AS
SELECT DATE_FORMAT(`date`,'%Y-%m') AS `month`,
      SUM(stock_received) AS total_stock_received,
      SUM(damaged_stock) AS total_damaged_stock,
      ROUND(SUM(stock_received)/SUM(damaged_stock),2) AS damaged_ratio
FROM inventory      
GROUP BY `month`
ORDER BY `month`;

-- 2. PRODUCTS WHICH HIT minimum stock level
CREATE VIEW low_stock_products AS
SELECT i.product_id, p.product_name, p.min_stock_level,
       SUM(stock_received - damaged_stock) AS net_stock
FROM Inventory i
JOIN Products p ON i.product_id = p.product_id
GROUP BY i.product_id, p.product_name, p.min_stock_level
HAVING net_stock <= p.min_stock_level;

-- 3. BRANDS FACING RECUURING STOCK ISSUES
CREATE VIEW low_stock_brands AS
SELECT p.brand, COUNT(*) AS low_stock_instances
FROM Inventory i
JOIN Products p ON i.product_id = p.product_id
WHERE (stock_received - damaged_stock) <= p.min_stock_level
GROUP BY p.brand
ORDER BY low_stock_instances DESC;      

-- 4. STOCK VAILABILITY VARIES ACCROSS CATEGORIES
CREATE VIEW stock_variation_category AS
SELECT p.category, SUM(stock_received-damaged_stock) AS available_stock
FROM inventory i
JOIN products p ON i.product_id = p.product_id
GROUP BY p.category
ORDER BY available_stock DESC;

-- DELIVERY PERFORMANCE
-- 1. AVERAGE DILEVERY TIME TAKEN BY PARTNER
CREATE VIEW avg_delivery_time_by_partner AS
SELECT delivery_partner_id, AVG(delivery_time_minutes) AS avg_delivery_time
FROM delivery_performance
GROUP BY delivery_partner_id;

-- 2. DELIVERIES DELAYED AND TOP REASONS
CREATE VIEW top_delay_reasons AS
SELECT reasons_if_delayed, COUNT(*) AS delay_count
FROM Delivery_performance
WHERE delivery_status = 'Significantly Delayed'
GROUP BY reasons_if_delayed
ORDER BY delay_count DESC;

-- 3. DELIVERY TIME VARIES WITH DISTANCE
CREATE VIEW delivery_time_vs_distance AS
SELECT distance_km, AVG(delivery_time_minutes) AS avg_delivery_time
FROM delivery_performance
GROUP BY distance_km
ORDER BY distance_km;
      
-- 4. ON-TIME DELIVERY RATE BY STORE
CREATE VIEW ontime_delivery_by_store AS
SELECT o.store_id,
       ROUND(SUM(CASE WHEN d.delivery_status = 'On Time' then 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS oo_time_rate
FROM delivery_performance d
JOIN orders o ON d.order_id = o.order_id
GROUP BY o.store_id;       

-- 5. DELIVERY PARTNER WHO CONSISTENTLY DELAYED PROMISED TIME
CREATE VIEW late_deliveries_by_partner AS
SELECT delivery_partner_id, COUNT(*) AS delayed_orders
FROM Delivery_performance
WHERE delivery_status = 'Significantly Delayed'
GROUP BY delivery_partner_id
ORDER BY delayed_orders DESC;

-- MARKETING PERFORMANCE
-- 1. CAMPAIGN HAVING HIGHEST ROAS
CREATE VIEW top_campaigns_roas AS
SELECT campaign_id, campaign_name, roas
FROM marketing_performance
ORDER BY roas DESC
LIMIT 10;

-- 2. MOST EFFECTIVE MAREKTING CHANNELS ACCORDING TO CONVERSIONS
CREATE VIEW conversions_by_channel AS
SELECT `channel`, SUM(conversions) AS total_conversions
FROM marketing_performance
GROUP BY `channel`
ORDER BY total_conversions DESC;

-- 3. MARKETING SPEND VS REVENUE SPEND MONTH OVER MONTH
CREATE VIEW monthly_spend_vs_revenue AS
SELECT DATE_FORMAT(`date`,'%Y-%m') AS `month`,
       SUM(spend) AS total_spend,
       SUM(revenue_generated) AS total_revenue
FROM marketing_performance
GROUP BY `month`
ORDER BY `month`;   

-- 4. CAMPAIGN PERFORMANCE BY AUDIENCE
CREATE VIEW campaign_conversions_by_audience AS
SELECT campaign_name, target_audience, SUM(conversions) AS total_conversions
FROM marketing_performance
GROUP BY target_audience, campaign_name;

-- CUSTOMER FEEDBACK AND SENTIMENT
-- 1. AVERAGE CUSTOMER RATING ACCROSS ALL ORDERS
CREATE VIEW avg_customer_rating AS
SELECT ROUND(AVG(rating), 2) AS avg_rating
FROM customer_feedback;

-- 2. AREAS AND SEGMENTS WITH MOST NEGATIVE FEEDBACK
CREATE VIEW negative_feedback_area_seg AS
SELECT c.area, c.customer_segment, COUNT(*) AS negative_feedbacks
FROM customer_feedback f
JOIN customers c ON f.customer_id = c.customer_id
WHERE sentiment = 'Negative'
GROUP BY c.area, c.customer_segment
ORDER BY negative_feedbacks DESC;

-- 3. TOP RECURRING ISSUES BASED ON FEEDBACK CATEGORIES
CREATE VIEW top_feedback_issues AS
SELECT feedback_category, COUNT(*) AS issue_count
FROM customer_feedback
GROUP BY feedback_category
ORDER BY issue_count DESC;

-- 4. DELIVERY PARTNERS OR PRODUCTS LINKED TO NEGATIVE FEEDBACKS
-- Delivery partner level
CREATE VIEW negative_feedback_by_partner AS
SELECT o.delivery_partner_id, COUNT(*) AS negative_feedbacks
FROM Customer_feedback f
JOIN Orders o ON f.order_id = o.order_id
WHERE f.sentiment = 'Negative'
GROUP BY o.delivery_partner_id
ORDER BY negative_feedbacks DESC;

-- Product level
CREATE VIEW negative_feedback_by_products AS
SELECT oi.product_id, p.product_name, COUNT(*) AS negative_feedbacks
FROM Customer_feedback f
JOIN Order_items oi ON f.order_id = oi.order_id
JOIN Products p ON oi.product_id = p.product_id
WHERE f.sentiment = 'Negative'
GROUP BY oi.product_id, p.product_name
ORDER BY negative_feedbacks DESC;

-- 5. CUSTOMER SENTIMENT EVOLVE OVER TIME
CREATE VIEW sentiment_over_time AS
SELECT DATE_FORMAT(feedback_date, '%Y-%m') AS `month`,
       sentiment, COUNT(*) AS sentiment_count
FROM Customer_feedback
GROUP BY `month`, sentiment
ORDER BY `month`;




