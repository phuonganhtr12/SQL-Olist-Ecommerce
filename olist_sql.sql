--Query 1. Create tables on database & use Python to import csv files into tables

CREATE TABLE customers (
    customer_id VARCHAR(100) PRIMARY KEY,
    customer_unique_id VARCHAR(100),
    customer_zip_code_prefix INTEGER,
    customer_city TEXT,
	customer_state TEXT
);

CREATE TABLE geolocation (
    geolocation_zip_code_prefix INTEGER,
	geolocation_lat NUMERIC,
	geolocation_lng NUMERIC,
	geolocation_city TEXT,
	geolocation_state TEXT
);

CREATE TABLE order_items (
    order_id VARCHAR(100),
    order_item_id VARCHAR(100),
    product_id VARCHAR(100),
    seller_id VARCHAR(100),
	shipping_limit_date TIMESTAMP,
	price NUMERIC,
	freight_value NUMERIC
);

CREATE TABLE order_payments (
	order_id VARCHAR(100),
	payment_sequential INTEGER,
	payment_type VARCHAR(50),
	payment_installments INTEGER,
	payment_value NUMERIC
);

CREATE TABLE order_reviews (
    reviews_id VARCHAR(50),
	order_id VARCHAR(50),
	review_score INTEGER,
	review_comment_title VARCHAR(100),
	review_comment_message TEXT,
	review_creation_date TIMESTAMP,
	review_answer_timestamp TIMESTAMP
);

CREATE TABLE orders (
    order_id VARCHAR(100) PRIMARY KEY,
	customer_id VARCHAR(100),
	order_status TEXT,
	order_purchase_timestamp TIMESTAMP,
	order_approved_at TIMESTAMP,
	order_delivered_carrier_date TIMESTAMP,
	order_delivered_customer_date TIMESTAMP,
	order_estimated_delivery_date TIMESTAMP
);

CREATE TABLE sellers (
    seller_id VARCHAR(100) PRIMARY KEY,
    seller_zip_code_prefix NUMERIC,
    seller_city TEXT,
    seller_state TEXT
);

CREATE TABLE products (
    product_id VARCHAR(100) PRIMARY KEY,
	product_category_name TEXT,
	product_name_lenght INTEGER,
	product_description_lenght INTEGER,
	product_photos_qty INTEGER,
	product_weight_g INTEGER,
	product_length_cm INTEGER,
	product_height_cm INTEGER,
	product_width_cm INTEGER
);

CREATE TABLE product_category_name_translation (
	product_category_name TEXT,
	product_category_name_english TEXT
);

--Query 2. Calculate customer growth & return rate, break down by month
WITH customer_first_purchase_month AS (
    SELECT 
        c.customer_unique_id,
        EXTRACT(YEAR FROM MIN(o.order_purchase_timestamp)) AS first_purchase_year,
        EXTRACT(MONTH FROM MIN(o.order_purchase_timestamp)) AS first_purchase_month
    FROM 
        customers c
    JOIN 
        orders o ON c.customer_id = o.customer_id
    GROUP BY 
        c.customer_unique_id
),

monthly_customer_growth AS (
	SELECT 
        EXTRACT(YEAR FROM o.order_purchase_timestamp) AS year,
		EXTRACT(MONTH FROM o.order_purchase_timestamp) AS month,
        COUNT(DISTINCT 
            CASE WHEN cf.first_purchase_year = EXTRACT(YEAR FROM o.order_purchase_timestamp)
			AND cf.first_purchase_month = EXTRACT(MONTH FROM o.order_purchase_timestamp)
            THEN cf.customer_unique_id END) AS new_customers,
        COUNT(DISTINCT 
            CASE WHEN cf.first_purchase_year < EXTRACT(YEAR FROM o.order_purchase_timestamp)
			OR (cf.first_purchase_year = EXTRACT(YEAR FROM o.order_purchase_timestamp)
			AND cf.first_purchase_month < EXTRACT(MONTH FROM o.order_purchase_timestamp))
            THEN cf.customer_unique_id END) AS returning_customers,
        COUNT(DISTINCT cf.customer_unique_id) AS total_customers
    FROM 
        orders o
	JOIN customers c
		ON o.customer_id = c.customer_id
    JOIN customer_first_purchase_month cf 
		ON c.customer_unique_id = cf.customer_unique_id
    GROUP BY 
        year, month
    ORDER BY 
        year, month
	)

SELECT 
    year,
    month,
	total_customers,
    new_customers,
	returning_customers,
    ROUND((CAST(total_customers - LAG(total_customers) OVER (ORDER BY year, month) AS numeric) / LAG(total_customers) OVER (ORDER BY year, month)) * 100, 2) AS total_customer_growth_rate,
	ROUND(CAST(returning_customers AS numeric) / total_customers * 100.00, 2) AS customer_return_rate
FROM 
    monthly_customer_growth;

--Query 3. Calculate average order value & averge number of items per order, break down by year
WITH order_value_item_count AS
    (SELECT order_id,
        COUNT(DISTINCT order_item_id) AS item_counts,
        SUM(price + freight_value) AS total_value
    FROM order_items
    GROUP BY order_id)

SELECT EXTRACT (YEAR FROM o.order_purchase_timestamp) AS year,
    EXTRACT (MONTH FROM o.order_purchase_timestamp) AS month,
    AVG(total_value) AS avg_order_value,
    ROUND(AVG(item_counts),2) AS avg_number_of_item
FROM order_value_item_count ovit
JOIN orders o
    ON ovit.order_id = o.order_id
WHERE order_status = 'delivered'
GROUP BY year, month

--Query 4. Calculating percentage of merchants having >= 96% on-time delivery rate, break down by month

WITH total_orders AS 
    (SELECT seller_id,
        EXTRACT (YEAR FROM o.order_purchase_timestamp) AS year,
		EXTRACT (MONTH FROM o.order_purchase_timestamp) AS month,
        COUNT(DISTINCT o.order_id) AS num_total_orders
    FROM orders o
    JOIN order_items oi
        ON o.order_id = oi.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY seller_id, year, month),
    
on_time_orders AS
    (SELECT oi.seller_id, 
        EXTRACT (YEAR FROM o.order_purchase_timestamp) AS year,
		EXTRACT (MONTH FROM o.order_purchase_timestamp) AS month,
        COUNT(DISTINCT oi.order_id) AS num_on_time_orders
    FROM orders o
    JOIN order_items oi
        ON o.order_id = oi.order_id
    WHERE o.order_status = 'delivered'
        AND EXTRACT (DAY FROM (o.order_delivered_customer_date - o.order_estimated_delivery_date)) <= 0
    GROUP BY oi.seller_id, year, month),

seller_on_time_delivery_rate AS
    (SELECT t.seller_id,
        t.year,
		t.month,
        t.num_total_orders,
        COALESCE(o.num_on_time_orders, 0) AS num_on_time_orders,
        ROUND((CAST(COALESCE(o.num_on_time_orders, 0) AS NUMERIC) * 100.0) / t.num_total_orders,2) AS on_time_delivery_rate
    FROM total_orders t
    LEFT JOIN on_time_orders o
        ON t.seller_id = o.seller_id 
		AND t.year = o.year
		AND t.month = o.month
    ORDER BY t.seller_id, t.year, t.month)
    
SELECT 
    year,
	month,
    COUNT(DISTINCT seller_id) AS total_sellers,
    COUNT(DISTINCT CASE WHEN on_time_delivery_rate >= 96.00 THEN seller_id END) AS num_qualified_sellers,
    ROUND(CAST(COUNT(DISTINCT CASE WHEN on_time_delivery_rate >= 96.00 THEN seller_id END) AS NUMERIC) / COUNT(DISTINCT seller_id) * 100.00,2) AS percentage_sellers_qualified
FROM seller_on_time_delivery_rate 
GROUP BY year, month; 

--Query 5. Calculate top 5 product category by revenue & their revenue contribution percentage, break down by year

WITH total_annual_revenue AS (
    SELECT 
        EXTRACT (YEAR FROM o.order_purchase_timestamp) AS year,
        SUM(oi.price + oi.freight_value) AS total_revenue
    FROM order_items oi
    JOIN orders o 
		ON oi.order_id = o.order_id
    GROUP BY year
),
ranked_categories AS (
    SELECT 
        EXTRACT (YEAR FROM o.order_purchase_timestamp) AS year,
        pt.product_category_name_english,
        SUM(oi.price + oi.freight_value) AS category_revenue,
        ROW_NUMBER() OVER (PARTITION BY EXTRACT (YEAR FROM o.order_purchase_timestamp) ORDER BY SUM(oi.price + oi.freight_value) DESC) AS rank
    FROM order_items oi
    JOIN products p ON oi.product_id = p.product_id
    JOIN product_category_name_translation pt ON p.product_category_name = pt.product_category_name
    JOIN orders o ON oi.order_id = o.order_id
    GROUP BY year, pt.product_category_name_english
)
SELECT 
    rc.year,
    rc.product_category_name_english,
    rc.category_revenue,
    ROUND((rc.category_revenue * 100.0) / tar.total_revenue, 2) AS category_revenue_percentage,
    rc.rank
FROM ranked_categories rc
JOIN total_annual_revenue tar ON rc.year = tar.year
WHERE rc.rank <= 5
ORDER BY rc.year ASC, rc.rank ASC;

--Query 5. Calculate year-on-year revenue growth for top 5 categories: 
'health_beauty', 'watches_gifts', 'bed_bath_table', 'sports_leisure', 'computers_accessories'
WITH category_revenue AS (
    SELECT 
        EXTRACT (YEAR FROM o.order_purchase_timestamp) AS year,
        pt.product_category_name_english,
        SUM(oi.price + oi.freight_value) AS total_revenue
    FROM order_items oi
    JOIN products p ON oi.product_id = p.product_id
    JOIN product_category_name_translation pt ON p.product_category_name = pt.product_category_name
    JOIN orders o ON oi.order_id = o.order_id
    GROUP BY year, pt.product_category_name_english
),
ranked_categories AS (
    SELECT 
        cr.year,
        cr.product_category_name_english,
        cr.total_revenue,
        LAG(cr.total_revenue) OVER (PARTITION BY cr.product_category_name_english ORDER BY cr.year) AS previous_year_revenue
    FROM category_revenue cr
),
category_growth AS (
    SELECT 
        year,
        product_category_name_english,
        total_revenue,
        previous_year_revenue,
        CASE 
            WHEN previous_year_revenue IS NOT NULL THEN ROUND((total_revenue - previous_year_revenue) * 100.0 / previous_year_revenue, 2)
            ELSE NULL
        END AS yoy_growth_percentage
    FROM ranked_categories
)

SELECT * 
FROM category_growth
WHERE product_category_name_english IN ('health_beauty', 'watches_gifts', 'bed_bath_table', 'sports_leisure', 'computers_accessories')
AND year IN ('2017', '2018')
ORDER BY product_category_name_english, year ASC, yoy_growth_percentage DESC;
