-- E-commerce Data Warehouse Schema
-- Snowflake DDL for comprehensive e-commerce analytics

-- Create database and schemas
CREATE DATABASE IF NOT EXISTS ECOMMERCE_DW;
USE DATABASE ECOMMERCE_DW;

CREATE SCHEMA IF NOT EXISTS RAW_DATA;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS ANALYTICS;
CREATE SCHEMA IF NOT EXISTS MART;

-- =====================================================
-- RAW DATA LAYER
-- =====================================================

USE SCHEMA RAW_DATA;

-- Customers table
CREATE OR REPLACE TABLE customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(20),
    date_of_birth DATE,
    gender VARCHAR(10),
    registration_date TIMESTAMP,
    address_line1 VARCHAR(255),
    address_line2 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    postal_code VARCHAR(20),
    country VARCHAR(100),
    preferred_language VARCHAR(10),
    marketing_consent BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Products table
CREATE OR REPLACE TABLE products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    description TEXT,
    unit_price DECIMAL(10,2),
    cost_price DECIMAL(10,2),
    weight DECIMAL(8,2),
    dimensions VARCHAR(50),
    color VARCHAR(50),
    size VARCHAR(20),
    stock_quantity INTEGER,
    is_active BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Orders table
CREATE OR REPLACE TABLE orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    order_date TIMESTAMP,
    order_status VARCHAR(50),
    total_amount DECIMAL(12,2),
    tax_amount DECIMAL(10,2),
    shipping_amount DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    payment_method VARCHAR(50),
    shipping_address_line1 VARCHAR(255),
    shipping_address_line2 VARCHAR(255),
    shipping_city VARCHAR(100),
    shipping_state VARCHAR(100),
    shipping_postal_code VARCHAR(20),
    shipping_country VARCHAR(100),
    delivery_date TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Order items table
CREATE OR REPLACE TABLE order_items (
    order_item_id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(50),
    product_id VARCHAR(50),
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    total_price DECIMAL(12,2),
    discount_amount DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- Website sessions table
CREATE OR REPLACE TABLE website_sessions (
    session_id VARCHAR(100) PRIMARY KEY,
    customer_id VARCHAR(50),
    session_start TIMESTAMP,
    session_end TIMESTAMP,
    device_type VARCHAR(50),
    browser VARCHAR(50),
    operating_system VARCHAR(50),
    traffic_source VARCHAR(100),
    utm_source VARCHAR(100),
    utm_medium VARCHAR(100),
    utm_campaign VARCHAR(100),
    pages_viewed INTEGER,
    bounce_rate DECIMAL(5,2),
    conversion_flag BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Page views table
CREATE OR REPLACE TABLE page_views (
    page_view_id VARCHAR(100) PRIMARY KEY,
    session_id VARCHAR(100),
    page_url VARCHAR(500),
    page_title VARCHAR(255),
    timestamp TIMESTAMP,
    time_on_page INTEGER,
    exit_page BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (session_id) REFERENCES website_sessions(session_id)
);

-- Marketing campaigns table
CREATE OR REPLACE TABLE marketing_campaigns (
    campaign_id VARCHAR(50) PRIMARY KEY,
    campaign_name VARCHAR(255),
    campaign_type VARCHAR(100),
    start_date DATE,
    end_date DATE,
    budget DECIMAL(12,2),
    target_audience VARCHAR(255),
    channel VARCHAR(100),
    status VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Customer support tickets table
CREATE OR REPLACE TABLE support_tickets (
    ticket_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    order_id VARCHAR(50),
    issue_type VARCHAR(100),
    priority VARCHAR(20),
    status VARCHAR(50),
    created_date TIMESTAMP,
    resolved_date TIMESTAMP,
    satisfaction_score INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

-- Reviews and ratings table
CREATE OR REPLACE TABLE product_reviews (
    review_id VARCHAR(50) PRIMARY KEY,
    product_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_id VARCHAR(50),
    rating INTEGER,
    review_text TEXT,
    helpful_votes INTEGER,
    verified_purchase BOOLEAN,
    review_date TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

-- =====================================================
-- STAGING LAYER
-- =====================================================

USE SCHEMA STAGING;

-- Staging tables with data quality checks and transformations
CREATE OR REPLACE TABLE stg_customers AS
SELECT 
    customer_id,
    TRIM(UPPER(first_name)) as first_name,
    TRIM(UPPER(last_name)) as last_name,
    LOWER(TRIM(email)) as email,
    phone,
    date_of_birth,
    CASE 
        WHEN UPPER(gender) IN ('M', 'MALE') THEN 'Male'
        WHEN UPPER(gender) IN ('F', 'FEMALE') THEN 'Female'
        ELSE 'Other'
    END as gender,
    registration_date,
    TRIM(address_line1) as address_line1,
    TRIM(address_line2) as address_line2,
    TRIM(city) as city,
    TRIM(state) as state,
    postal_code,
    TRIM(UPPER(country)) as country,
    preferred_language,
    marketing_consent,
    DATEDIFF('year', date_of_birth, CURRENT_DATE()) as age,
    CASE 
        WHEN DATEDIFF('year', date_of_birth, CURRENT_DATE()) < 25 THEN 'Gen Z'
        WHEN DATEDIFF('year', date_of_birth, CURRENT_DATE()) < 40 THEN 'Millennial'
        WHEN DATEDIFF('year', date_of_birth, CURRENT_DATE()) < 55 THEN 'Gen X'
        ELSE 'Baby Boomer'
    END as generation,
    created_at,
    updated_at
FROM RAW_DATA.customers
WHERE email IS NOT NULL AND email LIKE '%@%';

-- =====================================================
-- ANALYTICS LAYER
-- =====================================================

USE SCHEMA ANALYTICS;

-- Customer metrics fact table
CREATE OR REPLACE TABLE fact_customer_metrics (
    customer_id VARCHAR(50),
    metric_date DATE,
    total_orders INTEGER,
    total_revenue DECIMAL(12,2),
    avg_order_value DECIMAL(10,2),
    days_since_last_order INTEGER,
    lifetime_value DECIMAL(12,2),
    recency_score INTEGER,
    frequency_score INTEGER,
    monetary_score INTEGER,
    rfm_segment VARCHAR(20),
    churn_probability DECIMAL(5,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (customer_id, metric_date)
);

-- Product performance fact table
CREATE OR REPLACE TABLE fact_product_performance (
    product_id VARCHAR(50),
    metric_date DATE,
    units_sold INTEGER,
    revenue DECIMAL(12,2),
    avg_rating DECIMAL(3,2),
    review_count INTEGER,
    return_rate DECIMAL(5,4),
    stock_turnover DECIMAL(8,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (product_id, metric_date)
);

-- Marketing campaign performance
CREATE OR REPLACE TABLE fact_campaign_performance (
    campaign_id VARCHAR(50),
    metric_date DATE,
    impressions INTEGER,
    clicks INTEGER,
    conversions INTEGER,
    cost DECIMAL(10,2),
    revenue DECIMAL(12,2),
    ctr DECIMAL(5,4),
    conversion_rate DECIMAL(5,4),
    roas DECIMAL(8,2),
    cpa DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (campaign_id, metric_date)
);

-- =====================================================
-- DATA MART LAYER
-- =====================================================

USE SCHEMA MART;

-- Executive dashboard aggregations
CREATE OR REPLACE TABLE dm_executive_kpis (
    report_date DATE PRIMARY KEY,
    total_revenue DECIMAL(15,2),
    total_orders INTEGER,
    new_customers INTEGER,
    returning_customers INTEGER,
    avg_order_value DECIMAL(10,2),
    customer_acquisition_cost DECIMAL(10,2),
    customer_lifetime_value DECIMAL(12,2),
    churn_rate DECIMAL(5,4),
    monthly_active_users INTEGER,
    conversion_rate DECIMAL(5,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Customer cohort analysis
CREATE OR REPLACE TABLE dm_customer_cohorts (
    cohort_month DATE,
    period_number INTEGER,
    customers_count INTEGER,
    retention_rate DECIMAL(5,4),
    avg_revenue_per_user DECIMAL(10,2),
    cumulative_revenue DECIMAL(15,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (cohort_month, period_number)
);

-- Product category performance
CREATE OR REPLACE TABLE dm_category_performance (
    category VARCHAR(100),
    subcategory VARCHAR(100),
    report_date DATE,
    revenue DECIMAL(12,2),
    units_sold INTEGER,
    avg_price DECIMAL(10,2),
    profit_margin DECIMAL(5,4),
    market_share DECIMAL(5,4),
    growth_rate DECIMAL(5,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (category, subcategory, report_date)
);

-- Create indexes for better performance
CREATE INDEX idx_orders_customer_date ON RAW_DATA.orders(customer_id, order_date);
CREATE INDEX idx_order_items_product ON RAW_DATA.order_items(product_id);
CREATE INDEX idx_sessions_customer ON RAW_DATA.website_sessions(customer_id);
CREATE INDEX idx_reviews_product ON RAW_DATA.product_reviews(product_id);

-- Create views for easy access
CREATE OR REPLACE VIEW vw_customer_360 AS
SELECT 
    c.*,
    COUNT(o.order_id) as total_orders,
    SUM(o.total_amount) as total_spent,
    AVG(o.total_amount) as avg_order_value,
    MAX(o.order_date) as last_order_date,
    MIN(o.order_date) as first_order_date,
    DATEDIFF('day', MAX(o.order_date), CURRENT_DATE()) as days_since_last_order
FROM STAGING.stg_customers c
LEFT JOIN RAW_DATA.orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.email, c.phone, 
         c.date_of_birth, c.gender, c.registration_date, c.address_line1,
         c.address_line2, c.city, c.state, c.postal_code, c.country,
         c.preferred_language, c.marketing_consent, c.age, c.generation,
         c.created_at, c.updated_at;