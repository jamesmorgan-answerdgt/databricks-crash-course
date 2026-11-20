CREATE TABLE dev.bronze.sales_customers_raw(
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email_address VARCHAR(255),
    phone_number VARCHAR(255),
    address	VARCHAR(255),
    city VARCHAR(255),
    state VARCHAR(255),
    country	VARCHAR(255),
    continent VARCHAR(255),
    postal_zip_code	VARCHAR(255),
    gender VARCHAR(255)
)
USING DELTA
COMMENT 'The table contains data related to customers'
CLUSTER BY AUTO;