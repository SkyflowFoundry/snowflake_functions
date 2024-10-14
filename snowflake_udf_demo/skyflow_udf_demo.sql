-- Step 0: Set default database to use. Please ensure Skyflow Setup worksheet has been run end-to-end.
USE DATABASE SKYFLOW_DEMO;

-- Step 1: Query the table to see initial plain text PII
SELECT * FROM CUSTOMERS;

-- Step 2: Tokenize the table
-- SKYFLOW_TOKENIZE_TABLE(VAULT_NAME, TABLE_NAME, PRIMARY_KEY, 'PII_COL1,PII_COL2,PII_COL3,PII_COL4,...', 'VAULT_OWNER_EMAIL');
CALL SKYFLOW_TOKENIZE_TABLE('SkyflowPIIVault', 'CUSTOMERS', 'CUSTOMER_ID', 'NAME,EMAIL,PHONE,ADDRESS', 'sam@skyflow.com');
--   SKYFLOW_TOKENIZE_TABLE(VAULT_NAME       , TABLE_NAME , PRIMARY_KEY  , 'PII_COL1,PII_COL2,...'   , 'VAULT_OWNER_EMAIL');

-- Step 3: Query the table data again (PII is tokenized!)
SELECT * FROM CUSTOMERS;

-- Step 4: Switch to ROLE_DATA_ENGINEER (Masked/Partial PII access) and query the data again
USE ROLE ROLE_DATA_ENGINEER;
SELECT * FROM CUSTOMERS

-- Query to find the percentage distribution of email domains
SELECT 
    LOWER(SUBSTRING(EMAIL, CHARINDEX('@', EMAIL) + 1, LEN(EMAIL))) AS DOMAIN, 
    COUNT(*) AS DOMAIN_COUNT,
    ROUND((COUNT(*) * 100.0) / SUM(COUNT(*)) OVER (), 2) AS PERCENTAGE_DISTRIBUTION
FROM SKYFLOW_DEMO.PUBLIC.CUSTOMERS
GROUP BY DOMAIN
ORDER BY PERCENTAGE_DISTRIBUTION DESC;

-- Step 5: Switch to ROLE_MARKETING (No PII access) and query the data again
USE ROLE ROLE_MARKETING;
SELECT * FROM CUSTOMERS;

-- Query to calculate the average lifetime purchase amount across all customers
SELECT AVG(LIFETIME_PURCHASE_AMOUNT) AS AVG_PURCHASE_AMOUNT FROM SKYFLOW_DEMO.PUBLIC.CUSTOMERS;

-- Query to group customers into tiers based on lifetime purchase amount and calculate the average purchase amount for each tier
SELECT 
    CASE 
        WHEN LIFETIME_PURCHASE_AMOUNT > 10000 THEN 'Platinum' 
        WHEN LIFETIME_PURCHASE_AMOUNT BETWEEN 5000 AND 10000 THEN 'Gold' 
        WHEN LIFETIME_PURCHASE_AMOUNT BETWEEN 1000 AND 4999 THEN 'Silver' 
        ELSE 'Bronze' 
    END AS CUSTOMER_TIER, 
    COUNT(*) AS CUSTOMER_COUNT, 
    AVG(LIFETIME_PURCHASE_AMOUNT) AS AVERAGE_PURCHASE_AMOUNT
FROM SKYFLOW_DEMO.PUBLIC.CUSTOMERS 
GROUP BY CUSTOMER_TIER;

-- Query to get the top 10 customers by lifetime purchase amount
SELECT CUSTOMER_ID, LIFETIME_PURCHASE_AMOUNT FROM SKYFLOW_DEMO.PUBLIC.CUSTOMERS ORDER BY LIFETIME_PURCHASE_AMOUNT DESC LIMIT 10;

-- Query to get the top 10 customers by lifetime purchase amount... as well as their phone number and email address...
SELECT CUSTOMER_ID, PHONE, EMAIL, LIFETIME_PURCHASE_AMOUNT FROM SKYFLOW_DEMO.PUBLIC.CUSTOMERS ORDER BY LIFETIME_PURCHASE_AMOUNT DESC LIMIT 10;

-- Step 6: Switch to ROLE_AUDIT_ADMIN role (Full PII access) and query the data again
USE ROLE ROLE_AUDIT_ADMIN;

-- Query to get the top 10 customers by lifetime purchase amount... as well as their phone number and email address...
SELECT CUSTOMER_ID, PHONE, EMAIL, LIFETIME_PURCHASE_AMOUNT FROM SKYFLOW_DEMO.PUBLIC.CUSTOMERS ORDER BY LIFETIME_PURCHASE_AMOUNT DESC LIMIT 10;

SELECT * FROM CUSTOMERS;

-- Step 7: Delete and add some data
DELETE FROM CUSTOMERS WHERE CUSTOMER_ID = 1 OR CUSTOMER_ID = 2; -- Delete a few records
INSERT INTO CUSTOMERS (NAME, EMAIL, PHONE, ADDRESS, LIFETIME_PURCHASE_AMOUNT, CUSTOMER_SINCE) VALUES 
    ('John Smith', 'john@example.com', '555-222-5555', '123 Fake Street NY NY 10019', 5000, '2020-01-01'),
    ('Harry Truman', 'harry@example.com', '555-333-5555', '234 Fake Street NY NY 10019', 6000, '2023-01-01'),
    ('Sally Field', 'sally@example.com', '555-444-5555', '345 Fake Street NY NY 10019', 99, '2022-01-01');

-- Step 8: Check on data in snowflake table
SELECT * FROM CUSTOMERS;

-- Step 9: Check on snowflake sync status
SELECT * FROM SKYFLOW_PII_STREAM_CUSTOMERS SHOW_CURSOR;
SELECT STATE, ERROR_MESSAGE, SCHEDULED_TIME
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME=>'SKYFLOW_PII_STREAM_CUSTOMERS_TASK'))
ORDER BY SCHEDULED_TIME DESC;

-- Step 10: Check on data in snowflake table, PII has been tokenized automatically
SELECT * FROM CUSTOMERS;