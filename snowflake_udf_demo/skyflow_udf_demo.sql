-- Step 0: Set default database to use. Please ensure Skyflow Setup worksheet has been completed.
USE DATABASE SKYFLOW_DEMO;

-- Step 1: Check table to see initial plain text PII
SELECT * FROM CUSTOMERS;

-- Step 2: Tokenize the table, passing in table name as a parameter, and query the table again.
--   SKYFLOW_TOKENIZE_TABLE(VAULT_NAME, TABLE_NAME, PRIMARY_KEY, 'PII_COL1,PII_COL2,PII_COL3,PII_COL4,...', 'VAULT_OWNER_EMAIL');

CALL SKYFLOW_TOKENIZE_TABLE('SkyflowVault', 'CUSTOMERS', 'CUSTOMER_ID', 'NAME,EMAIL,PHONE,ADDRESS', 'yourname@yourdomain.com');

-- Step 3a: Run a sample query without detokenizing, for example the customers who joined after year 2010. PII will be tokenized.
SELECT  CUSTOMER_ID,
        NAME,
        EMAIL,
        LIFETIME_PURCHASE_AMOUNT,
        CUSTOMER_SINCE
FROM CUSTOMERS
WHERE CUSTOMER_SINCE > '2010';

-- Step 3b: Now run the query using Skyflow detokenize to obtain PII data.
SELECT  CUSTOMER_ID,
        SKYFLOW_DETOKENIZE(NAME),
        SKYFLOW_DETOKENIZE(EMAIL),
        LIFETIME_PURCHASE_AMOUNT,
        CUSTOMER_SINCE
FROM CUSTOMERS
WHERE CUSTOMER_SINCE > '2010';

-- Step 4: Make some updates to the data
DELETE FROM CUSTOMERS WHERE CUSTOMER_ID = 1 OR CUSTOMER_ID = 2; -- Delete a few records
UPDATE CUSTOMERS SET NAME = 'Michael Smith', PHONE = '555-111-5555' WHERE CUSTOMER_ID = 3; -- Update a record
INSERT INTO CUSTOMERS VALUES -- Insert a few new records
    (101, 'John Smith', 'john@example.com', '555-222-5555', '123 Fake Street NY NY 10019', '$5000', '2020-01-01'),
    (102, 'Harry Truman', 'harry@example.com', '555-333-5555', '234 Fake Street NY NY 10019', '$6000', '2023-01-01'),
    (103, 'Sally Field', 'sally@exmaple.com', '555-444-5555', '345 Fake Street NY NY 10019', '$99', '2022-01-01');

-- Step 5: Check on data in snowflake table
SELECT * FROM CUSTOMERS;

-- Step 6: Check on snowflake sync status
SELECT * FROM SKYFLOW_PII_STREAM_CUSTOMERS SHOW_CURSOR;
SELECT STATE, ERROR_MESSAGE, SCHEDULED_TIME
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME=>'SKYFLOW_PII_STREAM_CUSTOMERS_TASK'))
ORDER BY SCHEDULED_TIME DESC;

-- Step 7: Check on data in snowflake table, PII has been tokenized automatically
SELECT * FROM CUSTOMERS;
