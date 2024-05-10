-- Step 0: Set default database to use. Please ensure Skyflow Setup worksheet has been completed.
USE DATABASE SKYFLOW_DEMO;

-- Step 1: Check table to see initial plain text PII
SELECT * FROM DOCTORS_DEMO;

-- Step 2: Tokenize the table, passing in table name as a parameter, and query the table again.
CALL SKYFLOW_TOKENIZE_TABLE('DOCTORS_DEMO');

-- Step 3a: Run a sample query without detokenizing, for example the doctors who started after year 2010. PII will be tokenized.
SELECT  DOCTOR_ID,
        NAME,
        EMAIL,
        SPECIALTY,
        EMPLOYMENT_START_DATE
FROM DOCTORS_DEMO
WHERE EMPLOYMENT_START_DATE > '2010';

-- Step 3b: Now run the query using Skyflow detokenize to obtain PII data.
SELECT  DOCTOR_ID,
        SKYFLOW_DETOKENIZE(NAME),
        SKYFLOW_DETOKENIZE(EMAIL),
        SPECIALTY,
        EMPLOYMENT_START_DATE
FROM DOCTORS_DEMO
WHERE EMPLOYMENT_START_DATE > '2010';

-- Step 4: Make some updates to the data
DELETE FROM DOCTORS_DEMO WHERE DOCTOR_ID = 1 OR DOCTOR_ID = 2; -- Delete a few records
UPDATE DOCTORS_DEMO SET NAME = 'New Name', PHONE = '212-555-5555' WHERE DOCTOR_ID = 3; -- Update a record
INSERT INTO DOCTORS_DEMO VALUES -- Insert a few new records
    (101, 'John Smith', 'john@example.com', '718-444-5555', '123 Fake Street NY NY 10019', 'Cardiology', '2020-01-01'),
    (102, 'Harry Truman', 'harry@example.com', '718-333-5555', '234 Fake Street NY NY 10019', 'Podiatry', '2023-01-01'),
    (103, 'Sally Field', 'sally@exmaple.com', '718-222-5555', '345 Fake Street NY NY 10019', 'Surgeon', '2022-01-01');

-- Step 5: Check on data in snowflake table
SELECT * FROM DOCTORS_DEMO;

-- Step 6: Check on snowflake sync status
SELECT * FROM SKYFLOW_PII_STREAM_DOCTORS_DEMO SHOW_CURSOR;
SELECT STATE, ERROR_MESSAGE, SCHEDULED_TIME
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME=>'SKYFLOW_PII_STREAM_DOCTORS_DEMO_TASK'))
ORDER BY SCHEDULED_TIME DESC;

-- Step 7: Check on data in snowflake table, PII has been tokenized automatically
SELECT * FROM DOCTORS_DEMO;
