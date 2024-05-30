# Skyflow for Snowflake Setup Instructions

This is a demo script for Skyflow. Please ensure the Skyflow Setup worksheet has been completed before running this script.

## Steps

1. Create an account-level Service account with Account Admin role
2. Overwrite the credentials.json template in this repo directory with the credentials.json file downloaded from service account creation
3. Paste your account id into vault_details.cfg
4. In a terminal, navigate to this snowflake_udf_demo folder and run 'python script_setup.py' or 'python3 script_setup.py' (depending on your python version)
5. Import all three .sql files into Snowflake as Worksheets
6. Run the entire skyflow_udf_setup.sql worksheet end to end
7. Run the skyflow_udf_demo.sql worksheet line by line. Prior to running the tokenize table step, update the last parameter with your email address. Note that this email address must correspond to an existing Skyflow Studio user in the relevant account.
8. Upon running tokenize table step, there will be a new vault and table in skyflow. Continue through the demo steps accordingly.


-----Below is Deprecated-----
***Setup Demo***

1. **In Skyflow, [create a vault](https://docs.skyflow.com/create-a-vault/).**

2. **In your new Skyflow Vault, [create a table](https://docs.skyflow.com/create-a-vault/#edit-the-vault-schema) having the same name as your Snowflake table. For this demo, use the table name ```DOCTORS_DEMO```. Add a Skyflow table column for each Snowflake PII column you want to protect. Ensure the column names match. For this demo, add 4 columns: ```doctor_id```, ```name```, ```address```, ```email```, ```phone```**

3. **[Create a Service Account](https://docs.skyflow.com/api-authentication/#create-a-service-account) and save the downloaded `credentials.json` file.**

4. **In Snowflake, import the ```skyflow_udf_setup.sql``` file as a worksheet. Do not execute the worksheet yet.**

5. **Within the imported worksheet, replace the ```<TODO: SERVICE_ACCOUNT_CREDENTIALS_JSON>``` tag with the contents of the ```credentials.json``` file. You may copy-paste the entire string as-is.**

6. **Replace these additional TODO tags with the values found in the Skyflow Vault panel (settings icon on the left when viewing your vault): ```<TODO: VAULT_URL>```, ```<TODO: VAULT_ID>```, ```<TODO: ACCOUNT_ID>```.**

7. **You are now ready to run the setup worksheet! Execute the worksheet in full, and proceed to import the ```skyflow_udf_demo.sql``` file to start tokenizing.**

***Run Demo***

1. **In the ```skyflow_udf_demo.sql``` worksheet, execute statements 1-by-1 to walk through the demo incrementally, starting by setting your default database to ```SKYFLOW_DEMO``` as created by the Skyflow setup script**
    ```sql
    USE DATABASE SKYFLOW_DEMO;
    ```

2. **Check table to see initial plain text PII**
    ```sql
    SELECT * FROM DOCTORS_DEMO;
    ```

3. **Tokenize the table in one step, passing in table name as a parameter, and query the table again**
    ```sql
    CALL SKYFLOW_TOKENIZE_TABLE('DOCTORS_DEMO');
    ```
    ```sql
    SELECT * FROM DOCTORS_DEMO;
    ```

4. **Run a sample query without detokenizing. For example, the query for all doctors who started after year 2010. The PII data will be tokenized.**
    ```sql
    SELECT  DOCTOR_ID,
            NAME,
            EMAIL,
            SPECIALTY,
            EMPLOYMENT_START_DATE
    FROM DOCTORS_DEMO
    WHERE EMPLOYMENT_START_DATE > '2010';
    ```

5. **Now run the query using Skyflow detokenize to obtain PII data**
    ```sql
    SELECT  DOCTOR_ID,
            SKYFLOW_DETOKENIZE(NAME),
            SKYFLOW_DETOKENIZE(EMAIL),
            SPECIALTY,
            EMPLOYMENT_START_DATE
    FROM DOCTORS_DEMO
    WHERE EMPLOYMENT_START_DATE > '2010';
    ```

6. **Make some updates to the data. For example, delete a few records, update a record, and insert a few new records.**
    ```sql
    DELETE FROM DOCTORS_DEMO WHERE DOCTOR_ID = 1 OR DOCTOR_ID = 2;
    UPDATE DOCTORS_DEMO SET NAME = 'New Name', PHONE = '212-555-5555' WHERE DOCTOR_ID = 3;
    INSERT INTO DOCTORS_DEMO VALUES
        (101, 'John Smith', 'john@example.com', '718-444-5555', '123 Fake Street NY NY 10019', 'Cardiology', '2020-01-01'),
        (102, 'Harry Truman', 'harry@example.com', '718-333-5555', '234 Fake Street NY NY 10019', 'Podiatry', '2023-01-01'),
        (103, 'Sally Field', 'sally@exmaple.com', '718-222-5555', '345 Fake Street NY NY 10019', 'Surgeon', '2022-01-01');
    ```

7. **Check on data in snowflake table. The data will apear momentarily in plain text. This is due to Snowflake's "streaming" functionality, which streams data each minute to Skyflow to be tokenized.**
    ```sql
    SELECT * FROM DOCTORS_DEMO;
    ```

8. **Optional: Check on the status of Snowflake's streaming service**
    ```sql
    SELECT * FROM SKYFLOW_PII_STREAM_DOCTORS_DEMO SHOW_CURSOR;
    SELECT STATE, ERROR_MESSAGE, SCHEDULED_TIME
    FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME=>'SKYFLOW_PII_STREAM_DOCTORS_DEMO_TASK'))
    ORDER BY SCHEDULED_TIME DESC;
    ```

9. **Check on the data again after a few moments. Within a minute, the new PII data has been automatically tokenized.**
    ```sql
    SELECT * FROM DOCTORS_DEMO;
    ```

**This concludes the demo. To reset the Snowflake portion of the demo, import the ```skyflow_udf_reset.sql``` file as a worksheet and execute. You can then start again from the ```Run Demo``` portion of these instructions. Thank you!**