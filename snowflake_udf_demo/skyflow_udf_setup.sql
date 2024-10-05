-- Step 1: Create Database
CREATE OR REPLACE DATABASE SKYFLOW_DEMO;
USE DATABASE SKYFLOW_DEMO;

-- Step 2: Create table with PII and Non-PII columns
CREATE OR REPLACE TABLE SKYFLOW_DEMO.PUBLIC.CUSTOMERS (
    CUSTOMER_ID NUMBER(38,0) IDENTITY(1,1),  -- Auto-incrementing column
	NAME VARCHAR(16777216),
	EMAIL VARCHAR(16777216),
	PHONE VARCHAR(16777216),
    ADDRESS VARCHAR(16777216),
    LIFETIME_PURCHASE_AMOUNT NUMBER(10, 2),
    CUSTOMER_SINCE VARCHAR(16777216)
);

CREATE OR REPLACE TABLE SKYFLOW_DEMO.PUBLIC.SKYFLOW_CACHE (
    key VARCHAR PRIMARY KEY,
    value VARCHAR, -- The cached value, stored as a string
    expiry TIMESTAMP_LTZ -- Expiry time, after which the cache should be considered invalid
);

INSERT INTO SKYFLOW_DEMO.PUBLIC.CUSTOMERS (NAME, EMAIL, PHONE, ADDRESS, LIFETIME_PURCHASE_AMOUNT, CUSTOMER_SINCE)
SELECT 

    -- Simplified name generation
    CONCAT(
        CASE WHEN RANDOM() > 0.5 THEN 'Mr. ' ELSE 'Ms. ' END,
        CASE WHEN RANDOM() < 0.25 THEN 'John ' 
             WHEN RANDOM() < 0.5 THEN 'Robert ' 
             WHEN RANDOM() < 0.75 THEN 'James ' 
             ELSE 'William ' END, 
        INITCAP(SUBSTR(MD5(RANDOM()), 1, 10))
    ) AS NAME,

    -- Simplified email generation based on name
    LOWER(CONCAT(
        CASE WHEN RANDOM() > 0.5 THEN 'mr.' ELSE 'ms.' END, 
        CASE WHEN RANDOM() < 0.25 THEN 'john.' 
             WHEN RANDOM() < 0.5 THEN 'robert.' 
             WHEN RANDOM() < 0.75 THEN 'james.' 
             ELSE 'william.' END, 
        SUBSTR(MD5(RANDOM()), 1, 10), 
        CASE WHEN RANDOM() > 0.5 THEN '@example.com' ELSE '@company.com' END
    )) AS EMAIL,

    -- Phone number with (XXX) 555-XXXX format
    CONCAT(
        '+1 (', UNIFORM(200, 999, RANDOM()), ') 555-',  -- Random 3-digit area code and fixed 555 exchange
        LPAD(TO_VARCHAR(UNIFORM(1000, 9999, RANDOM())), 4, '0')  -- Subscriber number
    ) AS PHONE,

    -- Simplified address generation
    CONCAT(
        UNIFORM(100, 999, RANDOM()), ' ', 
        CASE WHEN RANDOM() < 0.25 THEN 'Main St' 
             WHEN RANDOM() < 0.5 THEN 'Maple Ave' 
             WHEN RANDOM() < 0.75 THEN 'Elm St' 
             ELSE 'Oak St' END, ', ', 
        CASE WHEN RANDOM() < 0.25 THEN 'New York' 
             WHEN RANDOM() < 0.5 THEN 'Los Angeles' 
             WHEN RANDOM() < 0.75 THEN 'Chicago' 
             ELSE 'San Francisco' END, ' ', 
        SUBSTR(MD5(RANDOM()), 1, 2), ' ', 
        UNIFORM(10000, 99999, RANDOM())
    ) AS ADDRESS,

    -- Random lifetime purchase amount
    ROUND(UNIFORM(0, 10000, RANDOM())::NUMERIC(10,2), 2) AS LIFETIME_PURCHASE_AMOUNT,

    -- Random 'customer since' date
    CONCAT(UNIFORM(2000, 2022, RANDOM()), '-', 
           LPAD(TO_VARCHAR(UNIFORM(1, 12, RANDOM())), 2, '0'), '-', 
           LPAD(TO_VARCHAR(UNIFORM(1, 28, RANDOM())), 2, '0')) AS CUSTOMER_SINCE

FROM TABLE(GENERATOR(ROWCOUNT => 100));

-- Step 4: Insert sample records into table

-- Step 5: In Skyflow Studio, create a Service Account having Vault Owner access. Upon creation, a credentials.json file will be downloaded. Use it in the next step.
-- Step 6: Store Skyflow service account key with Snowflake Secrets Manager, pasting in the contents of the credentials.json file into the SECRET_STRING variable.
CREATE OR REPLACE SECRET SKYFLOW_VAULT_SECRET
    TYPE = GENERIC_STRING
    SECRET_STRING = '<TODO: SERVICE_ACCOUNT_CREDENTIALS>';

    
-- Step 5: Create the external networking rules to enable Snowflake to access Skyflow
CREATE OR REPLACE NETWORK RULE SKYFLOW_APIS_NETWORK_RULE -- Grant access to the Skyflow API endpoints for authentication and vault APIs
 MODE = EGRESS
 TYPE = HOST_PORT
 VALUE_LIST = ('ebfc9bee4242.vault.skyflowapis.com', 'manage.skyflowapis.com');
 
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION SKYFLOW_EXTERNAL_ACCESS_INTEGRATION -- Create an integration using the network rule and secret
 ALLOWED_NETWORK_RULES = (SKYFLOW_APIS_NETWORK_RULE)
 ALLOWED_AUTHENTICATION_SECRETS = (SKYFLOW_VAULT_SECRET)
 ENABLED = true;

 
-- Step 6: Create Stored Procedure for table tokenization.
CREATE OR REPLACE PROCEDURE SKYFLOW_TOKENIZE_TABLE(
    vault_name VARCHAR,
    table_name VARCHAR,
    primary_key VARCHAR,
    pii_fields_delimited STRING,
    vault_owner_email VARCHAR)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.8
HANDLER = 'SKYFLOW_TOKENIZE_TABLE'
EXTERNAL_ACCESS_INTEGRATIONS = (SKYFLOW_EXTERNAL_ACCESS_INTEGRATION)
PACKAGES = ('snowflake-snowpark-python', 'pyjwt', 'cryptography', 'requests', 'simplejson')
SECRETS = ('cred' = SKYFLOW_VAULT_SECRET)
AS
$$
import _snowflake
import simplejson as json
import jwt
import requests 
import time
import logging
import re
from urllib.parse import quote_plus
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, row_number
from snowflake.snowpark.window import Window
from datetime import datetime

# Initialize logging
logger = logging.getLogger("python_logger")
logger.setLevel(logging.INFO)
logger.info("Logging from SKYFLOW_TOKENIZE_TABLE Python module.")

def GET_CACHED_AUTH_TOKEN(snowflake_session, key):
    try:
        result = snowflake_session.sql(f"SELECT value, expiry FROM skyflow_cache WHERE key = '{key}'").collect()
        if result:
            value, expiry = result[0]
            if expiry is None or expiry.timestamp() > time.time():  # Convert expiry to float for comparison
                return value
        return None
    except Exception as e:
        logger.error(f"Error retrieving cached value for key '{key}': {e}")
        raise

def STORE_AUTH_TOKEN(snowflake_session, key, value, expiry=None):
    try:
        if expiry:
            # Convert expiry to ISO 8601 format for TIMESTAMP_LTZ
            expiry_iso = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(expiry))
            snowflake_session.sql(f"""
                MERGE INTO skyflow_cache AS target
                USING (SELECT '{key}' AS key, '{value}' AS value, '{expiry_iso}' AS expiry) AS source
                ON target.key = source.key
                WHEN MATCHED THEN UPDATE SET value = source.value, expiry = source.expiry
                WHEN NOT MATCHED THEN INSERT (key, value, expiry) VALUES (source.key, source.value, source.expiry)
            """).collect()
        else:
            snowflake_session.sql(f"""
                MERGE INTO skyflow_cache AS target
                USING (SELECT '{key}' AS key, '{value}' AS value) AS source
                ON target.key = source.key
                WHEN MATCHED THEN UPDATE SET value = source.value
                WHEN NOT MATCHED THEN INSERT (key, value) VALUES (source.key, source.value)
            """).collect()
        logger.info(f"Cached value for key '{key}' updated successfully.")
    except Exception as e:
        logger.error(f"Error updating cached value for key '{key}': {e}")
        raise

def GENERATE_AUTH_TOKEN(snowflake_session, req_session):
    auth_token = GET_CACHED_AUTH_TOKEN(snowflake_session, 'auth_token')
    
    # If a valid token is in the cache, return it
    if auth_token:
        logger.info("Using cached auth_token.")
        return auth_token

    # Otherwise, generate a new one
    credentials = json.loads(_snowflake.get_generic_secret_string('cred'), strict=False)
    claims = {
        "iss": credentials["clientID"],
        "key": credentials["keyID"], 
        "aud": credentials["tokenURI"], 
        "exp": int(time.time()) + 3600,  # JWT expires in Now + 60 minutes
        "sub": credentials["clientID"], 
    }
    signedJWT = jwt.encode(claims, credentials["privateKey"], algorithm='RS256')
    body = {
       'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
       'assertion': signedJWT,
    }
    tokenURI = credentials["tokenURI"]

    try:
        response = req_session.post(tokenURI, json=body)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to generate auth token: {e}")
        raise Exception(f"Failed to generate auth token: {e}")

    auth = json.loads(response.text)
    auth_token = auth["accessToken"]
    expiry = time.time() + 3600  # Cache expiry in 1 hour

    # Update the cache with the new auth_token
    STORE_AUTH_TOKEN(snowflake_session, 'auth_token', auth_token, expiry)
    
    logger.info("Generated and cached new auth_token.")
    return auth_token
    
def SKYFLOW_CREATE_VAULT(snowflake_session, auth_token, req_session, vault_name, table_name, primary_key, pii_fields_delimited, vault_owner_email):
    # Split the comma-separated fields into a list
    pii_fields = pii_fields_delimited.split(',')
    
    # Define a dictionary mapping field names to their configurations
    field_configurations = {
        'NAME': {
            "datatype": "DT_STRING",
            "tags": [
                {"name": "skyflow.options.default_dlp_policy", "values": ["MASK"]},
                {"name": "skyflow.options.find_pattern", "values": ["(.).*(.{2})"]},
                {"name": "skyflow.options.replace_pattern", "values": ["${1}***${2}"]},
                {"name": "skyflow.options.identifiability", "values": ["MODERATE_IDENTIFIABILITY"]},
                {"name": "skyflow.options.operation", "values": ["EXACT_MATCH"]},
                {"name": "skyflow.options.default_token_policy", "values": ["DETERMINISTIC_UUID"]},
                {"name": "skyflow.options.configuration_tags", "values": ["NULLABLE"]},
                {"name": "skyflow.options.personal_information_type", "values": ["PII", "PHI"]},
                {"name": "skyflow.options.privacy_law", "values": ["GDPR", "CCPA", "HIPAA"]},
                {"name": "skyflow.options.description", "values": ["An individual's first, middle, or last name"]},
                {"name": "skyflow.options.display_name", "values": ["name"]}
            ]
        },
        'EMAIL': {
            "datatype": "DT_STRING",
            "tags": [
                {"name": "skyflow.options.default_dlp_policy", "values": ["MASK"]},
                {"name": "skyflow.options.find_pattern", "values": ["^(.).*?(.)?@(.+)$"]},
                {"name": "skyflow.options.replace_pattern", "values": ["$1******$2@$3"]},
                {"name": "skyflow.options.identifiability", "values": ["HIGH_IDENTIFIABILITY"]},
                {"name": "skyflow.options.operation", "values": ["EXACT_MATCH"]},
                {"name": "skyflow.options.default_token_policy", "values": ["DETERMINISTIC_FPT"]},
                {"name": "skyflow.options.format_preserving_regex", "values": ["^([a-z]{20})@([a-z]{10})\\.com$"]},
                {"name": "skyflow.options.personal_information_type", "values": ["PII", "PHI"]},
                {"name": "skyflow.options.privacy_law", "values": ["GDPR", "CCPA", "HIPAA"]},
                {"name": "skyflow.options.data_type", "values": ["skyflow.Email"]},
                {"name": "skyflow.options.description", "values": ["An email address"]},
                {"name": "skyflow.options.display_name", "values": ["email"]}
            ]
                        },
        'PHONE': {
            "datatype": "DT_STRING",
                            "tags": [
                {"name": "skyflow.options.default_dlp_policy", "values": ["MASK"]},
                {"name": "skyflow.options.find_pattern", "values": [".*([0-9]{4})"]},
                {"name": "skyflow.options.replace_pattern", "values": ["XXXXXX${1}"]},
                {"name": "skyflow.options.identifiability", "values": ["HIGH_IDENTIFIABILITY"]},
                {"name": "skyflow.options.operation", "values": ["EXACT_MATCH"]},
                {"name": "skyflow.options.default_token_policy", "values": ["DETERMINISTIC_FPT"]},
                {"name": "skyflow.options.configuration_tags", "values": ["NULLABLE"]},
                {"name": "skyflow.options.personal_information_type", "values": ["PII", "PHI"]},
                {"name": "skyflow.options.privacy_law", "values": ["GDPR", "CCPA", "HIPAA"]},
                {"name": "skyflow.options.description", "values": ["Details about a phone number"]},
                {"name": "skyflow.options.display_name", "values": ["phone"]}
            ]
        },
        'ADDRESS': {
            "datatype": "DT_STRING",
            "tags": [
                {"name": "skyflow.options.default_dlp_policy", "values": ["MASK"]},
                {"name": "skyflow.options.find_pattern", "values": ["(.).*(.{2})"]},
                {"name": "skyflow.options.replace_pattern", "values": ["${1}***${2}"]},
                {"name": "skyflow.options.identifiability", "values": ["HIGH_IDENTIFIABILITY"]},
                {"name": "skyflow.options.operation", "values": ["EXACT_MATCH"]},
                {"name": "skyflow.options.default_token_policy", "values": ["DETERMINISTIC_UUID"]},
                {"name": "skyflow.options.configuration_tags", "values": ["NULLABLE"]},
                {"name": "skyflow.options.personal_information_type", "values": ["PII", "PHI"]},
                {"name": "skyflow.options.privacy_law", "values": ["GDPR", "CCPA", "HIPAA"]},
                {"name": "skyflow.options.description", "values": ["A generic street address usually contains the house number and street name"]},
                {"name": "skyflow.options.display_name", "values": ["address"]}
            ]
        }
        # Add configurations for other fields as needed
    }
    
    # Initialize a list with skyflow_id and primary_key to start
    field_blocks = [
        {
            "name": "skyflow_id",
            "datatype": "DT_STRING",
            "isArray": False,
                            "tags": [
                {"name": "skyflow.options.default_dlp_policy", "values": ["PLAIN_TEXT"]},
                {"name": "skyflow.options.operation", "values": ["ALL_OP"]},
                {"name": "skyflow.options.sensitivity", "values": ["LOW"]},
                {"name": "skyflow.options.data_type", "values": ["skyflow.SkyflowID"]},
                {"name": "skyflow.options.description", "values": ["Skyflow defined Primary Key"]},
                {"name": "skyflow.options.display_name", "values": ["Skyflow ID"]}
                            ],
                            "index": 0
        },
        {
            "name": primary_key.lower(),
            "datatype": "DT_INT32",
            "isArray": False,
            "tags": [
                {"name": "skyflow.options.default_dlp_policy", "values": ["PLAIN_TEXT"]},
                {"name": "skyflow.options.operation", "values": ["ALL_OP"]},
                {"name": "skyflow.options.unique", "values": ["true"]},
                {"name": "skyflow.options.display_name", "values": [primary_key.lower()]}
            ],
            "index": 0
        }
    ]
    
    # Loop over each field in the pii_fields list and create a field block
    for field in pii_fields:
        field = field.strip()  # Remove any leading/trailing whitespace
        field_upper = field.upper()
        field_config = field_configurations.get(field_upper)
        
        if field_config:
            field_block = {
                "name": field.lower(),
                "datatype": field_config["datatype"],
                "isArray": False,
                "tags": field_config["tags"],
                "index": 0
            }
            field_blocks.append(field_block)
        else:
            print(f"No specific configuration found for field '{field}'. Skipping.")
            pass  # Skip fields without specific configurations
    
    # Build the body for the API request
    body = {
        "name": vault_name,
        "description": "A vault for Snowflake PII",
        "vaultSchema": {
            "schemas": [
                {
                    "name": table_name,
                    "fields": field_blocks,
                    "childrenSchemas": [],
                    "schemaTags": []
                }
            ],
            "tags": [
                {"name": "skyflow.options.experimental", "values": ["true"]},
                {"name": "skyflow.options.vault_main_object", "values": ["Quickstart"]},
                {"name": "skyflow.options.query_interface", "values": ["REST", "SQL"]},
                {"name": "skyflow.options.env_name", "values": ["ALL_ENV"]},
                {"name": "skyflow.options.display_name", "values": ["Quickstart"]}
            ]
        },
        "workspaceID": "<TODO: WORKSPACE_ID>",
        "owners": [
            {
                "ID": GET_USER_ID_BY_EMAIL(snowflake_session, auth_token, req_session, vault_owner_email),
                "type": "USER"
            },
            {
                "ID": "<TODO: SERVICE_ACCOUNT_ID>",
                "type": "SERVICE_ACCOUNT"
            }
        ]
    }
    
    url = "https://manage.skyflowapis.com/v1/vaults"
    headers = {
        "Authorization": "Bearer " + auth_token,
        "X-SKYFLOW-ACCOUNT-ID": "<TODO: ACCOUNT_ID>"
    }
    
    try:
        response = req_session.post(url, json=body, headers=headers)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to create vault: {e}")
        raise Exception(f"Failed to create vault: {e}")
    
    vault_response = json.loads(response.text)
    
    return vault_response["ID"]

def SKYFLOW_TOKENIZE_TABLE(snowflake_session, vault_name, table_name, primary_key, pii_fields_delimited, vault_owner_email):
    import re

    # Initialize a separate requests session
    req_session = requests.Session()
    
    # Generate or retrieve the cached credentials
    auth_token = GENERATE_AUTH_TOKEN(snowflake_session, req_session)

    # Vault ID can either be passed directly or needs to be fetched by name
    if re.match(r"^[a-z0-9]{32}$", vault_name):
        vault_id = vault_name
    else:
        vault_id = SKYFLOW_CREATE_VAULT(
            auth_token, vault_name, table_name, primary_key, pii_fields_delimited, vault_owner_email
        )

    # Convert the comma-separated list of PII fields into a list and lowercase
    pii_columns = [field.strip().lower() for field in pii_fields_delimited.split(",")]
    primary_key_lower = primary_key.lower()
    primary_key_upper = primary_key.upper()
    pii_columns.append(primary_key_lower)
    
    # Fetch data from the Snowflake table and add a row number
    df = snowflake_session.table(table_name).select([col(c.upper()) for c in pii_columns])

    # Add a row number to the DataFrame
    window_spec = Window.order_by(col(primary_key_upper))
    df = df.with_column('ROW_NUM', row_number().over(window_spec))

    # Calculate total records and batches
    total_records = df.count()
    batch_size = 25
    total_batches = (total_records + batch_size - 1) // batch_size

    # Initialize a list to store mappings
    mapping_data = []

    # Process batches sequentially
    for batch_num in range(total_batches):
        try:
            lower_bound = batch_num * batch_size + 1
            upper_bound = min((batch_num + 1) * batch_size, total_records)

            # Fetch batch data without collecting all data
            batch_df = df.filter((col('ROW_NUM') >= lower_bound) & (col('ROW_NUM') <= upper_bound))

            batch_records = batch_df.collect()

            if not batch_records:
                continue

            records = []
            for row in batch_records:
                record = {
                        "fields": {column.lower(): row[column.upper()] for column in pii_columns}
                }
                records.append(record)
            
            body = {
                "records": records,
                "tokenization": True
            }

                # Make the API call
                skyflow_url = f"https://ebfc9bee4242.vault.skyflowapis.com/v1/vaults/{vault_id}/{table_name.lower()}"
                headers = {"Authorization": "Bearer " + auth_token}
            
                try:
                    response = req_session.post(skyflow_url, json=body, headers=headers)
                    response.raise_for_status()
                    response_as_json = response.json()
                except requests.exceptions.RequestException as e:
                    logger.error(f"Tokenization request failed: {e}")
                    raise Exception(f"Tokenization request failed: {e}")
                except json.JSONDecodeError as e:
                    logger.error(f"JSON decoding failed: {e}, Response Text: {response.text}")
                    raise Exception(f"JSON decoding failed: {e}")
            
            if "records" in response_as_json:
                batch_mapping_data = []
                for original_record, tokenized_record in zip(records, response_as_json["records"]):
                    primary_key_value = original_record["fields"][primary_key_lower]
                    for field, token in tokenized_record.get("tokens", {}).items():
                        batch_mapping_data.append((primary_key_value, field, token))
                    
                # Append to the mapping_data list
                mapping_data.extend(batch_mapping_data)
            else:
                error_msg = f"Key 'records' not found in the response for batch {batch_num}. Response: {response_as_json}"
                raise Exception(error_msg)
        except Exception as e:
            logger.error(f"Error processing batch {batch_num}: {e}")
            raise  # Re-raise the exception to halt execution

    if not mapping_data:
        return "No data to update."

    # Create a mapping table to hold the mapping
    mapping_schema = [primary_key_lower, "FIELD", "TOKEN"]
    mapping_df = snowflake_session.create_dataframe(mapping_data, schema=mapping_schema)
    mapping_table_name = f"TOKEN_MAPPING_{table_name}_{int(time.time())}"
    mapping_df.write.mode("overwrite").save_as_table(mapping_table_name)

    # Prepare the update statement using JOINs
    update_fields = [f'"{field.upper()}" = m."{field}_token"' for field in pii_columns if field != primary_key_lower]
    set_clause = ", ".join(update_fields)

    select_fields = [f"MAX(CASE WHEN FIELD = '{field}' THEN TOKEN END) AS \"{field}_token\"" for field in pii_columns if field != primary_key_lower]
    select_clause = ", ".join(select_fields)

    # Perform the update using a JOIN
    try:
    snowflake_session.sql(
        f'''
        UPDATE "{table_name}" AS t
        SET {set_clause}
        FROM (
            SELECT "{primary_key_upper}", {select_clause}
            FROM "{mapping_table_name}"
            GROUP BY "{primary_key_upper}"
        ) AS m
        WHERE t."{primary_key_upper}" = m."{primary_key_upper}"
        '''
    ).collect()
    except Exception as e:
        logger.error(f"Failed to execute UPDATE statement: {e}")
        raise Exception(f"Failed to execute UPDATE statement: {e}")

    # Drop the mapping table
    try:
    snowflake_session.sql(f'DROP TABLE IF EXISTS "{mapping_table_name}"').collect()
    except Exception as e:
        logger.error(f"Failed to drop mapping table {mapping_table_name}: {e}")

    return f"Tokenization for table {table_name} completed successfully!"
$$;


-- Step 7: Create a function to detokenize data for use in snowflake queries
CREATE OR REPLACE FUNCTION SKYFLOW_DETOKENIZE(token VARCHAR)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.8
HANDLER = 'SKYFLOW_DETOKENIZE'
EXTERNAL_ACCESS_INTEGRATIONS = (SKYFLOW_EXTERNAL_ACCESS_INTEGRATION)
PACKAGES = ('pandas', 'pyjwt', 'cryptography', 'requests', 'simplejson')
SECRETS = ('cred' = SKYFLOW_VAULT_SECRET)
AS
$$
import pandas
import _snowflake
import simplejson as json
import jwt
import requests 
import time
from _snowflake import vectorized
import logging

# Initialize a session object at the global scope
session = requests.Session()

logger = logging.getLogger("python_logger")
logger.setLevel(logging.INFO)
logger.info("Logging from SKYFLOW_DETOKENIZE Python module.")

def GENERATE_AUTH_TOKEN():
    # Generate a new token
    credentials = json.loads(_snowflake.get_generic_secret_string('cred'), strict=False)
    claims = {
       "iss": credentials["clientID"],
       "key": credentials["keyID"], 
       "aud": credentials["tokenURI"], 
       "exp": int(time.time()) + (3600),  # JWT expires in Now + 60 minutes
       "sub": credentials["clientID"], 
    }
    signedJWT = jwt.encode(claims, credentials["privateKey"], algorithm='RS256')
    body = {
       'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
       'assertion': signedJWT,
    }
    tokenURI = credentials["tokenURI"]
    
    # Use the session to send the request
    r = session.post(tokenURI, json=body)
    auth = json.loads(r.text)
    
    return auth["accessToken"]

@vectorized(input=pandas.DataFrame, max_batch_size=25)
def SKYFLOW_DETOKENIZE(token_df):
    auth_token = GENERATE_AUTH_TOKEN()
    vault_id = "<TODO: VAULT_ID>"

    # Convert the DataFrame Series into the token format needed for the detokenize call.
    token_values = token_df[0].apply(lambda x: {'token': x, 'redaction': 'PLAIN_TEXT'}).tolist()
    body = {
        'detokenizationParameters': token_values
    }

    url = f"https://ebfc9bee4242.vault.skyflowapis.com/v1/vaults/{vault_id}/detokenize"
    headers = { 'Authorization': 'Bearer ' + auth_token }

    # Use the session to send the request
    response = session.post(url, json=body, headers=headers)
    
    # Check if the request was successful
    if response.status_code != 200:
        logger.error(f"Detokenization request failed with status code {response.status_code}")
        return pandas.Series([None] * len(token_df))  # Return a series of None values

    response_as_json = response.json()

    # Check if 'records' key exists in the response
    if 'records' not in response_as_json:
        logger.error("'records' key not found in the response.")
        return pandas.Series([None] * len(token_df))  # Return a series of None values

    # Convert the JSON response into a DataFrame Series.
    data = [record['value'] for record in response_as_json['records']]

    return pandas.Series(data)

$$;


-- Step 7: Create a function to detokenize data for use in snowflake queries
CREATE OR REPLACE FUNCTION SKYFLOW_DETOKENIZE(token VARCHAR, redaction_level STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.8
HANDLER = 'SKYFLOW_DETOKENIZE'
EXTERNAL_ACCESS_INTEGRATIONS = (SKYFLOW_EXTERNAL_ACCESS_INTEGRATION)
PACKAGES = ('pandas', 'pyjwt', 'cryptography', 'requests', 'simplejson')
SECRETS = ('cred' = SKYFLOW_VAULT_SECRET)
AS
$$
import pandas
import simplejson as json
import jwt
import requests 
import time
import logging
import _snowflake  # Import the _snowflake module
from _snowflake import vectorized

# Initialize a requests session object at the global scope
http_session = requests.Session()

logger = logging.getLogger("python_logger")
logger.setLevel(logging.INFO)
logger.info("Logging from SKYFLOW_DETOKENIZE Python module.")

def GENERATE_AUTH_TOKEN():
    credentials = json.loads(_snowflake.get_generic_secret_string('cred'), strict=False)
    claims = {
       "iss": credentials["clientID"],
       "key": credentials["keyID"], 
       "aud": credentials["tokenURI"], 
        "exp": int(time.time()) + 3600,
       "sub": credentials["clientID"], 
    }
    signedJWT = jwt.encode(claims, credentials["privateKey"], algorithm='RS256')
    body = {
       'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
       'assertion': signedJWT,
    }
    tokenURI = credentials["tokenURI"]
    
    try:
        response = http_session.post(tokenURI, json=body)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to generate auth token: {e}")
        raise Exception(f"Failed to generate auth token: {e}")

    auth = json.loads(response.text)
    return auth["accessToken"]

def GET_VAULT_ID_BY_NAME(auth_token, vault_name):
    workspace_id = "<TODO: WORKSPACE_ID>"
    url = f"https://manage.skyflowapis.com/v1/vaults?filterOps.name={vault_name}&workspaceID={workspace_id}"
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "X-SKYFLOW-ACCOUNT-ID": "<TODO: ACCOUNT_ID>"
    }

    try:
        response = http_session.get(url, headers=headers)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to get vault ID: {e}")
        raise Exception(f"Failed to get vault ID: {e}")

    try:
        vault_response = json.loads(response.text)
        vault_id = vault_response["vaults"][0]["ID"]
        logger.info(f"Retrieved vault_id: {vault_id}")
        return vault_id
    except (KeyError, IndexError, json.JSONDecodeError) as e:
        logger.error(f"Error parsing vault response: {e}, Response Text: {response.text}")
        raise Exception(f"Error parsing vault response: {e}")

@vectorized(input=pandas.DataFrame, max_batch_size=10000)
def SKYFLOW_DETOKENIZE(token_df):
    auth_token = GENERATE_AUTH_TOKEN()
    vault_id = GET_VAULT_ID_BY_NAME(auth_token, 'SkyflowVault')

    tokens_list = token_df.iloc[:, 0].tolist()
    redaction_levels = token_df.iloc[:, 1].tolist()

    unique_tokens = {}
    for idx, (token, redaction) in enumerate(zip(tokens_list, redaction_levels)):
        if token not in unique_tokens:
            unique_tokens[token] = {'indices': [idx], 'redaction': redaction}
        else:
            unique_tokens[token]['indices'].append(idx)

    detokenized_results = {}
    batch_size = 25
    tokens_items = list(unique_tokens.items())
    for i in range(0, len(tokens_items), batch_size):
        batch = tokens_items[i:i+batch_size]
        detokenization_parameters = [{'token': token, 'redaction': info['redaction']} for token, info in batch]
    
        body = {
            'detokenizationParameters': detokenization_parameters
        }

        url = f"https://ebfc9bee4242.vault.skyflowapis.com/v1/vaults/{vault_id}/detokenize"
        headers = {'Authorization': f'Bearer {auth_token}'}

        try:
            response = http_session.post(url, json=body, headers=headers)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(f"Detokenization request failed: {e}")
            continue

        try:
            response_as_json = response.json()
        except json.JSONDecodeError as e:
            logger.error(f"JSON decoding failed: {e}, Response Text: {response.text}")
            continue

        if 'records' not in response_as_json:
            logger.error("'records' key not found in the response.")
            continue

        for record in response_as_json['records']:
            detokenized_results[record['token']] = record['value']

    result_series = [None] * len(tokens_list)
    for token, info in unique_tokens.items():
        detokenized_value = detokenized_results.get(token, None)
        for idx in info['indices']:
            result_series[idx] = detokenized_value

    return pandas.Series(result_series)

$$;


-- Step 8: Create a function for processing PII updates from a snowflake stream
CREATE OR REPLACE PROCEDURE SKYFLOW_PROCESS_PII(vault_id VARCHAR, table_name VARCHAR, primary_key VARCHAR, pii_fields STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.8
HANDLER = 'SKYFLOW_PROCESS_PII'
EXTERNAL_ACCESS_INTEGRATIONS = (SKYFLOW_EXTERNAL_ACCESS_INTEGRATION)
PACKAGES = ('snowflake-snowpark-python', 'pyjwt', 'cryptography', 'requests', 'simplejson')
SECRETS = ('cred' = SKYFLOW_VAULT_SECRET)
AS
$$
import _snowflake
import simplejson as json
import jwt
import requests 
import logging
import time

# Initialize a session object at the global scope
session = requests.Session()

logger = logging.getLogger("python_logger")
logger.setLevel(logging.INFO)
logger.info("Logging from SKYFLOW_PROCESS_PII Python module.")

def GET_CACHED_AUTH_TOKEN(snowflake_session):
    # Query the SKYFLOW_CACHE table for an existing token
    result = snowflake_session.sql("SELECT token, expiry FROM SKYFLOW_CACHE WHERE token_type = 'AUTH'").collect()

    if result and len(result) > 0:
        token = result[0]['TOKEN']
        expiry = result[0]['EXPIRY']
        
        # Check if the token is still valid (expiry > current time)
        if expiry > time.time():
            return token
    return None

def STORE_AUTH_TOKEN(snowflake_session, token, expiry):
    # Insert or update the token in the SKYFLOW_CACHE table
    snowflake_session.sql(f"""
        MERGE INTO SKYFLOW_CACHE AS target
        USING (SELECT 'AUTH' AS token_type, '{token}' AS token, {expiry} AS expiry) AS source
        ON target.token_type = source.token_type
        WHEN MATCHED THEN
            UPDATE SET target.token = source.token, target.expiry = source.expiry
        WHEN NOT MATCHED THEN
            INSERT (token_type, token, expiry) VALUES (source.token_type, source.token, source.expiry)
    """).collect()

def GENERATE_AUTH_TOKEN(snowflake_session):
    # Try to get a cached token
    cached_token = GET_CACHED_AUTH_TOKEN(snowflake_session)
    if cached_token:
        return cached_token

    # Generate a new token if no valid cached token was found
    credentials = json.loads(_snowflake.get_generic_secret_string('cred'), strict=False)
    claims = {
       "iss": credentials["clientID"],
       "key": credentials["keyID"], 
       "aud": credentials["tokenURI"], 
       "exp": int(time.time()) + (3600),  # JWT expires in Now + 60 minutes
       "sub": credentials["clientID"], 
    }
    signedJWT = jwt.encode(claims, credentials["privateKey"], algorithm='RS256')
    body = {
       'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
       'assertion': signedJWT,
    }
    tokenURI = credentials["tokenURI"]

    # Use the session to send the request
    r = session.post(tokenURI, json=body)
    auth = json.loads(r.text)

    # Store the new token and its expiry time in the cache table
    auth_token = auth["accessToken"]
    expiry_time = time.time() + 3600  # 1 hour from now
    STORE_AUTH_TOKEN(snowflake_session, auth_token, expiry_time)
    
    return auth_token

def SKYFLOW_PROCESS_PII(snowflake_session, vault_id, table_name, primary_key, pii_fields):
    # Load credentials and generate (or retrieve) auth token
    auth_token = GENERATE_AUTH_TOKEN(snowflake_session)

    # Convert primary_key and pii_fields to uppercase
    primary_key = primary_key.upper()
    pii_fields_list = [field.strip().upper() for field in pii_fields.split(',')]
    pii_fields_list.append(primary_key)

    # Skyflow static variables
    table_name_skyflow = table_name.lower()
    skyflow_account_id = "<TODO: ACCOUNT_ID>"
    skyflow_url_vault = f"https://ebfc9bee4242.vault.skyflowapis.com/v1/vaults/{vault_id}/"

    # Retrieve all stream records
    stream_records = snowflake_session.sql(f"SELECT {primary_key}, METADATA$ACTION, {', '.join(pii_fields_list)} FROM SKYFLOW_PII_STREAM_{table_name}").collect()

    # Initialize lists for different actions
    primary_keys_to_delete = []
    records_to_insert = []

    # Iterate through records to determine the action
    for record in stream_records:
        action = record['METADATA$ACTION']
        primary_key_value = record[primary_key]
        if action == 'DELETE':
            primary_keys_to_delete.append(primary_key_value)
        elif action == 'INSERT':
            records_to_insert.append(record)

    # Process DELETE actions
    if primary_keys_to_delete:
        # Make a single GET request to obtain all skyflow_ids
        response = requests.get(
            skyflow_url_vault + table_name_skyflow,
            headers={
                'Accept': 'application/json',
                'X-SKYFLOW-ACCOUNT-ID': skyflow_account_id,
                'Authorization': f'Bearer {auth_token}'
            },
            params={
                'column_name': primary_key.lower(),
                'column_values': primary_keys_to_delete,
                'fields': 'skyflow_id',
                'redaction': 'PLAIN_TEXT'
            }
        )
        response.raise_for_status()
        skyflow_ids_to_delete = [record['fields']['skyflow_id'] for record in response.json()['records']]

        # Make a single DELETE request to Skyflow
        delete_response = requests.delete(
            skyflow_url_vault + table_name_skyflow,
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'X-SKYFLOW-ACCOUNT-ID': skyflow_account_id,
                'Authorization': f'Bearer {auth_token}'
            },
            json={
                'skyflow_ids': skyflow_ids_to_delete
            }
        )
        delete_response.raise_for_status()

    # Process INSERT actions
    if records_to_insert: 
        batch_size = 25
        # Split records into batches of 25
        batches = [records_to_insert[i:i + batch_size] for i in range(0, len(records_to_insert), batch_size)]

        # Initialize dictionaries to store CASE expressions for each field
        case_expressions = {column: [] for column in pii_fields_list}
        update_ids = []

        # Define a function to execute the update statement
        def execute_update(sql_command, update_ids_subset):
            # Construct the final UPDATE statement with CASE expressions for each field
            sql_command += f" WHERE {primary_key} IN ({', '.join(update_ids_subset)})"
            # Execute the UPDATE statement
            snowflake_session.sql(sql_command).collect()

        for batch_index, batch in enumerate(batches):
            records = []
            for row_index, row in enumerate(batch):
                # Construct the record with the specific fields
                record = {
                    "fields": {
                        column: row[column.upper()] if column.upper() in row else None for column in pii_fields_list
                    }
                }
                records.append(record)
                # Add the plaintext primary_key to the list for WHERE clause matching
                update_ids.append(str(row[primary_key]))
            
            body = {
                "records": records,
                "tokenization": True
            }

            url = skyflow_url_vault + table_name_skyflow
            headers = {
                "Authorization": "Bearer " + auth_token
            }
            
            response = requests.post(url, json=body, headers=headers)
            response_as_json = response.json()
            
            # Check if 'records' key exists in the response
            if "records" in response_as_json:
                # Construct the CASE expressions for each field using update_ids
                for i, record in enumerate(response_as_json["records"]):
                    # Use the index to find the corresponding primary_key from update_ids
                    id_index = batch_index * batch_size + i
                    primary_key_value = update_ids[id_index]
                    for field, token in record["tokens"].items():
                        field = field.upper()
                        case_expression = f"WHEN '{primary_key_value}' THEN '{token}'"
                        case_expressions[field].append(case_expression)
                        
                        # Check if the limit is reached, then execute the update
                        if len(case_expressions[field]) >= 10000:  # Set to one less than the limit to account for the current expression
                            sql_command = f"UPDATE {table_name} SET {field} = CASE {primary_key} {' '.join(case_expressions[field])} END"
                            execute_update(sql_command, update_ids[:10000])
                            # Reset the expressions and primary_key values for the next batch
                            case_expressions[field] = case_expressions[field][10000:]
                            update_ids = update_ids[10000:]
            else:
                print("Key 'records' not found in the response.")
                return "Key 'records' not found in the response."

        # Execute any remaining updates
        for field in case_expressions:
            if case_expressions[field]:
                sql_command = f"UPDATE {table_name} SET {field} = CASE {primary_key} {' '.join(case_expressions[field])} END"
                execute_update(sql_command, update_ids)

    # Refresh the stream for future processing
    snowflake_session.sql(f'CREATE OR REPLACE STREAM SKYFLOW_PII_STREAM_{table_name} ON TABLE SKYFLOW_DEMO.PUBLIC.{table_name}').collect()
    return "Changes processed"

$$;


-- Create roles
CREATE ROLE ROLE_FULL_PII;
CREATE ROLE ROLE_NO_PII;
CREATE ROLE ROLE_PARTIAL_PII;

-- Grant usage on database SKYFLOW_DEMO to roles
GRANT USAGE ON DATABASE SKYFLOW_DEMO TO ROLE ROLE_FULL_PII;
GRANT USAGE ON DATABASE SKYFLOW_DEMO TO ROLE ROLE_NO_PII;
GRANT USAGE ON DATABASE SKYFLOW_DEMO TO ROLE ROLE_PARTIAL_PII;

-- Grant usage on schema SKYFLOW_DEMO.PUBLIC to roles
GRANT USAGE ON SCHEMA SKYFLOW_DEMO.PUBLIC TO ROLE ROLE_FULL_PII;
GRANT USAGE ON SCHEMA SKYFLOW_DEMO.PUBLIC TO ROLE ROLE_NO_PII;
GRANT USAGE ON SCHEMA SKYFLOW_DEMO.PUBLIC TO ROLE ROLE_PARTIAL_PII;

-- Grant access to table SKYFLOW_DEMO.PUBLIC.CUSTOMERS to roles
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SKYFLOW_DEMO.PUBLIC.CUSTOMERS TO ROLE ROLE_FULL_PII;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SKYFLOW_DEMO.PUBLIC.CUSTOMERS TO ROLE ROLE_NO_PII;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SKYFLOW_DEMO.PUBLIC.CUSTOMERS TO ROLE ROLE_PARTIAL_PII;

CREATE OR REPLACE MASKING POLICY DETOKENIZE_COLUMN AS (VAL STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('ROLE_FULL_PII') THEN SKYFLOW_DETOKENIZE(VAL, 'PLAIN_TEXT')
    WHEN CURRENT_ROLE() IN ('ROLE_PARTIAL_PII') THEN SKYFLOW_DETOKENIZE(VAL, 'MASKED')
    WHEN CURRENT_ROLE() IN ('ROLE_NO_PII') THEN SKYFLOW_DETOKENIZE(VAL, 'REDACTED')
    ELSE VAL
  END;

ALTER TABLE IF EXISTS CUSTOMERS MODIFY COLUMN NAME SET MASKING POLICY DETOKENIZE_COLUMN;
ALTER TABLE IF EXISTS CUSTOMERS MODIFY COLUMN EMAIL SET MASKING POLICY DETOKENIZE_COLUMN;
ALTER TABLE IF EXISTS CUSTOMERS MODIFY COLUMN PHONE SET MASKING POLICY DETOKENIZE_COLUMN;
ALTER TABLE IF EXISTS CUSTOMERS MODIFY COLUMN ADDRESS SET MASKING POLICY DETOKENIZE_COLUMN;

CREATE OR REPLACE PROCEDURE GRANT_WAREHOUSE_ACCESS(role_name STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.8
HANDLER = 'GRANT_WAREHOUSE_ACCESS'
PACKAGES = ('snowflake-snowpark-python', 'pyjwt', 'cryptography', 'requests', 'simplejson')
EXECUTE AS CALLER
AS
$$
def GRANT_WAREHOUSE_ACCESS(session, role_name):
    # Query to get the current active warehouse
    current_warehouse_result = session.sql("SELECT CURRENT_WAREHOUSE()").collect()
    
    # Fetch the result from the query
    current_warehouse = current_warehouse_result[0][0] if current_warehouse_result else None
    
    if not current_warehouse:
        raise Exception("No current warehouse set!")
    
    # Prepare the dynamic grant statement
    grant_query = f"GRANT USAGE ON WAREHOUSE {current_warehouse} TO ROLE {role_name}"
    
    # Execute the grant query
    session.sql(grant_query).collect()
    
    # Return a success message
    return f"Granted usage on warehouse {current_warehouse} to the role {role_name}."
$$;


CALL GRANT_WAREHOUSE_ACCESS('ROLE_FULL_PII');
CALL GRANT_WAREHOUSE_ACCESS('ROLE_NO_PII');
CALL GRANT_WAREHOUSE_ACCESS('ROLE_PARTIAL_PII');

CREATE OR REPLACE PROCEDURE GRANT_ROLE_TO_CURRENT_USER(role_name STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.8
HANDLER = 'GRANT_ROLE_TO_CURRENT_USER'
PACKAGES = ('snowflake-snowpark-python', 'pyjwt', 'cryptography', 'requests', 'simplejson')
EXECUTE AS CALLER
AS
$$
def GRANT_ROLE_TO_CURRENT_USER(session, role_name):
    # Get the current user
    current_user_query = "SELECT CURRENT_USER()"
    current_user_result = session.sql(current_user_query).collect()
    current_user = current_user_result[0][0]

    # Prepare the grant role query
    grant_role_query = f"GRANT ROLE {role_name} TO USER {current_user}"
    
    # Execute the grant role query
    session.sql(grant_role_query).collect()
    
    # Return success message
    return f"Granted role {role_name} to user {current_user}."
$$;

CALL GRANT_ROLE_TO_CURRENT_USER('ROLE_FULL_PII');
CALL GRANT_ROLE_TO_CURRENT_USER('ROLE_NO_PII');
CALL GRANT_ROLE_TO_CURRENT_USER('ROLE_PARTIAL_PII');