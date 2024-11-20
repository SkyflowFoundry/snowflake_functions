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

CREATE OR REPLACE TABLE SKYFLOW_DEMO.PUBLIC.SUPPLIERS (
    SUPPLIER_ID NUMBER(38,0) IDENTITY(1,1),  -- Auto-incrementing column
    SUPPLIER_NAME VARCHAR(16777216),
    CONTACT_PERSON VARCHAR(16777216),
    CONTACT_PHONE VARCHAR(16777216),
    CONTACT_EMAIL VARCHAR(16777216),
    CONTACT_ADDRESS VARCHAR(16777216),
    TOTAL_ORDERS NUMBER(10, 0)
);

INSERT INTO SKYFLOW_DEMO.PUBLIC.CUSTOMERS (NAME, EMAIL, PHONE, ADDRESS, LIFETIME_PURCHASE_AMOUNT, CUSTOMER_SINCE)
SELECT 
    CONCAT(
        CASE WHEN RANDOM() > 0.5 THEN 'Mr. ' ELSE 'Ms. ' END,
        CASE WHEN RANDOM() < 0.25 THEN 'John ' 
             WHEN RANDOM() < 0.5 THEN 'Robert ' 
             WHEN RANDOM() < 0.75 THEN 'James ' 
             ELSE 'William ' END, 
        INITCAP(SUBSTR(MD5(RANDOM()), 1, 10))
    ) AS NAME,
    LOWER(CONCAT(
        CASE WHEN RANDOM() > 0.5 THEN 'mr.' ELSE 'ms.' END, 
        CASE WHEN RANDOM() < 0.25 THEN 'john.' 
             WHEN RANDOM() < 0.5 THEN 'robert.' 
             WHEN RANDOM() < 0.75 THEN 'james.' 
             ELSE 'william.' END, 
        SUBSTR(MD5(RANDOM()), 1, 10), 
        CASE WHEN RANDOM() > 0.5 THEN '@example.com' ELSE '@company.com' END
    )) AS EMAIL,
    CONCAT(
        '+1 (', UNIFORM(200, 999, RANDOM()), ') 555-',  -- Random 3-digit area code and fixed 555 exchange
        LPAD(TO_VARCHAR(UNIFORM(1000, 9999, RANDOM())), 4, '0')  -- Subscriber number
    ) AS PHONE,
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
    ROUND(UNIFORM(0, 10000, RANDOM())::NUMERIC(10,2), 2) AS LIFETIME_PURCHASE_AMOUNT,
    CONCAT(UNIFORM(2000, 2022, RANDOM()), '-', 
           LPAD(TO_VARCHAR(UNIFORM(1, 12, RANDOM())), 2, '0'), '-', 
           LPAD(TO_VARCHAR(UNIFORM(1, 28, RANDOM())), 2, '0')) AS CUSTOMER_SINCE
FROM TABLE(GENERATOR(ROWCOUNT => 100));

INSERT INTO SKYFLOW_DEMO.PUBLIC.SUPPLIERS (SUPPLIER_NAME, CONTACT_PERSON, CONTACT_PHONE, CONTACT_EMAIL, CONTACT_ADDRESS, TOTAL_ORDERS)
SELECT 
    CONCAT(
        CASE WHEN RANDOM() < 0.25 THEN 'Global ' 
             WHEN RANDOM() < 0.5 THEN 'Elite ' 
             WHEN RANDOM() < 0.75 THEN 'Prime ' 
             ELSE 'NextGen ' END, 
        INITCAP(SUBSTR(MD5(RANDOM()), 1, 8)), ' Inc.'
    ) AS NAME,
    CONCAT(
        CASE WHEN RANDOM() > 0.5 THEN 'Mr. ' ELSE 'Ms. ' END,
        CASE WHEN RANDOM() < 0.25 THEN 'Chris ' 
             WHEN RANDOM() < 0.5 THEN 'Pat ' 
             WHEN RANDOM() < 0.75 THEN 'Taylor ' 
             ELSE 'Jordan ' END, 
        INITCAP(SUBSTR(MD5(RANDOM()), 1, 10))
    ) AS CONTACT_PERSON,
    CONCAT(
        '+1 (', UNIFORM(200, 999, RANDOM()), ') 444-',  -- Random 3-digit area code and fixed 444 exchange
        LPAD(TO_VARCHAR(UNIFORM(1000, 9999, RANDOM())), 4, '0')  -- Subscriber number
    ) AS PHONE,
    LOWER(CONCAT(
        CASE WHEN RANDOM() < 0.25 THEN 'info@' 
             WHEN RANDOM() < 0.5 THEN 'support@' 
             WHEN RANDOM() < 0.75 THEN 'sales@' 
             ELSE 'contact@' END, 
        SUBSTR(MD5(RANDOM()), 1, 10), 
        CASE WHEN RANDOM() > 0.5 THEN '.com' ELSE '.org' END
    )) AS EMAIL,
    CONCAT(
        UNIFORM(100, 999, RANDOM()), ' ', 
        CASE WHEN RANDOM() < 0.25 THEN 'Commerce Blvd' 
             WHEN RANDOM() < 0.5 THEN 'Industrial Ave' 
             WHEN RANDOM() < 0.75 THEN 'Enterprise St' 
             ELSE 'Market Lane' END, ', ', 
        CASE WHEN RANDOM() < 0.25 THEN 'Houston' 
             WHEN RANDOM() < 0.5 THEN 'Phoenix' 
             WHEN RANDOM() < 0.75 THEN 'Dallas' 
             ELSE 'Seattle' END, ' ', 
        SUBSTR(MD5(RANDOM()), 1, 2), ' ', 
        UNIFORM(10000, 99999, RANDOM())
    ) AS ADDRESS,
    UNIFORM(1, 500, RANDOM()) AS TOTAL_ORDERS
FROM TABLE(GENERATOR(ROWCOUNT => 100));

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

CREATE OR REPLACE PROCEDURE SKYFLOW_TOKENIZE_TABLE(
    vault_id VARCHAR,
    table_name VARCHAR,
    primary_key VARCHAR,
    pii_fields_delimited STRING,
    vault_owner_email VARCHAR
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.8
HANDLER = 'tokenize_table'
EXTERNAL_ACCESS_INTEGRATIONS = (SKYFLOW_EXTERNAL_ACCESS_INTEGRATION)
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'pyjwt', 'cryptography', 'requests', 'simplejson', 'cachetools')
SECRETS = ('cred' = SKYFLOW_VAULT_SECRET)
AS
$$
import simplejson as sjson
import jwt
import requests 
import time
import pandas
import re
from snowflake.snowpark import Session
from cachetools import cached, TTLCache
import _snowflake
from _snowflake import vectorized
from urllib.parse import quote_plus

http_session = requests.Session()
cache=TTLCache(maxsize=1024, ttl=3600)

def retry(attempts, delay, multiplier, callback):
    for i in range(attempts):
        result = callback()
        if result is not None:
            return result
        time.sleep(delay / 1000)  # Convert milliseconds to seconds
        delay *= multiplier  # Exponential backoff
    raise Exception(
        "Max retries exceeded. Error occurred generating bearer token")

@cached(cache)
def get_signed_jwt(credentials):
    try:
        # Create the claims object with the data in the creds object
        claims = {
            "iss": credentials["clientID"],
            "key": credentials["keyID"],
            "aud": credentials["tokenURI"],
            # JWT expires in Now + 60 minutes
            "exp": int(time.time()) + (3600),
            "sub": credentials["clientID"],
        }
        # Sign the claims object with the private key contained in the creds
        # object
        signedJWT = jwt.encode(
            claims,
            credentials["privateKey"],
            algorithm='RS256')

        return signedJWT

    except Exception:
        raise Exception("Unexpected error during JWT creation")


@cached(cache)
def get_bearer_token(credentials_hashable):
    try:
        credentials = dict(credentials_hashable)

        claims = {
            "iss": credentials["clientID"],
            "key": credentials["keyID"],
            "aud": credentials["tokenURI"],
            "exp": int(time.time()) + 3600,  # JWT expires in Now + 60 minutes
            "sub": credentials["clientID"],
        }
        
        signed_jwt = jwt.encode(claims, credentials["privateKey"], algorithm='RS256')
        
        # Request body parameters
        body = {
            'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
            'assertion': signed_jwt,
        }

        token_uri = credentials["tokenURI"]
        
        response = http_session.post(url=token_uri, json=body)
        response.raise_for_status()
        auth = sjson.loads(response.text)
        return auth["accessToken"]

    except Exception:
        return None

@cached(cache)
def GET_WORKSPACE_ID(auth_token):
    url = f"https://manage.skyflowapis.com/v1/workspaces"
    headers = {
        "Authorization": "Bearer " + auth_token,
        "X-SKYFLOW-ACCOUNT-ID": "<TODO: SKYFLOW_ACCOUNT_ID>"
    }
    response = http_session.get(url, headers=headers)
    workspace_response = sjson.loads(response.text)
    
    return workspace_response["workspaces"][0]["ID"]

def create_detokenize_udf(session, vault_id):
    # Define the SQL to create the UDF with Python as the handler
    udf_sql = '''
CREATE OR REPLACE FUNCTION SKYFLOW_DETOKENIZE(
    token VARCHAR,
    redaction_level STRING,
    username VARCHAR
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.8
HANDLER = 'SKYFLOW_DETOKENIZE'
EXTERNAL_ACCESS_INTEGRATIONS = (SKYFLOW_EXTERNAL_ACCESS_INTEGRATION)
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'pyjwt', 'cryptography', 'requests', 'simplejson', 'cachetools')
SECRETS = ('cred' = SKYFLOW_VAULT_SECRET)
AS
$REPLACE_ME$
import simplejson as sjson
import jwt
import requests
import time
import pandas
from snowflake.snowpark import Session
from cachetools import cached, TTLCache
import _snowflake
from _snowflake import vectorized

http_session = requests.Session()
cache = TTLCache(maxsize=1024, ttl=3600)

def retry(attempts, delay, multiplier, callback):
    for i in range(attempts):
        result = callback()
        if result is not None:
            return result
        time.sleep(delay / 1000)  # Convert milliseconds to seconds
        delay *= multiplier  # Exponential backoff
    raise Exception(
        "Max retries exceeded. Error occurred generating bearer token")

@cached(cache)
def get_signed_jwt(credentials_hashable, username):
    try:
        credentials = dict(credentials_hashable)
        # Create the claims object with the data in the creds object
        claims = {
            "iss": credentials["clientID"],
            "key": credentials["keyID"],
            "aud": credentials["tokenURI"],
            "exp": int(time.time()) + 3600,
            "sub": credentials["clientID"],
            "ctx": username
        }
        # Sign the claims object with the private key contained in the creds
        signedJWT = jwt.encode(
            claims,
            credentials["privateKey"],
            algorithm='RS256')

        return signedJWT

    except Exception as e:
        raise Exception(f"Unexpected error during JWT creation: {e}")

@cached(cache)
def get_bearer_token(credentials_hashable, username):
    try:
        # Retrieve the signed JWT
        signed_jwt = get_signed_jwt(credentials_hashable, username)

        # Request body parameters
        body = {
            'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
            'assertion': signed_jwt,
        }

        token_uri = dict(credentials_hashable)["tokenURI"]

        response = http_session.post(url=token_uri, json=body)
        response.raise_for_status()
        auth = sjson.loads(response.text)
        return auth["accessToken"]

    except Exception as e:
        raise Exception(f"Error generating bearer token: {e}")


@cached(cache)
def get_secret(secret_name):
    return sjson.loads(_snowflake.get_generic_secret_string(secret_name), strict=False)

@vectorized(input=pandas.DataFrame, max_batch_size=5000)
def SKYFLOW_DETOKENIZE(token_df):
    vault_id = 'VAULT_ID_HERE'
    
    # Extract token, redaction level, and username columns from the DataFrame
    tokens_list = token_df.iloc[:, 0].tolist()
    redaction_levels = token_df.iloc[:, 1].tolist()
    usernames = token_df.iloc[:, 2].tolist()
    
    # Assuming all rows have the same username
    username = usernames[0]  # Take the first username from the batch for the JWT
    
    # Retrieve credentials and generate a bearer token
    credentials = get_secret('cred')
    credentials_hashable = tuple(sorted(credentials.items()))  # Make credentials hashable
    auth_token = get_bearer_token(credentials_hashable, username)
    
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
            # Make the detokenization API request
            response = http_session.post(url, json=body, headers=headers)
            response.raise_for_status()

        except requests.exceptions.RequestException as e:
            error_msg = f"Detokenization request failed: {e}."
            return pandas.Series([f"Error: {error_msg}" for _ in range(len(tokens_list))])

        response_as_json = response.json()

        if 'records' not in response_as_json:
            error_msg = "'records' key not found in the response."
            return pandas.Series([f"Error: {error_msg}" for _ in range(len(tokens_list))])

        for record in response_as_json['records']:
            detokenized_results[record['token']] = record['value']

    # Map the detokenized values back to their original positions
    result_series = [None] * len(tokens_list)
    for token, info in unique_tokens.items():
        detokenized_value = detokenized_results.get(token, None)
        for idx in info['indices']:
            result_series[idx] = detokenized_value

    return pandas.Series(result_series)
$REPLACE_ME$;
'''
    udf_sql = udf_sql.replace("REPLACE_ME", "")
    udf_sql = udf_sql.replace("VAULT_ID_HERE", vault_id)
    
    session.sql(udf_sql).collect()
        
def create_masking_policy(session, policy_name):
    masking_policy_sql = f"""
    CREATE OR REPLACE MASKING POLICY {policy_name} AS (VAL STRING) RETURNS STRING ->
      CASE
        WHEN CURRENT_ROLE() IN ('ROLE_AUDIT_ADMIN') THEN SKYFLOW_DETOKENIZE(VAL, 'PLAIN_TEXT', CURRENT_USER())
        WHEN CURRENT_ROLE() IN ('ROLE_DATA_ENGINEER') THEN SKYFLOW_DETOKENIZE(VAL, 'MASKED', CURRENT_USER())
        WHEN CURRENT_ROLE() IN ('ROLE_MARKETING') THEN SKYFLOW_DETOKENIZE(VAL, 'REDACTED', CURRENT_USER())
        ELSE VAL
      END;
    """
    session.sql(masking_policy_sql).collect()

def set_masking_policy(session, table_name, column_name, policy_name):
    alter_column_sql = f"ALTER TABLE IF EXISTS {table_name} MODIFY COLUMN {column_name} SET MASKING POLICY {policy_name};"
    
    session.sql(alter_column_sql).collect()


def tokenize_table(session, vault_name, table_name, primary_key, pii_fields_delimited, vault_owner_email):
    credentials = sjson.loads(_snowflake.get_generic_secret_string('cred'), strict=False)
    credentials_hashable = tuple(sorted(credentials.items()))
    auth_token = get_bearer_token(credentials_hashable)

    vault_id = GET_VAULT_ID_BY_NAME(auth_token, vault_name)

    if vault_id:
        table_exists = GET_TABLE_BY_NAME(auth_token, vault_id, table_name)
        if not table_exists:
            ADD_TABLE_TO_VAULT(auth_token, vault_id, table_name, primary_key, pii_fields_delimited)
    else:
        # Create a new vault if it doesn't exist
        vault_id = SKYFLOW_CREATE_VAULT(auth_token, vault_name, table_name, primary_key, pii_fields_delimited, vault_owner_email)
        create_detokenize_udf(session, vault_id)

    pii_fields = [field.strip().upper() for field in pii_fields_delimited.split(',')]

    create_masking_policy(session, f"DETOKENIZE_COLUMN_{table_name}")
    for field in pii_fields:
        set_masking_policy(session, table_name, field, f"DETOKENIZE_COLUMN_{table_name}")

    columns = [primary_key.upper()] + pii_fields
    df = session.table(table_name).select(columns)

    batch_size = 25
    tokens_list = []

    data = df.to_pandas()

    skyflow_url = f"https://ebfc9bee4242.vault.skyflowapis.com/v1/vaults/{vault_id}/{table_name.lower()}"
    headers = {"Authorization": "Bearer " + auth_token}

    for i in range(0, len(data), batch_size):
        batch = data.iloc[i:i + batch_size]
        records = []
        for idx, row in batch.iterrows():
            # Include primary key and PII fields
            all_fields = [primary_key.upper()] + pii_fields
            fields = {col.lower(): row[col] for col in all_fields}  # Use lowercase for Skyflow API
            record = {'fields': fields}
            records.append(record)
        
        # Prepare the request payload and send to Skyflow API
        body = {'records': records, 'tokenization': True}
        response = http_session.post(skyflow_url, json=body, headers=headers)
        response.raise_for_status()  # Raise an error if the request fails
        response_json = response.json()
        
        # Process the response and build tokens_list
        for idx, record in enumerate(response_json['records']):
            tokens_per_record = record.get('tokens', {})
            # Add the primary key from the original batch
            tokens_per_record[primary_key.upper()] = batch.iloc[idx][primary_key.upper()]
            tokens_list.append(tokens_per_record)
        
        # Convert tokens_list to a Pandas DataFrame
        tokens_df = pandas.DataFrame(tokens_list)
        
        # **Option 1 Change:** Rename columns to uppercase
        tokens_df.columns = [col.upper() for col in tokens_df.columns]
        
        # Write tokens_df back to a temporary table in Snowflake
        tokens_snow_df = session.create_dataframe(tokens_df)
        tokens_table_name = f"TOKENS_{table_name.upper()}"
        
        tokens_snow_df.write.mode('overwrite').save_as_table(tokens_table_name)
        
        # **Option 1 Change:** Adjust SET clause to use unquoted uppercase column names
        set_clause = ', '.join([f's."{col.upper()}" = t.{col.upper()}' for col in pii_fields])
        
        # **Option 1 Change:** Adjust UPDATE statement to use unquoted tokens table and column names
        update_stmt = f'''
        UPDATE "{table_name.upper()}" AS s
        SET {set_clause}
        FROM {tokens_table_name} AS t
        WHERE s."{primary_key.upper()}" = t.{primary_key.upper()}
        '''
        session.sql(update_stmt).collect()

    # Drop the tokens table
    session.sql(f'DROP TABLE IF EXISTS {tokens_table_name}').collect()

    create_snowflake_stream(session, vault_id, table_name, primary_key, pii_fields_delimited)
    
    return f"Tokenization for table {table_name} completed successfully!"

def SKYFLOW_CREATE_VAULT(auth_token, vault_name, table_name, primary_key, pii_fields_delimited, vault_owner_email):

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
        else:
            # Use default configuration for fields without specific configurations
            field_block = {
                "name": field.lower(),
                "datatype": "DT_STRING",
                "isArray": False,
                "tags": [
                    {
                        "name": "skyflow.options.default_dlp_policy",
                        "values": ["REDACT"]
                    },
                    {
                        "name": "skyflow.options.operation",
                        "values": ["EXACT_MATCH"]
                    },
                    {
                        "name": "skyflow.options.default_token_policy",
                        "values": ["DETERMINISTIC_UUID"]
                    },
                    {
                        "name": "skyflow.options.description",
                        "values": ["String"]
                    },
                    {
                        "name": "skyflow.options.display_name",
                        "values": [field.lower()]
                    }
                ],
                "properties": None,
                "index": 0,
                "ID": ""
            }
        field_blocks.append(field_block)

    
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
        "workspaceID": GET_WORKSPACE_ID(auth_token),
        "owners": [
            {
                "ID": GET_USER_ID_BY_EMAIL(auth_token, vault_owner_email),
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
        "X-SKYFLOW-ACCOUNT-ID": "<TODO: SKYFLOW_ACCOUNT_ID>"
    }
    
    try:
        response = http_session.post(url, json=body, headers=headers)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to create vault: {e}")
    
    vault_response = sjson.loads(response.text)
    
    return vault_response["ID"]

def GET_USER_ID_BY_EMAIL(auth_token, user_email):
    encoded_email = quote_plus(user_email)
    url = f"https://manage.skyflowapis.com/v1/users?filterOps.email={encoded_email}"
    headers = {
        "Authorization": "Bearer " + auth_token,
        "X-SKYFLOW-ACCOUNT-ID": "<TODO: SKYFLOW_ACCOUNT_ID>"
    }

    response = http_session.get(url, headers=headers)
    user_response = sjson.loads(response.text)
    
    return user_response["users"][0]["ID"]   

def GET_VAULT_ID_BY_NAME(auth_token, vault_name):
    workspaceID = GET_WORKSPACE_ID(auth_token)
    url = f"https://manage.skyflowapis.com/v1/vaults?filterOps.name={vault_name}&workspaceID={workspaceID}"
    headers = {
        "Authorization": "Bearer " + auth_token,
        "X-SKYFLOW-ACCOUNT-ID": "<TODO: SKYFLOW_ACCOUNT_ID>"
    }

    response = http_session.get(url, headers=headers)
    vault_response = sjson.loads(response.text)
    
    # Check if the "vaults" key exists and if it contains any entries
    if "vaults" in vault_response and vault_response["vaults"]:
        return vault_response["vaults"][0]["ID"]
    else:
        return None

def GET_VAULT_DETAILS(auth_token, vault_id):
    url = f"https://manage.skyflowapis.com/v1/vaults/{vault_id}"
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "X-SKYFLOW-ACCOUNT-ID": "<TODO: SKYFLOW_ACCOUNT_ID>"
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        vault_details = response.json()
        return vault_details
    except requests.exceptions.RequestException as e:
        print(f"Failed to get vault details: {e}")
        return None

def GET_TABLE_BY_NAME(auth_token, vault_id, table_name):
    vault_details = GET_VAULT_DETAILS(auth_token, vault_id)

    if "vault" in vault_details:
        vault_data = vault_details["vault"]
        schemas = vault_data.get("schemas", [])
        table_name_lower = table_name.lower()
        for schema in schemas:
            schema_name = schema.get("name", "").lower()
            if schema_name == table_name_lower:
                return True
        return False
    else:
        return False

def GET_VAULT_SCHEMA(auth_token, vault_id):
    vault_details = GET_VAULT_DETAILS(auth_token, vault_id)
    if vault_details and "vault" in vault_details:
        vault_data = vault_details["vault"]
        return vault_data.get("schemas", [])
    else:
        return None

def UPDATE_VAULT_SCHEMA(vault_schema_current, table_name, primary_key, pii_fields_delimited):
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
                "index": 0,
                "properties": None,
                "ID": ""
            }
        else:
            # Use default configuration for fields without specific configurations
            field_block = {
                "name": field.lower(),
                "datatype": "DT_STRING",
                "isArray": False,
                "tags": [
                    {
                        "name": "skyflow.options.default_dlp_policy",
                        "values": ["REDACT"]
                    },
                    {
                        "name": "skyflow.options.operation",
                        "values": ["EXACT_MATCH"]
                    },
                    {
                        "name": "skyflow.options.default_token_policy",
                        "values": ["DETERMINISTIC_UUID"]
                    },
                    {
                        "name": "skyflow.options.description",
                        "values": ["String"]
                    },
                    {
                        "name": "skyflow.options.display_name",
                        "values": [field.lower()]
                    }
                ],
                "properties": None,
                "index": 0,
                "ID": ""
            }
        field_blocks.append(field_block)
    
    # Create the new table schema
    new_table_schema = {
        "name": table_name,
        "parentSchemaProperties": None,
        "fields": field_blocks,
        "childrenSchemas": [],
        "schemaTags": [],
        "properties": None,
        "ID": ""
    }
    
    # Add the new table to the existing vault schema
    vault_schema_current.append(new_table_schema)
    
    # Return the updated list of schemas
    return vault_schema_current

def ADD_TABLE_TO_VAULT(auth_token, vault_id, table_name, primary_key, pii_fields_delimited):
    # Get current vault schema
    vault_schema_current = GET_VAULT_SCHEMA(auth_token, vault_id)
    if not vault_schema_current:
        raise Exception(f"Failed to retrieve current schema for vault '{vault_id}'.")
    
    # Update the vault schema by adding the new table
    vault_schema_new = UPDATE_VAULT_SCHEMA(vault_schema_current, table_name, primary_key, pii_fields_delimited)
        
    # Retrieve current vault details to include necessary fields
    vault_details = GET_VAULT_DETAILS(auth_token, vault_id)
    if not vault_details or 'vault' not in vault_details:
        raise Exception(f"Vault with ID '{vault_id}' not found.")
    vault_info = vault_details['vault']

    # Build the body for the PATCH request
    body = {
        "vaultSchema": {
            "schemas": vault_schema_new  # Use the updated list of schemas
        }
    }

    # Define the PATCH request URL and headers
    url = f"https://manage.skyflowapis.com/v1/vaults/{vault_id}"
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json",
        "X-SKYFLOW-ACCOUNT-ID": "<TODO: SKYFLOW_ACCOUNT_ID>"  # Replace with actual account ID
    }

    # Make the PATCH request
    try:
        response = requests.patch(url, headers=headers, json=body)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to patch vault: {e}")

    vault_response = response.json()
    return vault_response

def create_snowflake_stream(session, vault_id, table_name, primary_key, pii_fields_delimited):
    # Retrieve the current warehouse from the session
    warehouse_name = session.sql("SELECT CURRENT_WAREHOUSE()").collect()[0][0]
    
    # Ensure that a warehouse is set
    if not warehouse_name:
        raise ValueError("No warehouse is currently set in the session. Please set a warehouse before running this code.")
    
    # Create or replace the stream and task for continuous tokenization
    # Create the stream
    session.sql(f"""
        CREATE OR REPLACE STREAM SKYFLOW_PII_STREAM_{table_name}
        ON TABLE {table_name}
    """).collect()

    # Create the task using the current warehouse
    session.sql(f"""
        CREATE OR REPLACE TASK SKYFLOW_PII_STREAM_{table_name}_TASK
        WAREHOUSE = '{warehouse_name}'
        SCHEDULE = '1 MINUTE'
        WHEN SYSTEM$STREAM_HAS_DATA('SKYFLOW_PII_STREAM_{table_name}')
        AS CALL SKYFLOW_PROCESS_PII(
            '{vault_id}',
            '{table_name}',
            '{primary_key}',
            '{pii_fields_delimited}'
        )
    """).collect()
    
    # Resume the task
    session.sql(f"""
        ALTER TASK SKYFLOW_PII_STREAM_{table_name}_TASK RESUME
    """).collect()

$$;


CREATE OR REPLACE PROCEDURE SKYFLOW_PROCESS_PII(vault_id VARCHAR, table_name VARCHAR, primary_key VARCHAR, pii_fields STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.8
HANDLER = 'SKYFLOW_PROCESS_PII'
EXTERNAL_ACCESS_INTEGRATIONS = (SKYFLOW_EXTERNAL_ACCESS_INTEGRATION)
PACKAGES = ('snowflake-snowpark-python', 'pyjwt', 'cryptography', 'requests', 'simplejson', 'cachetools')
SECRETS = ('cred' = SKYFLOW_VAULT_SECRET)
AS
$$
import _snowflake
import simplejson as sjson
import jwt
import requests 
import time
from cachetools import cached, TTLCache

http_session = requests.Session()
cache=TTLCache(maxsize=1024, ttl=3600)

def retry(attempts, delay, multiplier, callback):
    for i in range(attempts):
        result = callback()
        if result is not None:
            return result
        time.sleep(delay / 1000)  # Convert milliseconds to seconds
        delay *= multiplier  # Exponential backoff
    raise Exception(
        "Max retries exceeded. Error occurred generating bearer token")

@cached(cache)
def get_signed_jwt(credentials):
    try:
        # Create the claims object with the data in the creds object
        claims = {
            "iss": credentials["clientID"],
            "key": credentials["keyID"], 
            "aud": credentials["tokenURI"], 
            # JWT expires in Now + 60 minutes
            "exp": int(time.time()) + (3600),
            "sub": credentials["clientID"], 
        }
        # Sign the claims object with the private key contained in the creds
        # object
        signedJWT = jwt.encode(
            claims,
            credentials["privateKey"],
            algorithm='RS256')

        return signedJWT

    except Exception:
        raise Exception("Unexpected error during JWT creation")

@cached(cache)
def get_bearer_token(credentials_hashable):
    try:
        credentials = dict(credentials_hashable)

        claims = {
            "iss": credentials["clientID"],
            "key": credentials["keyID"], 
            "aud": credentials["tokenURI"], 
            "exp": int(time.time()) + 3600,  # JWT expires in Now + 60 minutes
            "sub": credentials["clientID"], 
        }
        
        signed_jwt = jwt.encode(claims, credentials["privateKey"], algorithm='RS256')
        
        # Request body parameters
        body = {
            'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
            'assertion': signed_jwt,
        }

        token_uri = credentials["tokenURI"]

        response = http_session.post(url=token_uri, json=body)
        response.raise_for_status()
        auth = sjson.loads(response.text)
        return auth["accessToken"]

    except Exception:
        return None

@cached(cache)
def get_secret(secret_name):
    return sjson.loads(_snowflake.get_generic_secret_string(secret_name), strict=False)
        
def SKYFLOW_PROCESS_PII(session, vault_id, table_name, primary_key, pii_fields):
    credentials = get_secret('cred')
    credentials_hashable = tuple(sorted(credentials.items()))
    auth_token = get_bearer_token(credentials_hashable)

    # Convert primary_key and pii_fields to uppercase
    primary_key = primary_key.upper()
    pii_fields_list = [field.strip().upper() for field in pii_fields.split(',')]
    pii_fields_list.append(primary_key)

    # Skyflow static variables
    table_name_skyflow = table_name.lower()
    skyflow_account_id = "<TODO: SKYFLOW_ACCOUNT_ID>"
    skyflow_url_vault = f"https://ebfc9bee4242.vault.skyflowapis.com/v1/vaults/{vault_id}/"

    # Retrieve all stream records
    stream_records = session.sql(f"SELECT {primary_key}, METADATA$ACTION, {', '.join(pii_fields_list)} FROM SKYFLOW_PII_STREAM_{table_name}").collect()

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
        response = http_session.get(
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
            session.sql(sql_command).collect()

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
            
            response = http_session.post(url, json=body, headers=headers)
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
    session.sql(f'CREATE OR REPLACE STREAM SKYFLOW_PII_STREAM_{table_name} ON TABLE SKYFLOW_DEMO.PUBLIC.{table_name}').collect()
    return "Changes processed"

$$;

-- Create roles
CREATE ROLE ROLE_AUDIT_ADMIN;
CREATE ROLE ROLE_DATA_ENGINEER;
CREATE ROLE ROLE_MARKETING;

-- Grant usage on database SKYFLOW_DEMO to roles
GRANT USAGE ON DATABASE SKYFLOW_DEMO TO ROLE ROLE_AUDIT_ADMIN;
GRANT USAGE ON DATABASE SKYFLOW_DEMO TO ROLE ROLE_DATA_ENGINEER;
GRANT USAGE ON DATABASE SKYFLOW_DEMO TO ROLE ROLE_MARKETING;

-- Grant usage on schema SKYFLOW_DEMO.PUBLIC to roles
GRANT USAGE ON SCHEMA SKYFLOW_DEMO.PUBLIC TO ROLE ROLE_AUDIT_ADMIN;
GRANT USAGE ON SCHEMA SKYFLOW_DEMO.PUBLIC TO ROLE ROLE_DATA_ENGINEER;
GRANT USAGE ON SCHEMA SKYFLOW_DEMO.PUBLIC TO ROLE ROLE_MARKETING;

-- Grant access to tables SKYFLOW_DEMO.PUBLIC.CUSTOMERS and SKYFLOW_DEMO.PUBLIC.SUPPLIERS to roles
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SKYFLOW_DEMO.PUBLIC.CUSTOMERS TO ROLE ROLE_AUDIT_ADMIN;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SKYFLOW_DEMO.PUBLIC.CUSTOMERS TO ROLE ROLE_DATA_ENGINEER;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SKYFLOW_DEMO.PUBLIC.CUSTOMERS TO ROLE ROLE_MARKETING;

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SKYFLOW_DEMO.PUBLIC.SUPPLIERS TO ROLE ROLE_AUDIT_ADMIN;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SKYFLOW_DEMO.PUBLIC.SUPPLIERS TO ROLE ROLE_DATA_ENGINEER;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SKYFLOW_DEMO.PUBLIC.SUPPLIERS TO ROLE ROLE_MARKETING;

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

CALL GRANT_WAREHOUSE_ACCESS('ROLE_AUDIT_ADMIN');
CALL GRANT_WAREHOUSE_ACCESS('ROLE_DATA_ENGINEER');
CALL GRANT_WAREHOUSE_ACCESS('ROLE_MARKETING');

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

CALL GRANT_ROLE_TO_CURRENT_USER('ROLE_AUDIT_ADMIN');
CALL GRANT_ROLE_TO_CURRENT_USER('ROLE_DATA_ENGINEER');
CALL GRANT_ROLE_TO_CURRENT_USER('ROLE_MARKETING');
