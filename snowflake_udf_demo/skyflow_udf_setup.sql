-- Step 1: Create Database
CREATE OR REPLACE DATABASE SKYFLOW_DEMO;
USE DATABASE SKYFLOW_DEMO;

-- Step 2: Create table with PII and Non-PII columns
CREATE OR REPLACE TABLE SKYFLOW_DEMO.PUBLIC.CUSTOMERS (
	CUSTOMER_ID NUMBER(38,0),
	NAME VARCHAR(16777216),
	EMAIL VARCHAR(16777216),
	PHONE VARCHAR(16777216),
    ADDRESS VARCHAR(16777216),
    LIFETIME_PURCHASE_AMOUNT VARCHAR(16777216),
    CUSTOMER_SINCE VARCHAR(16777216)
);

-- Step 3: In Skyflow Studio, create a table having the same name as your Snowflake table. Add a table column for each Snowflake PII column you want to protect, also matching column name.

-- Step 4: Insert sample records into table
INSERT INTO CUSTOMERS (CUSTOMER_ID, NAME, EMAIL, PHONE, ADDRESS, LIFETIME_PURCHASE_AMOUNT, CUSTOMER_SINCE) VALUES
    (1,'Mr Emily Williams','Mr.emily.williams@example.com','+1 555-628-8461','546 Maple St Chicago CA 51610','$100.30','2006-01-10'),
    (2,'Mr Anna Jones','Mr.anna.jones@example.com','+1 555-520-7701','595 Pine St Phoenix IL 49184','$2350.00','2010-02-27'),
    (3,'Mr Anna Williams','Mr.anna.williams@example.com','+1 555-683-9434','610 Maple St Chicago NY 27393','$10000.50','2014-01-05'),
    (4,'Mr Michael Williams','Mr.michael.williams@example.com','+1 555-860-5835','114 Maple St Chicago TX 20094','$2350.00','2022-06-12'),
    (5,'Mr John Smith','Mr.john.smith@example.com','+1 555-949-7779','631 Pine St Houston IL 14847','$10000.50','2005-08-06'),
    (6,'Mr John Johnson','Mr.john.johnson@example.com','+1 555-607-2361','931 Birch St Chicago CA 83399','$0.00','2021-05-14'),
    (7,'Mr Sarah Johnson','Mr.sarah.johnson@example.com','+1 555-997-1200','236 Maple St New York NY 50705','$2350.00','2020-12-20'),
    (8,'Mr John Smith','Mr.john.smith@example.com','+1 555-568-7149','432 Main St Los Angeles TX 54601','$100.30','2000-04-06'),
    (9,'Mr Anna Smith','Mr.anna.smith@example.com','+1 555-805-8960','369 Pine St Los Angeles AZ 48124','$5.00','2020-12-02'),
    (10,'Mr Emily Johnson','Mr.emily.johnson@example.com','+1 555-523-8453','973 Birch St Phoenix CA 80252','$0.00','2010-12-03'),
    (11,'Mr Anna Williams','Mr.anna.williams@example.com','+1 555-253-5153','276 Maple St Chicago CA 63105','$2350.00','2010-10-28'),
    (12,'Mr John Jones','Mr.john.jones@example.com','+1 555-557-6005','732 Pine St Phoenix NY 29608','$10000.50','2017-11-12'),
    (13,'Mr Sarah Jones','Mr.sarah.jones@example.com','+1 555-598-3481','354 Maple St Houston IL 24895','$100.30','2020-12-11'),
    (14,'Mr Anna Jones','Mr.anna.jones@example.com','+1 555-645-9231','496 Pine St Los Angeles NY 56789','$0.00','2009-07-11'),
    (15,'Mr Emily Johnson','Mr.emily.johnson@example.com','+1 555-636-4305','875 Oak St Phoenix IL 12450','$2350.00','2018-02-28'),
    (16,'Mr Sarah Brown','Mr.sarah.brown@example.com','+1 555-986-8006','106 Oak St Chicago AZ 25991','$5.00','2021-07-04'),
    (17,'Mr John Brown','Mr.john.brown@example.com','+1 555-807-3363','913 Main St Houston NY 25919','$5.00','2011-01-20'),
    (18,'Mr Emily Williams','Mr.emily.williams@example.com','+1 555-155-1355','920 Pine St New York NY 43337','$100.30','2018-04-11'),
    (19,'Mr Sarah Williams','Mr.sarah.williams@example.com','+1 555-931-4635','165 Birch St Los Angeles IL 73464','$0.00','2014-04-23'),
    (20,'Mr Emily Brown','Mr.emily.brown@example.com','+1 555-732-6362','901 Pine St Chicago TX 79604','$100.30','2017-12-07'),
    (21,'Mr Anna Williams','Mr.anna.williams@example.com','+1 555-947-1780','356 Oak St New York AZ 20027','$5.00','2017-08-04'),
    (22,'Mr Sarah Brown','Mr.sarah.brown@example.com','+1 555-141-9085','468 Pine St Phoenix CA 84368','$10000.50','2012-09-19'),
    (23,'Mr Emily Brown','Mr.emily.brown@example.com','+1 555-533-1275','731 Pine St Los Angeles NY 30352','$10000.50','2016-12-07'),
    (24,'Mr Sarah Smith','Mr.sarah.smith@example.com','+1 555-118-1603','909 Maple St New York TX 84958','$100.30','2012-03-01'),
    (25,'Mr Michael Johnson','Mr.michael.johnson@example.com','+1 555-719-2980','547 Main St Phoenix CA 34402','$10000.50','2000-07-10'),
    (26,'Mr Anna Brown','Mr.anna.brown@example.com','+1 555-405-3115','402 Birch St Phoenix TX 21963','$5.00','2021-11-08'),
    (27,'Mr Michael Smith','Mr.michael.smith@example.com','+1 555-536-7415','596 Main St New York NY 79193','$100.30','2004-02-04'),
    (28,'Mr John Smith','Mr.john.smith@example.com','+1 555-527-8872','832 Maple St New York AZ 65481','$10000.50','2012-09-10'),
    (29,'Mr Emily Williams','Mr.emily.williams@example.com','+1 555-451-5503','684 Main St Phoenix TX 17913','$2350.00','2014-10-23'),
    (30,'Mr Anna Williams','Mr.anna.williams@example.com','+1 555-412-2403','675 Oak St New York AZ 34649','$10000.50','2021-12-12'),
    (31,'Mr Michael Johnson','Mr.michael.johnson@example.com','+1 555-650-8115','730 Maple St Chicago NY 70293','$10000.50','2008-09-26'),
    (32,'Mr Michael Johnson','Mr.michael.johnson@example.com','+1 555-880-6807','787 Main St Los Angeles TX 93741','$100.30','2018-03-19'),
    (33,'Mr Michael Williams','Mr.michael.williams@example.com','+1 555-653-1151','918 Maple St New York AZ 28774','$10000.50','2015-08-24'),
    (34,'Mr Sarah Brown','Mr.sarah.brown@example.com','+1 555-628-8883','735 Birch St Houston NY 15679','$2350.00','2017-07-23'),
    (35,'Mr Emily Johnson','Mr.emily.johnson@example.com','+1 555-874-5521','966 Main St Los Angeles AZ 83625','$2350.00','2004-11-19'),
    (36,'Mr Michael Smith','Mr.michael.smith@example.com','+1 555-631-9568','639 Maple St Houston IL 82723','$0.00','2005-01-24'),
    (37,'Mr Emily Johnson','Mr.emily.johnson@example.com','+1 555-383-7664','804 Maple St Los Angeles NY 18453','$100.30','2013-12-03'),
    (38,'Mr Anna Williams','Mr.anna.williams@example.com','+1 555-876-6670','375 Pine St Chicago AZ 31448','$2350.00','2002-09-06'),
    (39,'Mr Emily Brown','Mr.emily.brown@example.com','+1 555-476-2852','905 Main St Chicago AZ 81225','$2350.00','2003-06-26'),
    (40,'Mr John Smith','Mr.john.smith@example.com','+1 555-415-4763','380 Main St Los Angeles CA 47263','$0.00','2018-11-14'),
    (41,'Mr Anna Jones','Mr.anna.jones@example.com','+1 555-396-2464','389 Maple St Houston IL 97103','$2350.00','2004-03-08'),
    (42,'Mr John Brown','Mr.john.brown@example.com','+1 555-212-6343','132 Maple St Chicago TX 92655','$10000.50','2004-05-03'),
    (43,'Mr Michael Williams','Mr.michael.williams@example.com','+1 555-556-1788','991 Birch St Los Angeles TX 98976','$2350.00','2003-11-05'),
    (44,'Mr Anna Brown','Mr.anna.brown@example.com','+1 555-624-5962','491 Main St Houston AZ 83095','$2350.00','2017-01-24'),
    (45,'Mr Anna Williams','Mr.anna.williams@example.com','+1 555-684-7247','360 Maple St New York IL 92069','$100.30','2013-05-02'),
    (46,'Mr Anna Williams','Mr.anna.williams@example.com','+1 555-839-3067','211 Oak St New York NY 58052','$0.00','2015-09-09'),
    (47,'Mr Emily Williams','Mr.emily.williams@example.com','+1 555-943-6665','551 Main St Houston CA 86607','$0.00','2002-10-04'),
    (48,'Mr Michael Jones','Mr.michael.jones@example.com','+1 555-804-2178','336 Maple St Los Angeles CA 99571','$2350.00','2016-01-17'),
    (49,'Mr John Brown','Mr.john.brown@example.com','+1 555-803-3573','743 Maple St Chicago AZ 89997','$0.00','2002-10-05'),
    (50,'Mr Anna Smith','Mr.anna.smith@example.com','+1 555-784-5680','200 Pine St Los Angeles CA 43183','$2350.00','2012-09-05'),
    (51,'Mr Michael Brown','Mr.michael.brown@example.com','+1 555-339-8110','598 Maple St Los Angeles AZ 68893','$0.00','2009-01-02'),
    (52,'Mr Emily Smith','Mr.emily.smith@example.com','+1 555-570-2056','497 Birch St Chicago TX 21113','$0.00','2009-06-27'),
    (53,'Mr Sarah Williams','Mr.sarah.williams@example.com','+1 555-403-6942','705 Birch St New York IL 54979','$5.00','2014-02-25'),
    (54,'Mr Anna Smith','Mr.anna.smith@example.com','+1 555-756-1547','950 Pine St Houston IL 24355','$100.30','2000-09-05'),
    (55,'Mr Emily Jones','Mr.emily.jones@example.com','+1 555-245-3459','190 Birch St New York NY 33074','$100.30','2021-05-22'),
    (56,'Mr Anna Johnson','Mr.anna.johnson@example.com','+1 555-359-2592','967 Main St New York CA 31678','$10000.50','2022-01-16'),
    (57,'Mr Michael Smith','Mr.michael.smith@example.com','+1 555-762-7150','652 Oak St New York CA 39769','$5.00','2018-09-06'),
    (58,'Mr Anna Brown','Mr.anna.brown@example.com','+1 555-799-9741','277 Birch St Los Angeles IL 81298','$5.00','2011-01-28'),
    (59,'Mr Anna Jones','Mr.anna.jones@example.com','+1 555-705-4453','597 Oak St New York IL 67312','$0.00','2013-08-22'),
    (60,'Mr Michael Brown','Mr.michael.brown@example.com','+1 555-330-1073','865 Pine St Phoenix AZ 86148','$100.30','2019-03-26'),
    (61,'Mr Sarah Brown','Mr.sarah.brown@example.com','+1 555-730-8010','143 Pine St Chicago CA 84620','$100.30','2008-07-19'),
    (62,'Mr John Brown','Mr.john.brown@example.com','+1 555-764-1226','744 Pine St Los Angeles CA 10398','$100.30','2020-05-13'),
    (63,'Mr Emily Johnson','Mr.emily.johnson@example.com','+1 555-767-8354','859 Pine St Houston IL 12621','$5.00','2022-06-18'),
    (64,'Mr Michael Johnson','Mr.michael.johnson@example.com','+1 555-949-7401','875 Main St New York NY 62393','$10000.50','2005-05-15'),
    (65,'Mr Emily Williams','Mr.emily.williams@example.com','+1 555-899-8752','892 Maple St Houston AZ 78538','$5.00','2002-02-05'),
    (66,'Mr Emily Williams','Mr.emily.williams@example.com','+1 555-675-3953','807 Maple St Chicago NY 24247','$100.30','2007-05-08'),
    (67,'Mr Emily Johnson','Mr.emily.johnson@example.com','+1 555-431-3742','172 Birch St Los Angeles IL 96172','$0.00','2020-01-15'),
    (68,'Mr Anna Brown','Mr.anna.brown@example.com','+1 555-182-5269','454 Oak St Los Angeles AZ 98016','$5.00','2008-01-08'),
    (69,'Mr Michael Jones','Mr.michael.jones@example.com','+1 555-748-1654','364 Oak St Los Angeles TX 50887','$100.30','2012-03-06'),
    (70,'Mr Emily Brown','Mr.emily.brown@example.com','+1 555-817-3273','979 Birch St Phoenix CA 62320','$100.30','2000-12-18'),
    (71,'Mr Anna Smith','Mr.anna.smith@example.com','+1 555-411-3795','886 Maple St Chicago CA 57098','$10000.50','2003-06-02'),
    (72,'Mr John Smith','Mr.john.smith@example.com','+1 555-405-8614','398 Birch St Chicago IL 66257','$100.30','2009-01-20'),
    (73,'Mr Michael Williams','Mr.michael.williams@example.com','+1 555-211-6617','660 Pine St Los Angeles IL 27143','$2350.00','2002-05-28'),
    (74,'Mr Sarah Johnson','Mr.sarah.johnson@example.com','+1 555-866-2798','302 Maple St New York CA 72035','$100.30','2011-07-28'),
    (75,'Mr John Johnson','Mr.john.johnson@example.com','+1 555-609-9944','935 Pine St New York IL 87011','$100.30','2001-08-12'),
    (76,'Mr John Johnson','Mr.john.johnson@example.com','+1 555-284-8448','111 Birch St Chicago AZ 46405','$2350.00','2016-08-19'),
    (77,'Mr Emily Johnson','Mr.emily.johnson@example.com','+1 555-542-3567','356 Birch St Los Angeles AZ 41347','$100.30','2007-06-03'),
    (78,'Mr John Johnson','Mr.john.johnson@example.com','+1 555-674-1736','432 Main St Phoenix IL 19186','$5.00','2003-09-19'),
    (79,'Mr Michael Brown','Mr.michael.brown@example.com','+1 555-358-4259','506 Pine St Los Angeles TX 76302','$0.00','2010-09-12'),
    (80,'Mr Anna Brown','Mr.anna.brown@example.com','+1 555-494-9607','657 Main St New York TX 45174','$100.30','2018-11-06'),
    (81,'Mr Sarah Johnson','Mr.sarah.johnson@example.com','+1 555-828-5684','541 Maple St Chicago CA 19324','$10000.50','2015-08-27'),
    (82,'Mr John Williams','Mr.john.williams@example.com','+1 555-683-2906','337 Birch St Phoenix NY 31778','$0.00','2002-03-01'),
    (83,'Mr Anna Smith','Mr.anna.smith@example.com','+1 555-817-9947','557 Main St Chicago CA 13999','$0.00','2006-05-15'),
    (84,'Mr Sarah Williams','Mr.sarah.williams@example.com','+1 555-316-5070','354 Birch St Los Angeles AZ 86985','$2350.00','2016-02-14'),
    (85,'Mr Sarah Jones','Mr.sarah.jones@example.com','+1 555-457-9550','211 Main St Chicago CA 81096','$5.00','2003-07-17'),
    (86,'Mr Sarah Brown','Mr.sarah.brown@example.com','+1 555-921-5979','839 Oak St Los Angeles TX 51667','$10000.50','2014-07-08'),
    (87,'Mr John Brown','Mr.john.brown@example.com','+1 555-325-8763','546 Birch St Los Angeles IL 83452','$10000.50','2012-11-26'),
    (88,'Mr Michael Johnson','Mr.michael.johnson@example.com','+1 555-870-4352','905 Oak St Los Angeles TX 42840','$5.00','2008-09-05'),
    (89,'Mr Michael Williams','Mr.michael.williams@example.com','+1 555-362-7354','895 Oak St Phoenix TX 38050','$5.00','2003-06-24'),
    (90,'Mr Emily Johnson','Mr.emily.johnson@example.com','+1 555-518-1078','874 Oak St Los Angeles AZ 64077','$10000.50','2007-06-26'),
    (91,'Mr Emily Johnson','Mr.emily.johnson@example.com','+1 555-452-7067','562 Birch St Houston AZ 48430','$5.00','2011-03-07'),
    (92,'Mr Anna Smith','Mr.anna.smith@example.com','+1 555-870-6047','246 Oak St Phoenix IL 39563','$2350.00','2013-04-23'),
    (93,'Mr Anna Smith','Mr.anna.smith@example.com','+1 555-791-4356','582 Pine St New York IL 40424','$2350.00','2001-09-28'),
    (94,'Mr Anna Williams','Mr.anna.williams@example.com','+1 555-124-6681','419 Maple St Phoenix AZ 96044','$100.30','2021-10-02'),
    (95,'Mr John Jones','Mr.john.jones@example.com','+1 555-288-7646','579 Birch St Chicago TX 64340','$10000.50','2000-10-20'),
    (96,'Mr Michael Smith','Mr.michael.smith@example.com','+1 555-935-9877','714 Birch St Houston NY 53582','$0.00','2017-08-18'),
    (97,'Mr Michael Williams','Mr.michael.williams@example.com','+1 555-974-5761','892 Oak St Houston TX 43428','$0.00','2017-02-02'),
    (98,'Mr Michael Brown','Mr.michael.brown@example.com','+1 555-410-3792','820 Main St Chicago NY 21156','$10000.50','2003-02-01'),
    (99,'Mr Anna Jones','Mr.anna.jones@example.com','+1 555-470-7280','950 Oak St Chicago IL 77228','$2350.00','2009-11-24'),
    (100,'Mr Michael Johnson','Mr.michael.johnson@example.com','+1 555-296-8615','976 Pine St Los Angeles AZ 76122','$2350.00','2002-04-15');



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

 
-- Step 6: Create Stored Procedure for table tokenization. Skyflow table and column names should match the snowflake table and column names. Include PII columns only.
CREATE OR REPLACE PROCEDURE SKYFLOW_TOKENIZE_TABLE(vault_name VARCHAR, table_name VARCHAR, primary_key VARCHAR, pii_fields_delimited STRING, vault_owner_email VARCHAR)
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
from urllib.parse import quote_plus

def GENERATE_AUTH_TOKEN(session):
    credentials = json.loads(_snowflake.get_generic_secret_string('cred'), strict=False)
    
    # Create the claims object with the data in the creds object
    claims = {
       "iss": credentials["clientID"],
       "key": credentials["keyID"], 
       "aud": credentials["tokenURI"], 
       "exp": int(time.time()) + (3600), # JWT expires in Now + 60 minutes
       "sub": credentials["clientID"], 
    }
    # Sign the claims object with the private key contained in the creds object
    signedJWT = jwt.encode(claims, credentials["privateKey"], algorithm='RS256')

    body = {
       'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
       'assertion': signedJWT,
    }
    tokenURI = credentials["tokenURI"]

    session = requests.Session()
    r = session.post(tokenURI, json=body)
    auth = json.loads(r.text)
    
    return auth["accessToken"]

def GET_ACCOUNT_ID():
    return "<TODO: ACCOUNT_ID>"

def GET_WORKSPACE_ID(session, auth_token):
    url = f"https://manage.skyflowapis.com/v1/workspaces"
    headers = {
        "Authorization": "Bearer " + auth_token,
        "X-SKYFLOW-ACCOUNT-ID": GET_ACCOUNT_ID()
    }

    session = requests.Session()
    response = session.get(url, headers=headers)
    workspace_response = json.loads(response.text)
    
    return workspace_response["workspaces"][0]["ID"]  

def GET_USER_ID_BY_EMAIL(session, auth_token, user_email):
    encoded_email = quote_plus(user_email)
    url = f"https://manage.skyflowapis.com/v1/users?filterOps.email={encoded_email}"
    headers = {
        "Authorization": "Bearer " + auth_token,
        "X-SKYFLOW-ACCOUNT-ID": GET_ACCOUNT_ID()
    }

    session = requests.Session()
    response = session.get(url, headers=headers)
    user_response = json.loads(response.text)
    
    return user_response["users"][0]["ID"]   
    
def SKYFLOW_CREATE_VAULT(session, auth_token, vault_name, table_name, primary_key, pii_fields_delimited, vault_owner_email):
    # Split the comma-separated fields into a list
    pii_fields = pii_fields_delimited.split(',')
    
    # Initialize an list with skyflow_id and primary_key to start, pii fields will be appended
    field_blocks = [
                        {
                            "name": "skyflow_id",
                            "datatype": "DT_STRING",
            "isArray": False,
                            "tags": [
                                {
                                    "name": "skyflow.options.default_dlp_policy",
                                    "values": [
                                        "PLAIN_TEXT"
                                    ]
                                },
                                {
                                    "name": "skyflow.options.operation",
                                    "values": [
                                        "ALL_OP"
                                    ]
                                },
                                {
                                    "name": "skyflow.options.sensitivity",
                                    "values": [
                                        "LOW"
                                    ]
                                },
                                {
                                    "name": "skyflow.options.data_type",
                                    "values": [
                                        "skyflow.SkyflowID"
                                    ]
                                },
                                {
                                    "name": "skyflow.options.description",
                                    "values": [
                                        "Skyflow defined Primary Key"
                                    ]
                                },
                                {
                                    "name": "skyflow.options.display_name",
                                    "values": [
                                        "Skyflow ID"
                                    ]
                                }
                            ],
                            "index": 0
                        },
                        {
            "name": primary_key.lower(),
                            "datatype": "DT_INT32",
            "isArray": False,
                            "tags": [
                                {
                                    "name": "skyflow.options.default_dlp_policy",
                                    "values": [
                                        "PLAIN_TEXT"
                                    ]
                                },
                                {
                                    "name": "skyflow.options.operation",
                                    "values": [
                                        "ALL_OP"
                                    ]
                                },
                                {
                                    "name": "skyflow.options.unique",
                                    "values": [
                                        "true"
                                    ]
                                },
                                {
                                    "name": "skyflow.options.display_name",
                                    "values": [
                        primary_key.lower()
                                    ]
                                }
                            ],
                            "index": 0
        }
    ]
    
    # Loop over each field in the pii_fields list and create a field block
    for field in pii_fields:
        field_block = {
            "name": field.lower(),
                            "datatype": "DT_STRING",
            "isArray": False,
                            "tags": [
                                {
                                    "name": "skyflow.options.default_dlp_policy",
                    "values": ["MASK"]
                                },
                                {
                                    "name": "skyflow.options.operation",
                    "values": ["ALL_OP"]
                                },
                                {
                                    "name": "skyflow.options.default_token_policy",
                    "values": ["DETERMINISTIC_FPT"]
                                },
                                {
                                    "name": "skyflow.options.index",
                    "values": ["true"]
                                },
                                {
                                    "name": "skyflow.options.configuration_tags",
                    "values": ["NULLABLE"]
                                },
                                {
                                    "name": "skyflow.options.display_name",
                    "values": [field.lower()]
                                }
                            ],
                            "index": 0
                        }
        field_blocks.append(field_block)
    
    url = "https://manage.skyflowapis.com/v1/vaults"
    headers = {
        "Authorization": "Bearer " + auth_token,
        "X-SKYFLOW-ACCOUNT-ID": GET_ACCOUNT_ID()
    }
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
                {
                    "name": "skyflow.options.experimental",
                    "values": [
                        "true"
                    ]
                },
                {
                    "name": "skyflow.options.vault_main_object",
                    "values": [
                        "Quickstart"
                    ]
                },
                {
                    "name": "skyflow.options.query_interface",
                    "values": [
                        "REST",
                        "SQL"
                    ]
                },
                {
                    "name": "skyflow.options.env_name",
                    "values": [
                        "ALL_ENV"
                    ]
                },
                {
                    "name": "skyflow.options.display_name",
                    "values": [
                        "Quickstart"
                    ]
                }
            ]
        },
        "workspaceID": GET_WORKSPACE_ID(session, auth_token),
        "owners": [
            {
                "ID": GET_USER_ID_BY_EMAIL(session, auth_token, vault_owner_email),
                "type": "USER"
            },
            {
                "ID": "<TODO: SERVICE_ACCOUNT_ID>",
                "type": "SERVICE_ACCOUNT"
            }
        ]
    }
    session = requests.Session()
    response = session.post(url, json=body, headers=headers)
    vault_response = json.loads(response.text)
    
    return vault_response["ID"]

def SKYFLOW_TOKENIZE_TABLE(session, vault_name, table_name, primary_key, pii_fields_delimited, vault_owner_email):
    auth_token = GENERATE_AUTH_TOKEN(session)
    vault_id = SKYFLOW_CREATE_VAULT(session, auth_token, vault_name, table_name, primary_key, pii_fields_delimited, vault_owner_email)

    # Convert the comma-separated list of PII fields into a list
    pii_columns = pii_fields_delimited.split(',')
    pii_columns.append(primary_key)    
    
    # Fetch data from the Snowflake table
    df = session.table(table_name)
    all_records = df.collect()
    batch_size = 25

    http_session = requests.Session()
    
    # Split records into batches of 25
    batches = [all_records[i:i + batch_size] for i in range(0, len(all_records), batch_size)]

    # Initialize dictionaries to store CASE expressions for each field
    case_expressions = {column.lower(): [] for column in pii_columns}
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
                "fields": {column: row[column] for column in pii_columns}
            }
            records.append(record)
            # Add the plaintext primary_key to the list for WHERE clause matching
            update_ids.append(str(row[primary_key]))
        
        body = {
            "records": records,
            "tokenization": True
        }

        url = f"https://ebfc9bee4242.vault.skyflowapis.com/v1/vaults/{vault_id}/" + table_name
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

    # Create or replace the stream and task for continuous tokenization
    session.sql(f"CREATE OR REPLACE STREAM SKYFLOW_PII_STREAM_{table_name} ON TABLE {table_name}").collect()
    session.sql(f"CREATE OR REPLACE TASK SKYFLOW_PII_STREAM_{table_name}_TASK WAREHOUSE = 'COMPUTE_WH' SCHEDULE = '1 MINUTE' WHEN SYSTEM$STREAM_HAS_DATA('SKYFLOW_PII_STREAM_{table_name}') AS CALL SKYFLOW_PROCESS_PII('{vault_id}', '{table_name}', '{primary_key}', '{pii_fields_delimited}')").collect()
    session.sql(f"ALTER TASK SKYFLOW_PII_STREAM_{table_name}_TASK RESUME").collect()

    return "Tokenization completed successfully"

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

# Global cache for storing the auth token and its expiry time
AUTH_TOKEN_CACHE = {
    'token': None,
    'expiry': None
}

def GENERATE_AUTH_TOKEN():
    # Check if a valid token is already in the cache
    if AUTH_TOKEN_CACHE['token'] and AUTH_TOKEN_CACHE['expiry'] > time.time():
        return AUTH_TOKEN_CACHE['token']
    
    # Existing code to generate a new token
    credentials = json.loads(_snowflake.get_generic_secret_string('cred'), strict=False)
    claims = {
       "iss": credentials["clientID"],
       "key": credentials["keyID"], 
       "aud": credentials["tokenURI"], 
       "exp": int(time.time()) + (3600), # JWT expires in Now + 60 minutes
       "sub": credentials["clientID"], 
    }
    signedJWT = jwt.encode(claims, credentials["privateKey"], algorithm='RS256')
    body = {
       'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
       'assertion': signedJWT,
    }
    tokenURI = credentials["tokenURI"]
    
    # Use the persistent session to send the request
    r = session.post(tokenURI, json=body)
    auth = json.loads(r.text)
    
    # Store the new token and its expiry time in the cache
    AUTH_TOKEN_CACHE['token'] = auth["accessToken"]
    AUTH_TOKEN_CACHE['expiry'] = time.time() + (3600) # Assuming the token expires in 1 hour
    
    return auth["accessToken"]

def GET_ACCOUNT_ID():
    return "<TODO: ACCOUNT_ID>"

    
def GET_WORKSPACE_ID(session, auth_token):
    url = f"https://manage.skyflowapis.com/v1/workspaces"
    headers = {
        "Authorization": "Bearer " + auth_token,
        "X-SKYFLOW-ACCOUNT-ID": GET_ACCOUNT_ID()
    }

    session = requests.Session()
    response = session.get(url, headers=headers)
    workspace_response = json.loads(response.text)
    
    return workspace_response["workspaces"][0]["ID"]  
    
def GET_VAULT_ID_BY_NAME(session, auth_token, vault_name):
    workspace_id = GET_WORKSPACE_ID(session, auth_token)
    url = f"https://manage.skyflowapis.com/v1/vaults?filterOps.name={vault_name}&workspaceID={workspace_id}"
    headers = {
        "Authorization": "Bearer " + auth_token,
        "X-SKYFLOW-ACCOUNT-ID": GET_ACCOUNT_ID()
    }

    session = requests.Session()
    response = session.get(url, headers=headers)
    vault_response = json.loads(response.text)
    
    return vault_response["vaults"][0]["ID"]   

@vectorized(input=pandas.DataFrame, max_batch_size=100)
def SKYFLOW_DETOKENIZE(token_df):
    auth_token = GENERATE_AUTH_TOKEN()
    vault_id = GET_VAULT_ID_BY_NAME(session, auth_token, 'SkyflowVault')

    # Convert the DataFrame Series into the token format needed for the detokenize call.
    token_values = token_df[0].apply(lambda x: {'token': x, 'redaction': 'PLAIN_TEXT'}).tolist()
    body = {
        'detokenizationParameters': token_values
    }

    url = f"https://ebfc9bee4242.vault.skyflowapis.com/v1/vaults/{vault_id}/detokenize"
    headers = { 'Authorization': 'Bearer ' + auth_token }

    # Use the persistent session to send the request
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
import time

def GENERATE_AUTH_TOKEN(session):
    credentials = json.loads(_snowflake.get_generic_secret_string('cred'), strict=False)
    
    # Create the claims object with the data in the creds object
    claims = {
       "iss": credentials["clientID"],
       "key": credentials["keyID"], 
       "aud": credentials["tokenURI"], 
       "exp": int(time.time()) + (3600), # JWT expires in Now + 60 minutes
       "sub": credentials["clientID"], 
    }
    # Sign the claims object with the private key contained in the creds object
    signedJWT = jwt.encode(claims, credentials["privateKey"], algorithm='RS256')

    body = {
       'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
       'assertion': signedJWT,
    }
    tokenURI = credentials["tokenURI"]

    session = requests.Session()
    r = session.post(tokenURI, json=body)
    auth = json.loads(r.text)
    
    return auth["accessToken"]

def GET_ACCOUNT_ID():
    return "<TODO: ACCOUNT_ID>"

def SKYFLOW_PROCESS_PII(session, vault_id, table_name, primary_key, pii_fields):
    # Load credentials and generate auth token
    credentials = json.loads(_snowflake.get_generic_secret_string('cred'), strict=False)
    auth_token = GENERATE_AUTH_TOKEN(credentials)

    # Convert primary_key and pii_fields to uppercase
    primary_key = primary_key.upper()
    pii_fields_list = [field.strip().upper() for field in pii_fields.split(',')]
    pii_fields_list.append(primary_key)

    # Skyflow static variables
    table_name_skyflow = table_name.lower()
    skyflow_account_id = GET_ACCOUNT_ID()
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

    session.sql(f'CREATE OR REPLACE STREAM SKYFLOW_PII_STREAM_{table_name} ON TABLE SKYFLOW_DEMO.PUBLIC.{table_name}').collect()
    return "Changes processed"

$$;
