-- Variables for setting up app and testing integration
SET DATABASE_NAME = '<REPLACE_ME>';
SET SCHEMA_NAME = '<REPLACE_ME>';
SET APP_PACKAGE_NAME = 'skyflow_app_package';
SET APP_NAME = 'skyflow_app';
SET SERVICE_ACCOUNT_CREDENTIAL = $DATABASE_NAME || '.' || $SCHEMA_NAME || '.skyflow_vault_secret';
SET VAULT_URL = $DATABASE_NAME || '.' || $SCHEMA_NAME || '.skyflow_vault_url';

-- Skyflow Vault URL
-- Should be of the form
-- https://identifier.vault.skyflowapis.com/v1/vaults/vaultID
CREATE OR REPLACE SECRET IDENTIFIER($VAULT_URL)
        TYPE = GENERIC_STRING
        SECRET_STRING = '<REPLACE_ME>';

USE DATABASE IDENTIFIER($DATABASE_NAME);
USE SCHEMA IDENTIFIER($SCHEMA_NAME);

-- Skyflow Service Account credentials.json contents
CREATE OR REPLACE SECRET IDENTIFIER($SERVICE_ACCOUNT_CREDENTIAL)
        TYPE = GENERIC_STRING
        SECRET_STRING = '<REPLACE_ME>';
    
-- Grant network access to the Skyflow APIs
CREATE OR REPLACE NETWORK RULE skyflow_apis_network_rule
 MODE = EGRESS
 TYPE = HOST_PORT
 VALUE_LIST = ('manage.skyflowapis.com', '<REPLACE_ME_OF_THE_TYPE_ebfc9bee4242.vault.skyflowapis.com>');

-- Create a network integration based on the network rule
SET CREATE_INTEGRATION = 'CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION skyflow_external_access_integration
ALLOWED_NETWORK_RULES = (skyflow_apis_network_rule)
ALLOWED_AUTHENTICATION_SECRETS = (''' || $SERVICE_ACCOUNT_CREDENTIAL || ''', ''' || $VAULT_URL || ''')
ENABLED = TRUE';

SELECT $CREATE_INTEGRATION;
EXECUTE IMMEDIATE $CREATE_INTEGRATION;

-- Drop the native app in case it already exists
DROP APPLICATION IF EXISTS IDENTIFIER($APP_NAME);

-- Create the native app
CREATE APPLICATION IDENTIFIER($APP_NAME)
  FROM APPLICATION PACKAGE IDENTIFIER($APP_PACKAGE_NAME)
  USING '@skyflow_app_package.stage_content.skyflow_app_stage';

GRANT USAGE ON DATABASE TEST TO APPLICATION IDENTIFIER($APP_NAME); -- Replace TEST with your database_name
GRANT USAGE ON SCHEMA TEST.TEST_SCHEMA TO APPLICATION IDENTIFIER($APP_NAME); -- Replace TEST.TEST_SCHEMA with your database_name.schema_name
GRANT USAGE ON INTEGRATION skyflow_external_access_integration TO APPLICATION IDENTIFIER($APP_NAME);
GRANT READ ON SECRET IDENTIFIER($SERVICE_ACCOUNT_CREDENTIAL) TO APPLICATION IDENTIFIER($APP_NAME);
GRANT READ ON SECRET IDENTIFIER($VAULT_URL) TO APPLICATION IDENTIFIER($APP_NAME);

-- Initialize the Skyflow app
CALL skyflow_app.code_schema.init_app(PARSE_JSON('{
        "service_account_credential": "' || $SERVICE_ACCOUNT_CREDENTIAL || '",
        "vault_url": "' || $VAULT_URL || '",
        "external_access_integration_name": "skyflow_external_access_integration",
    }'));

USE DATABASE IDENTIFIER($DATABASE_NAME);
USE SCHEMA IDENTIFIER($SCHEMA_NAME);

-- Detokenize
-- Replace column_name and table_name with your column name and table name which contains the tokens to be detokenized
SELECT column_name, skyflow_app.code_schema.detokenize(column_name) from table_name;