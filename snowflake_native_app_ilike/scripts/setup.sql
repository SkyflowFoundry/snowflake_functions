CREATE APPLICATION ROLE app_public;
CREATE SCHEMA IF NOT EXISTS core;
GRANT USAGE ON SCHEMA core TO APPLICATION ROLE app_public;

CREATE OR ALTER VERSIONED SCHEMA code_schema;
GRANT USAGE ON SCHEMA code_schema TO APPLICATION ROLE app_public;

CREATE OR REPLACE PROCEDURE code_schema.init_app(config variant)
  RETURNS string
  LANGUAGE python
  runtime_version = '3.8'
  packages = ('snowflake-snowpark-python', 'pyjwt', 'cryptography', 'requests', 'simplejson')
  imports = ('/src/udf.py')
  handler = 'udf.init_app';

GRANT USAGE ON PROCEDURE code_schema.init_app(variant) TO APPLICATION ROLE app_public;

CREATE OR REPLACE FUNCTION code_schema.search(vault_url string, table_name string, column_name string, name_to_search string)
  RETURNS string
  LANGUAGE python
  runtime_version = '3.8'
  packages = ('snowflake-snowpark-python', 'pyjwt', 'cryptography', 'requests', 'simplejson')
  imports = ('/src/udf.py')
  handler = 'udf.search';

GRANT USAGE ON FUNCTION code_schema.search(string, string, string, string) TO APPLICATION ROLE app_public;