> [!CAUTION]
> Work in progress. Not officially supported by Skyflow or Snowflake. Consider this beta software which may have issues.

# Skyflow Native App

This project contains the source for a Snowflake Native App that supports detokenization of tokens present in Snowflake using a UDF.

## Installation

To test the application, perform the following:

- In Snowflake's Snowsight, run the `sql/deploy.sql` file.
- In Snowflake, upload the application files to the `SKYFLOW_APP_STAGE` maintaining the file structure (`src/udf.py` ; `scripts/setup.sql` ; `manifest.yml`).
- In Skyflow Studio, create a quickstart vault and note the vault URL. Create a service account for this vault and save the credentials.json. 
- In Snowflake create a table and insert the tokens that you want to detokenize:
```
-- Create a table which contains a column which stores your tokens 
create table table_name (column_name string); -- Replace table_name and column_name with your table name and column name
insert into table_name (column_name) values ('token_value'); -- Replace 'token value' with your token value
-- select count(column_name) from table_name;
-- select * from table_name;
```
- In Snowsight, open `sql/install.sql` from the application source and update the `<REPLACE_ME>`
placeholder values.
- Execute all statements in `sql/install.sql`.
