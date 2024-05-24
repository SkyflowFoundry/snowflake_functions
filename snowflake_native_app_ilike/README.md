> [!CAUTION]
> Work in progress. Not officially supported by Skyflow or Snowflake. Consider this alpha software which may have issues.

# Skyflow Native App

This project contains the source for a Snowflake Native App that supports ILIKE Skyflow queries using a UDF.

## Installation

To test the application, perform the following:

- In Snowflake's Snowsight, run the `sql/deploy.sql` file.
- In Snowflake, upload the application files to the `SKYFLOW_APP_STAGE` maintaining the file structure (`src/udf.py` ; `scripts/setup.sql` ; `manifest.yml`).
- In Skyflow Studio, create a quickstart vault and note the vault URL. Create a service account for this vault and save the credentials.json. 
- In Snowsight, open `sql/install.sql` from the application source and update the `<REPLACE_ME>`
placeholder values.
- Execute all statements in `sql/install.sql`.
