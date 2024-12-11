-- Create customers table
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(255),
    address VARCHAR(255),
    lifetime_purchase_amount VARCHAR(255),
    customer_since VARCHAR(255)
);

-- Ensure the http extension is loaded in the current database
CREATE EXTENSION IF NOT EXISTS http;

-- Create the function that will tokenize each column using the Skyflow API
CREATE OR REPLACE FUNCTION skyflowTokenizeCustomer()
RETURNS TRIGGER AS $$
DECLARE
    response jsonb;
    tokens_record jsonb;
    records_input jsonb := '[]'::jsonb;
    -- Define the columns you want to tokenize
    col_values text[];
    i int;
    -- Corresponding token fields
    name_token text;
    email_token text;
    phone_token text;
    address_token text;
    lpa_token text; -- lifetime_purchase_amount as text
    cs_token text;  -- customer_since as text
BEGIN
    /*
      Gather all columns that you want to tokenize.
      Each column will become one entry in the `records` array for Skyflow.
      Convert non-text fields to text as needed.
    */
    col_values := ARRAY[
        NEW.name::text,
        NEW.email::text,
        NEW.phone::text,
        NEW.address::text,
        NEW.lifetime_purchase_amount::text,
        NEW.customer_since::text
    ];

    -- Build the JSON array of records for Skyflow.
    -- Each entry: {"fields":{"pii":"<column_value>"}}
    FOR i IN 1..array_length(col_values, 1) LOOP
        records_input := records_input || jsonb_build_object(
            'fields', jsonb_build_object('pii', col_values[i])
        );
    END LOOP;

    -- Make a single HTTP POST request to Skyflow
    SELECT content::jsonb INTO response
    FROM http((
        'POST',
        'https://ebfc9bee4242.vault.skyflowapis.com/v1/vaults/nfb4008c34e84c42af2a13aecf1f4e85/pii',  -- Request URL
        ARRAY[
            http_header('X-SKYFLOW-ACCOUNT-ID', 'e5ef2e2ffd44443b81cdf79f9dc7e8dd'),
            http_header('Authorization', 'Bearer sky-df675-f9b64918a87b4b38ad7f11680bb5c7cc'),
            http_header('Content-Type', 'application/json'),
            http_header('Accept', 'application/json')
        ],
        'application/json',
        jsonb_build_object(
            'records', records_input,
            'tokenization', true
        )::text
    )::http_request);

    /*
      The response will look like this:
      {
        "records": [
          {
            "skyflow_id": "...",
            "tokens": { "pii": "token_for_col1" }
          },
          {
            "skyflow_id": "...",
            "tokens": { "pii": "token_for_col2" }
          },
          ...
        ]
      }

      We inserted one record per column, so the response->'records' array
      will align with the order we submitted the columns.
    */

    -- Extract tokens in the same order as col_values
    name_token := (response->'records'->0->'tokens'->>'pii');
    email_token := (response->'records'->1->'tokens'->>'pii');
    phone_token := (response->'records'->2->'tokens'->>'pii');
    address_token := (response->'records'->3->'tokens'->>'pii');
    lpa_token := (response->'records'->4->'tokens'->>'pii');
    cs_token := (response->'records'->5->'tokens'->>'pii');

    -- Update the NEW record with tokenized values
    NEW.name := name_token;
    NEW.email := email_token;
    NEW.phone := phone_token;
    NEW.address := address_token;
    NEW.lifetime_purchase_amount := (lpa_token);
    NEW.customer_since := (cs_token);

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create a BEFORE INSERT OR UPDATE trigger to call the function for each row
CREATE TRIGGER tokenize_customer_pii
    BEFORE INSERT OR UPDATE ON customers
    FOR EACH ROW
    EXECUTE FUNCTION skyflowTokenizeCustomer();