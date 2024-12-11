-- Create table
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

CREATE OR REPLACE FUNCTION skyflowTokenize()
RETURNS TRIGGER AS $$
DECLARE
    response jsonb;
    records_input jsonb := '[]'::jsonb;
    final_json jsonb := '{}';

    cols text[];
    colname text;
    new_json jsonb;
    i int;
BEGIN
    -- Convert NEW record to JSON for dynamic field access
    new_json := to_jsonb(NEW);

    -- Retrieve all column names from information_schema in the order they appear in the table
    SELECT array_agg(column_name ORDER BY ordinal_position)
    INTO cols
    FROM information_schema.columns
    WHERE table_name = TG_TABLE_NAME::text
      AND table_schema = 'public'
      -- If you want to skip certain columns from tokenization (e.g. the primary key), add:
      AND column_name <> 'customer_id';

    -- Build the JSON array of records for Skyflow, one record per column
    FOR i IN 1..array_length(cols, 1) LOOP
        colname := cols[i];
        records_input := records_input || jsonb_build_object(
            'fields', jsonb_build_object('pii', new_json->>colname)
        );
    END LOOP;

    -- Make a single HTTP POST request to Skyflow
    SELECT content::jsonb INTO response
    FROM http((
        'POST',
        'https://ebfc9bee4242.vault.skyflowapis.com/v1/vaults/nfb4008c34e84c42af2a13aecf1f4e85/pii',  -- Replace with your URL
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

    -- Reconstruct final_json with tokenized values
    FOR i IN 1..array_length(cols, 1) LOOP
        colname := cols[i];
        final_json := jsonb_set(
            final_json,
            ARRAY[colname],
            to_jsonb((response->'records'->(i-1)->'tokens'->>'pii'))
        );
    END LOOP;

    -- Populate NEW record with the tokenized values from final_json
    NEW := jsonb_populate_record(NEW, final_json);

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create a BEFORE INSERT OR UPDATE trigger to call the function for each row
CREATE TRIGGER tokenize_pii
    BEFORE INSERT OR UPDATE ON customers
    FOR EACH ROW
    EXECUTE FUNCTION skyflowTokenize();