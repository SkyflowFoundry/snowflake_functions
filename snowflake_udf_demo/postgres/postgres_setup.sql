-- Create the customers table
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(255),
    address VARCHAR(255),
    lifetime_purchase_amount VARCHAR(255),
    customer_since VARCHAR(255)
);

-- Insert sample data
INSERT INTO customers (
    name, 
    email, 
    phone, 
    address, 
    lifetime_purchase_amount, 
    customer_since
)
SELECT 
    'Customer ' || generate_series AS name,
    'customer' || generate_series || '@example.com' AS email,
    '555-' || LPAD(generate_series::text, 3, '0') || '-9999' AS phone,
    generate_series || ' Batch Street, NY NY 10019' AS address,
    (random() * 10000)::numeric(10,2)::text AS lifetime_purchase_amount,
    (current_date - (random() * 365)::integer)::text AS customer_since
FROM generate_series(1, 50);



-- Ensure the http extension is available
CREATE EXTENSION IF NOT EXISTS http;

-- Create the tokenization_config table if it does not exist
CREATE TABLE IF NOT EXISTS tokenization_config (
    table_name text NOT NULL,
    column_name text NOT NULL,
    tokenize boolean NOT NULL DEFAULT false,
    PRIMARY KEY (table_name, column_name)
);

-- Create or replace the skyflowTokenize function
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
    -- Convert NEW row to JSON
    new_json := to_jsonb(NEW);

    -- Get columns to tokenize from tokenization_config
    SELECT array_agg(c.column_name ORDER BY c.ordinal_position)
    INTO cols
    FROM information_schema.columns c
    JOIN tokenization_config t
      ON t.table_name = c.table_name
     AND t.column_name = c.column_name
     AND t.tokenize = true
    WHERE c.table_name = TG_TABLE_NAME::text
      AND c.table_schema = 'public';

    IF cols IS NULL THEN
        RETURN NEW; -- No tokenization needed
    END IF;

    -- Build records_input for Skyflow
    FOR i IN 1..array_length(cols, 1) LOOP
        colname := cols[i];
        records_input := records_input || jsonb_build_object(
            'fields', jsonb_build_object('pii', new_json->>colname)
        );
    END LOOP;

    IF jsonb_array_length(records_input) = 0 THEN
        RETURN NEW; -- No values to tokenize
    END IF;

    -- Call Skyflow API with hardcoded values
    SELECT content::jsonb INTO response
    FROM http((
        'POST',
        'https://ebfc9bee4242.vault.skyflowapis.com/v1/vaults/nfb4008c34e84c42af2a13aecf1f4e85/pii',
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

    -- Update NEW with tokenized values
    FOR i IN 1..array_length(cols, 1) LOOP
        colname := cols[i];
        final_json := jsonb_set(
            final_json,
            ARRAY[colname],
            to_jsonb((response->'records'->(i-1)->'tokens'->>'pii'))
        );
    END LOOP;

    NEW := jsonb_populate_record(NEW, final_json);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create or replace the trigger function for the given table
-- Note: The trigger will be created by tokenize_table at the end.

-- The tokenize_table function sets tokenization config, truncates, and re-inserts data
CREATE OR REPLACE FUNCTION tokenize_table(p_table_name text, p_columns text)
RETURNS void AS $$
DECLARE
    col_list text[];
    i int;
    col text;

BEGIN
    -- Parse and trim column list
    col_list := string_to_array(p_columns, ',');
    FOR i IN 1..array_length(col_list,1) LOOP
        col_list[i] := trim(col_list[i]);
    END LOOP;

    -- Update tokenization_config to enable tokenization for these columns
    FOREACH col IN ARRAY col_list LOOP
        EXECUTE format($sql$
            INSERT INTO tokenization_config (table_name, column_name, tokenize)
            VALUES (%L, %L, true)
            ON CONFLICT (table_name, column_name)
            DO UPDATE SET tokenize = excluded.tokenize
        $sql$, p_table_name, col);
    END LOOP;

    -- Create or replace the trigger for skyflowTokenize on the specified table
    EXECUTE format($trig$
        CREATE OR REPLACE TRIGGER tokenize_pii
        BEFORE INSERT OR UPDATE ON %I
        FOR EACH ROW
        EXECUTE FUNCTION skyflowTokenize()
    $trig$, p_table_name);

    -- Truncate/Insert Approach:
    -- 1. Create a temporary backup of the table
    EXECUTE format('CREATE TEMP TABLE %I_backup AS SELECT * FROM %I', p_table_name, p_table_name);

    -- 2. Truncate the original table
    EXECUTE format('TRUNCATE TABLE %I', p_table_name);

    -- 3. Re-insert all rows from the backup to trigger tokenization
    EXECUTE format('INSERT INTO %I SELECT * FROM %I_backup', p_table_name, p_table_name);

    -- Now all rows have been re-inserted and thus tokenized by the trigger.
END;
$$ LANGUAGE plpgsql;