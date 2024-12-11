-- Create customers table
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(50),
    address TEXT,
    lifetime_purchase_amount DECIMAL(10, 2),
    customer_since DATE
);

-- Enable http extension
CREATE EXTENSION IF NOT EXISTS http;

-- Create function to tokenize customer PII
CREATE OR REPLACE FUNCTION skyflowTokenizeCustomer()
RETURNS TRIGGER AS $$
DECLARE
    response JSON;
    tokens JSON;
BEGIN
    -- Insert a record into the Skyflow API
    SELECT status, content::json INTO response 
    FROM http_post(
        'https://ebfc9bee4242.vault.skyflowapis.com/v1/vaults/nfb4008c34e84c42af2a13aecf1f4e85/pii',
        jsonb_build_object(
            'records', jsonb_build_array(
                jsonb_build_object(
                    'fields', jsonb_build_object(
                        'customer_id', NEW.customer_id,
                        'name', NEW.name,
                        'email', NEW.email,
                        'phone', NEW.phone,
                        'address', NEW.address
                    )
                )
            ),
            'tokenization', true
        )::text,
        ARRAY[
            http_header('X-SKYFLOW-ACCOUNT-ID', 'e5ef2e2ffd44443b81cdf79f9dc7e8dd'),
            http_header('Authorization', 'Bearer sky-df675-f9b64918a87b4b38ad7f11680bb5c7cc'),
            http_header('Content-Type', 'application/json'),
            http_header('Accept', 'application/json')
        ]
    );

    -- Parse the response JSON
    tokens := response->'records'->0->'tokens';

    -- Update the NEW record with the values from the response
    NEW.name := tokens->>'name';
    NEW.email := tokens->>'email';
    NEW.phone := tokens->>'phone';
    NEW.address := tokens->>'address';

    -- Return the NEW record to be inserted into the table
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create BEFORE INSERT trigger for customers
CREATE TRIGGER tokenize_customer_pii
    BEFORE INSERT OR UPDATE ON customers
    FOR EACH ROW
    EXECUTE FUNCTION skyflowTokenizeCustomer();
