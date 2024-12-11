-- Step 1: Insert sample customer data
INSERT INTO customers (
    name, 
    email, 
    phone, 
    address, 
    lifetime_purchase_amount, 
    customer_since
) VALUES 
    ('John Smith', 'john@example.com', '555-222-5555', '123 Fake Street NY NY 10019', 5000000, '2020-01-01'),
    ('Harry Truman', 'harry@example.com', '555-333-5555', '234 Fake Street NY NY 10019', 6000000, '2023-01-01'),
    ('Sally Field', 'sally@example.com', '555-444-5555', '345 Fake Street NY NY 10019', 9900000, '2022-01-01');

-- View tokenized customer data

SELECT tokenize_table('customers', 'email,name,phone');

SELECT * FROM customers;

SELECT name, SKYFLOW_DETOKENIZE((email)) FROM customers;

-- Step 2: Test UPDATE functionality
UPDATE customers 
SET 
    email = 'john.smith@newdomain.com',
    phone = '555-999-8888',
    address = '999 Updated Street NY NY 10019'
WHERE customer_id = 1 OR customer_id = 2;

-- View updated customer data
SELECT * FROM customers WHERE customer_id = 1;

-- Step 3: Test batch insert
INSERT INTO customers (
    name, 
    email, 
    phone, 
    address, 
    lifetime_purchase_amount, 
    customer_since
)
SELECT 
    'Customer ' || generate_series as name,
    'customer' || generate_series || '@example.com' as email,
    '555-' || LPAD(generate_series::text, 3, '0') || '-9999' as phone,
    generate_series || ' Batch Street, NY NY 10019' as address,
    (random() * 10000)::numeric(10,2) as lifetime_purchase_amount,
    (current_date - (random() * 365)::integer) as customer_since
FROM generate_series(1, 50);

-- View all customer data
SELECT * FROM customers ORDER BY customer_id;
