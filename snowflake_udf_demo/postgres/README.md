# Skyflow for PostgreSQL Demo

A simple demonstration of using Skyflow's tokenization with PostgreSQL using BEFORE INSERT/UPDATE triggers.

## Files
* `postgres_setup.sql` - Creates the customers table and sets up the tokenization trigger
* `postgres_demo.sql` - Demonstrates the tokenization functionality with sample data
* `setup.sh` - Script to create or destroy the PostgreSQL environment

## Quick Start
1. Create the environment:
```bash
./setup.sh create
```
This will:
- Install PostgreSQL 17 via Homebrew
- Install PostgreSQL HTTP extension (for making API calls)
- Start the PostgreSQL service
- Create the skyflow_demo database
- Run the setup SQL file
- Add PostgreSQL to your PATH temporarily

2. Add PostgreSQL to your PATH permanently:
```bash
# Add this line to your ~/.zshrc or ~/.bash_profile:
export PATH="/opt/homebrew/opt/postgresql@17/bin:$PATH"

# Then reload your shell configuration:
source ~/.zshrc  # or source ~/.bash_profile
```

3. Run the demo:
```bash
psql -d skyflow_demo -f postgres_demo.sql
```

4. To remove everything when done:
```bash
./setup.sh destroy
```
This will:
- Stop the PostgreSQL service
- Uninstall PostgreSQL and extensions
- Remove all data directories
- Remind you to remove the PATH addition from your shell configuration

## Connection Details
Default PostgreSQL connection settings:
```json
{
  "host": "localhost",
  "port": 5432,
  "database": "skyflow_demo",
  "user": "postgres",
  "password": "",
  "ssl": false
}
```

Note: When installing PostgreSQL via Homebrew on macOS, it creates a superuser with your system username and no password by default. You can connect using:
```bash
psql skyflow_demo
```

## Manual Setup (if needed)
1. Install PostgreSQL and HTTP extension:
```bash
brew install postgresql@17
brew install pex
brew services start postgresql@17
```

2. Add PostgreSQL to your PATH:
```bash
export PATH="/opt/homebrew/opt/postgresql@17/bin:$PATH"
```

3. Create database and run setup:
```bash
createdb skyflow_demo
psql -d skyflow_demo -f postgres_setup.sql
```

## Features
- Automatic tokenization of PII data (name, email, phone, address) on insert/update
- Demonstrates both single row and batch operations
- Uses PostgreSQL's trigger system for seamless integration
- Direct HTTP requests to Skyflow API using PostgreSQL's HTTP extension

## Example Usage
```sql
-- Insert a customer (PII fields will be automatically tokenized)
INSERT INTO customers (
    name, 
    email, 
    phone, 
    address, 
    lifetime_purchase_amount, 
    customer_since
) VALUES (
    'John Smith',
    'john@example.com',
    '555-222-5555',
    '123 Fake Street NY NY 10019',
    5000,
    '2020-01-01'
);

-- Query the data to see tokenized values
SELECT * FROM customers;
```

## Requirements
- Homebrew (for installation)
- PostgreSQL 17
- PostgreSQL HTTP extension (pex)

## Script Commands
```bash
# Create environment
./setup.sh create

# Destroy environment
./setup.sh destroy

# Show usage
./setup.sh
```

## Troubleshooting
If you see "command not found" errors for PostgreSQL commands:
1. Ensure PostgreSQL is in your PATH:
```bash
export PATH="/opt/homebrew/opt/postgresql@17/bin:$PATH"
```
2. Add the above line to your shell configuration file (~/.zshrc or ~/.bash_profile) for permanent access
