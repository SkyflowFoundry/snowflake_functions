#!/bin/bash

function create() {
    echo "Installing PostgreSQL 17..."
    brew install postgresql@17

    echo "Installing build dependencies..."
    brew install curl
    brew install pkg-config

    # Add PostgreSQL to PATH
    echo "Adding PostgreSQL to PATH..."
    export PATH="/opt/homebrew/opt/postgresql@17/bin:$PATH"

    echo "Starting PostgreSQL service..."
    brew services start postgresql@17

    # Wait for PostgreSQL to be ready
    echo "Waiting for PostgreSQL to be ready..."
    sleep 5

    echo "Installing PostgreSQL HTTP extension..."
    git clone https://github.com/pramsey/pgsql-http.git
    cd pgsql-http
    make USE_PGXS=1
    make USE_PGXS=1 install
    cd ..
    rm -rf pgsql-http

    echo "Creating database..."
    createdb skyflow_demo

    echo "Running setup SQL..."
    psql -d skyflow_demo -f postgres_setup.sql

    echo "Setup complete!"
    echo
    echo "Connection Details:"
    echo "Host: localhost"
    echo "Port: 5432"
    echo "Database: skyflow_demo"
    echo "User: $USER (your system username)"
    echo "Password: (none)"
    echo "SSL: disabled"
    echo
    echo "Important: Add PostgreSQL to your PATH permanently by adding this line to your ~/.zshrc or ~/.bash_profile:"
    echo 'export PATH="/opt/homebrew/opt/postgresql@17/bin:$PATH"'
    echo
    echo "You can connect using:"
    echo "psql skyflow_demo"
    echo
    echo "Or run the demo with:"
    echo "psql -d skyflow_demo -f postgres_demo.sql"
}

function destroy() {
    echo "Stopping PostgreSQL service..."
    brew services stop postgresql@17

    echo "Uninstalling PostgreSQL..."
    brew uninstall postgresql@17

    echo "Removing PostgreSQL data directory..."
    rm -rf /opt/homebrew/var/postgresql@17

    echo "Cleanup complete!"
    echo
    echo "Note: You may want to remove the PostgreSQL PATH from your ~/.zshrc or ~/.bash_profile if you added it"
    echo 'The line to remove is: export PATH="/opt/homebrew/opt/postgresql@17/bin:$PATH"'
}

# Check command line argument
case "$1" in
    "create")
        create
        ;;
    "destroy")
        destroy
        ;;
    "recreate")
        destroy
        create
        ;;
    *)
        echo "Usage: $0 {create|destroy|recreate}"
        echo
        echo "Commands:"
        echo "  create  - Install and configure PostgreSQL with Skyflow demo"
        echo "  destroy - Stop and remove PostgreSQL installation"
        echo "  recreate - Perform a destroy and a then a create"
        exit 1
        ;;
esac
