import re
import json

def read_json_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()

def read_config_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()

def parse_vault_details(config_content):
    vault_details = dict(re.findall(r'(\w+)="(.*?)"', config_content))
    if 'VAULT_URL' in vault_details:
        vault_details['VAULT_URL'] = vault_details['VAULT_URL'].replace('https://', '')
    return vault_details

def get_user_input(prompt_message, pattern):
    # Prompt the user for input
    user_input = input(prompt_message)
    # Validate the input to ensure it follows the specified pattern
    if not re.match(pattern, user_input):
        raise ValueError("Invalid input. Please follow the specified pattern.")
    return user_input

def main():
    # Get user input for suffix
    suffix = get_user_input("Enter a suffix for the database name (alphanumeric and underscores only): ", r'^[A-Za-z0-9_]+$')
    
    # Get user input for ACCOUNT_ID
    account_id = get_user_input("Enter the ACCOUNT_ID (32-character alphanumeric): ", r'^[a-zA-Z0-9]{32}$')

    with open('credentials.json', 'r') as file:
        service_account_credentials = json.load(file)

    client_id = service_account_credentials.get('clientID', '')

    # List of SQL files to update
    sql_files = ['skyflow_udf_setup.sql', 'skyflow_udf_demo.sql', 'skyflow_udf_reset.sql']

    for sql_file in sql_files:
        with open(sql_file, 'r') as file:
            content = file.read()

        # Replace the database name and other resources with the user input, ensuring a single underscore before the suffix
        content = re.sub(r'<TODO: SUFFIX>', f'{suffix}', content)
        
        # Replace ACCOUNT_ID placeholder with user input
        content = re.sub(r'<TODO: ACCOUNT_ID>', account_id, content)

        service_account_credentials_raw = json.dumps(service_account_credentials)
        content = re.sub(r'<TODO: SERVICE_ACCOUNT_CREDENTIALS>', service_account_credentials_raw, content)
        content = re.sub(r'<TODO: SERVICE_ACCOUNT_ID>', client_id, content)

        with open(sql_file, 'w') as file:
            file.write(content)

    print('Token replacement complete.')

if __name__ == '__main__':
    main()
