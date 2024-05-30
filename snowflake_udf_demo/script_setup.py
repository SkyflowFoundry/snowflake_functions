import re
import json

def read_json_file(file_path):
    # Read the JSON file and return its content as a string
    with open(file_path, 'r') as file:
        return file.read()

def read_config_file(file_path):
    # Read the configuration file and return its content
    with open(file_path, 'r') as file:
        return file.read()

def parse_vault_details(config_content):
    # Parse the configuration content into a dictionary
    vault_details = dict(re.findall(r'(\w+)="(.*?)"', config_content))
    # Strip 'https://' from the VAULT_URL if present
    if 'VAULT_URL' in vault_details:
        vault_details['VAULT_URL'] = vault_details['VAULT_URL'].replace('https://', '')
    return vault_details

def main():
    # Read and parse the vault details configuration file
    vault_config_content = read_config_file('vault_details.cfg')
    vault_details = parse_vault_details(vault_config_content)

    # Load the service account credentials JSON file as a dictionary
    with open('credentials.json', 'r') as file:
        service_account_credentials = json.load(file)

    # Extract the clientID from the service account credentials
    client_id = service_account_credentials.get('clientID', '')

    # Read the content of the SQL file
    with open('skyflow_udf_setup.sql', 'r') as file:
        content = file.read()

    # Replace tokens with vault details
    for key, value in vault_details.items():
        content = re.sub(f'<TODO: {key.upper()}>', value, content)

    # Replace the token for Service Account Credentials with the raw string
    service_account_credentials_raw = json.dumps(service_account_credentials)
    content = re.sub(r'<TODO: SERVICE_ACCOUNT_CREDENTIALS>', service_account_credentials_raw, content)

    # Replace the token for Service Account ID with the clientID
    content = re.sub(r'<TODO: SERVICE_ACCOUNT_ID>', client_id, content)

    # Write the updated content back to the file
    with open('skyflow_udf_setup.sql', 'w') as file:
        file.write(content)

    print('Token replacement complete.')

if __name__ == '__main__':
    main()