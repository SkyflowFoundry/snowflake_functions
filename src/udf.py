import simplejson as sjson
import jwt
import requests
import time
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Row
import _snowflake


def get_signed_jwt(credentials):
    # Create the claims object with the data in the creds object
    claims = {
        "iss": credentials["clientID"],
        "key": credentials["keyID"],
        "aud": credentials["tokenURI"],
        "exp": int(time.time()) + (3600),  # JWT expires in Now + 60 minutes
        "sub": credentials["clientID"],
    }
    # Sign the claims object with the private key contained in the creds object
    signedJWT = jwt.encode(
        claims,
        credentials["privateKey"],
        algorithm='RS256')

    return signedJWT, credentials


def get_bearer_token(signed_jwt, credentials):
    # Request body parameters
    body = {
        'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
        'assertion': signed_jwt,
    }

    token_uri = credentials["tokenURI"]

    try:
        response = requests.post(url=token_uri, json=body)
        response.raise_for_status()
        return response.text

    except requests.exceptions.HTTPError as error:
        return (
            f"A HTTP error occurred while generating bearer token: {error}")

    except Exception as error:
        return (f"An error occurred generating bearer token: {error}")


def init_app(session: Session, config) -> str:
    """
      Initializes function API endpoints with access to the secret and API integration.

      Args:
          session (Session): An active session object for authentication and communication.
          config (Any): The configuration settings for the connector.

      Returns:
          str: A status message indicating the result of the provisioning process.
      """
    secret_name = config['secret_name']
    external_access_integration_name = config['external_access_integration_name']

    alter_function_sql = f'''
    ALTER FUNCTION code_schema.skyflowDetokenize(string, string) SET
    SECRETS = ('token' = {secret_name})
    EXTERNAL_ACCESS_INTEGRATIONS = ({external_access_integration_name})'''

    session.sql(alter_function_sql).collect()

    return 'Skyflow Detokenization App initialized'


def skyflowDetokenize(vault_url, token):
    """
      skyflowDetokenize performs a detokenize call for a specified vault and retrieves the data.

      Args:
          vault_url (string): The API URL of the vault where the tokenized data is stored. Must be of the form: https://identifier.vault.skyflowapis.com/v1/vaults/{vaultID}
          token (string): The tokens to be detokenized.

      Returns:
          string: A string representing the original data associated with the provided token.
      """

    credentials = sjson.loads(
        _snowflake.get_generic_secret_string('token'),
        strict=False)
    jwt_token, creds = get_signed_jwt(credentials)
    bearer_token = sjson.loads(
        get_bearer_token(
            jwt_token,
            creds),
        strict=False)

    batch_size = 25
    tokens = []
    tokens.append(token)
    output = []
    batched_tokens = [tokens[i:i + batch_size]
                      for i in range(0, len(tokens), batch_size)]

    for batch in batched_tokens:
        detokenization_parameters = [{"token": token} for token in batch]
        body = {"detokenizationParameters": detokenization_parameters}

        url = vault_url + "/detokenize"
        headers = {
            "Authorization": "Bearer " + bearer_token['accessToken']
        }

        try:
            session = requests.Session()
            response = session.post(url, json=body, headers=headers)
            response.raise_for_status()

            response_as_json = sjson.loads(response.text)
            for record in response_as_json['records']:
                output.append(record['value'])

        except requests.exceptions.HTTPError as error:
            return (f"A HTTP error occurred while performing detokenize: {error}")

        except Exception as error:
            return (f"An error occurred while performing detokenize: {error}")

    return sjson.dumps(output)
