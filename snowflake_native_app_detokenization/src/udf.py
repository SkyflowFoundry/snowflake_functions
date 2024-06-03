import simplejson as sjson
import jwt
import requests
import time
import pandas
from snowflake.snowpark import Session
from cachetools import cached, TTLCache
import _snowflake
from _snowflake import vectorized


def retry(attempts, delay, multiplier, callback):
    for i in range(attempts):
        result = callback()
        if result is not None:
            return result
        time.sleep(delay / 1000)  # Convert milliseconds to seconds
        delay *= multiplier  # Exponential backoff
    raise Exception(
        "Max retries exceeded. Error occurred generating bearer token")


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

    return signedJWT


@cached(cache=TTLCache(maxsize=1024, ttl=3600))
def get_bearer_token(credentials_hashable):
    credentials = dict(credentials_hashable) 

    # Get signed jwt
    signed_jwt = get_signed_jwt(credentials)

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

    except Exception as error:
        return None


def init_app(session: Session, config) -> str:
    """
      Initializes function API endpoints with access to the secret and API integration.

      Args:
          session (Session): An active session object for authentication and communication.
          config (Any): The configuration settings for the connector.

      Returns:
          str: A status message indicating the result of the provisioning process.
      """
    service_account_credential = config['service_account_credential']
    vault_url = config['vault_url']
    external_access_integration_name = config['external_access_integration_name']

    alter_function_sql = f'''
    ALTER FUNCTION code_schema.detokenize(varchar) SET
    SECRETS = ('token' = {service_account_credential}, 'vault_url' = {vault_url})
    EXTERNAL_ACCESS_INTEGRATIONS = ({external_access_integration_name})'''

    session.sql(alter_function_sql).collect()

    return 'Skyflow Detokenization App initialized'


@vectorized(input=pandas.DataFrame, max_batch_size=25)
def detokenize(token):
    """
      detokenize performs a detokenize call for a specified vault and retrieves the data.

      Args:
        token (varchar): The tokens to be detokenized.

      Returns:
          string: A string representing the original data associated with the provided token.
      """

    credentials = sjson.loads(
        _snowflake.get_generic_secret_string('token'),
        strict=False)

    credentials_hashable = tuple(sorted(credentials.items()))

    bearer_token_response = retry(
        3, 100, 2, lambda: get_bearer_token(credentials_hashable)
    )
    bearer_token = sjson.loads(bearer_token_response, strict=False)

    skyflow_vault_url = _snowflake.get_generic_secret_string('vault_url')

    token_values = token[0].apply(
        lambda x: {
            'token': x,
            'redaction': 'PLAIN_TEXT'}).tolist()
    body = {
        'detokenizationParameters': token_values
    }
    # url should be of the form
    # https://identifier.vault.skyflowapis.com/v1/vaults/{vaultID}/detokenize
    url = skyflow_vault_url + "/detokenize"
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        "Authorization": "Bearer " + bearer_token['accessToken']
    }

    try:
        session = requests.Session()
        response = retry(
            5, 100, 2, lambda: session.post(url, json=body, headers=headers))
        response.raise_for_status()

        response_as_json = sjson.loads(response.text)

        data = []

        for record in response_as_json['records']:
            data.append(record['value'])

    except requests.exceptions.HTTPError as error:
        return (f"A HTTP error occurred while performing detokenize: {error}")

    except Exception as error:
        return (f"An error occurred while performing detokenize: {error}")

    return pandas.Series(data)
