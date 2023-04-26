"""
This Python file hosts utility-based functions used to support creating
and/or sending an email with performance regressions detected by hunter.
"""

import json
from typing import List

import boto3
from botocore.exceptions import ClientError

from src.scripts.cassandra_email.constants import REGION_NAME, SECRET_NAME


def get_aws_secrets() -> dict:
    """
    Get secrets (username and password) from the AWS Secret Manager.

    Returns:
            A dictionary with AWS secrets.
    """
    secret_name = SECRET_NAME
    region_name = REGION_NAME

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secrets_dict = json.loads(get_secret_value_response['SecretString'])
    return secrets_dict


def get_list_of_dict_from_json(file_path: str) -> List[dict]:
    """
    Get list of dictionaries, e.g., results from hunter on performance regressions, from a json file.

    Args:
        file_path: str
                The json file path with the file name and extension (.json).

    Returns:
            A list of dictionaries (one dict for each line in the json file).
    """

    hunter_result_list_of_dicts = []
    with open(file_path, 'r') as json_file:
        for hunter_result_str in json_file:
            # Convert each line to a dict
            hunter_result_dict = json.loads(hunter_result_str)
            hunter_result_list_of_dicts.append(hunter_result_dict)
    return hunter_result_list_of_dicts
