"""
This Python file hosts utility-based functions used to support the
creation of a csv for Hunter.
"""

import glob
import json
import logging
import os
from typing import List

import boto3
import pandas as pd
from botocore.exceptions import ClientError

from src.scripts.constants import (CASSANDRA_COL_NAME, FALLOUT_TESTS_COL_NAME,
                                   FALLOUT_TESTS_SHA_PROJ_DIR, LWT_TESTS_NAMES,
                                   NEWLINE_SYMBOL, NIGHTLY_RESULTS_DIR,
                                   REGION_NAME, SECRET_NAME)


def add_cols_to_metrics_df(
        date_time: str,
        cassandra_commit: str,
        fallout_tests_commit: str,
        extract_col_from_raw_df: pd.DataFrame
) -> pd.DataFrame:  # pragma: no cover
    """
    Add relevant values (date and time, Git commit hash of Cassandra and fallout-tests repo)
    to a dataframe of performance-related metrics.

    Args:
        date_time: str
                    A date and time representing when a test was executed.
        cassandra_commit: str
                    The Git commit hash of the Apache Cassandra repository.
        fallout_tests_commit: str
                    The Git commit hash of the DataStax's fallout-tests repository.
        extract_col_from_raw_df: pd.Dataframe
                    A dataframe of the metrics columns extracted from the json file.

    Returns:
            A dataframe of the original metrics with time and commit columns added
    """

    combined_columns_df = extract_col_from_raw_df.copy()
    combined_columns_df['time'] = date_time
    combined_columns_df[CASSANDRA_COL_NAME] = cassandra_commit
    combined_columns_df[FALLOUT_TESTS_COL_NAME] = fallout_tests_commit
    return combined_columns_df


def add_suffix_to_col(phase_df: pd.DataFrame, phase: str) -> pd.DataFrame:
    """
    Appends appropriate suffix, i.e., '.read' or '.write', to each column name.

    Args:
        phase_df: pd.DataFrame
                    A phase-related dataframe.
        phase: str
            The phase of interest, i.e., 'read' or 'write'.

    Returns:
            A dataframe with columns appended with the appropriate suffix.
    """

    new_cols = []
    for col in phase_df.columns:
        new_cols.append(col + phase)

    phase_df.columns = new_cols
    return phase_df


def get_git_sha_for_cassandra(input_date: str) -> str:  # pragma: no cover
    """
    Get the Git sha of the Cassandra repo for a given date from a logs.txt file.

    Args:
        input_date: str
                    A date for which to get a Git sha of the Cassandra repo.

    Returns:
            The Git sha (str) of the Cassandra repo for a given date.
    """

    log_files_list = []
    list_of_log_file_path = []
    for _ in LWT_TESTS_NAMES:
        log_files_list = glob.glob(
            f"{NIGHTLY_RESULTS_DIR}{os.sep}{input_date}{os.sep}{'**/performance-tester-dc1-default-sts-0/logs.txt'}",
            recursive=True
        )
        for log_file_path in log_files_list:
            list_of_log_file_path.append(log_file_path)

    git_sha_list = []
    for logs in log_files_list:
        with open(logs, 'r') as text:
            content = ' '.join(text.readlines())
            git_sha = content.split('Git SHA: ')[1].split(NEWLINE_SYMBOL)[0]
            git_sha_list.append(git_sha)

    # Get the first non-empty Git sha as the final one, as at times one subtest may not yield results, whilst
    # another one (or all others) may.
    final_cass_sha = ''
    for i in range(len(git_sha_list)):
        if git_sha_list[i] != '':
            final_cass_sha = git_sha_list[i]
            break
    return final_cass_sha


def get_git_sha_for_fallout_tests(input_date: str) -> str:  # pragma: no cover
    """
    Get the Git sha of the fallout-tests repo for a given date from a fallout-tests_git_sha.log file.

    Args:
        input_date: str
                    A date for which to get a Git sha of the fallout-tests repo.

    Returns:
            The Git sha (str) of the fallout-tests repo for a given date.
    """

    fallout_tests_log_file_list = glob.glob(
        f"{FALLOUT_TESTS_SHA_PROJ_DIR}{os.sep}{input_date}{os.sep}{'fallout-tests_git_sha.log'}",
        recursive=True
    )

    if len(fallout_tests_log_file_list) == 0:
        logging.error(
            "The 'fallout_tests_log_file_list' is empty; "
            "thus, an empty string (instead of the fallout-tests Git sha) is being returned."
        )
        return ''

    with open(fallout_tests_log_file_list[0], 'r') as text:
        content = ' '.join(text.readlines())
        # The 1st element is the Git sha, the 2nd is the datetime
        final_fallout_tests_sha = content.split(',')[0]
    return final_fallout_tests_sha


def get_error_log(test_type: str) -> None:  # pragma: no cover
    logging.error(f"The type of test '{test_type}' is not supported; please ensure you use either "
                  f"of the following numbers of partitions (either fixed or rated): 100, 1000, "
                  f"or 10000.")


def get_relevant_dict(dict_of_dicts: dict, test_phase: str) -> dict:
    """
    Get the relevant dictionary (e.g., read- or write-related) from a dictionary of dictionaries.

    Args:
        dict_of_dicts: dict
                    A dictionary of dictionaries, each of which hosts the results from a test run (e.g., read or write).
        test_phase: str
            The test phase of interest, i.e., 'read' or 'write'.

    Returns:
            The relevant dictionary.
    """
    relevant_dict = {}
    for dict_test_run in dict_of_dicts['stats']:
        if 'result-success' in dict_test_run['test']:
            if test_phase in dict_test_run['test']:
                relevant_dict.update(dict_test_run)
    return relevant_dict


def save_df_to_csv(input_df: pd.DataFrame, path_to_output: str) -> None:  # pragma: no cover
    """
    Save an input dataframe to a csv file in a chosen filename.

    Args:
        input_df: pd.DataFrame
                An input dataframe.
        path_to_output: str
                The filename where to save the input dataframe.
    """
    input_df.to_csv(path_to_output, index=False)


def get_aws_secrets() -> dict:  # pragma: no cover
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
    # Get and return the last dictionary
    hunter_result_dict = hunter_result_list_of_dicts[-1]
    hunter_result_list_of_dict = [hunter_result_dict]
    return hunter_result_list_of_dict
