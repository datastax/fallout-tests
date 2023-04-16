"""
This Python file hosts utility-based functions used to support the
creation of a csv for Hunter.
"""

import logging
import os
import subprocess
from datetime import datetime, timedelta
from typing import Tuple

import git
import pandas as pd
from constants import (CASSANDRA_COL_NAME, CASSANDRA_PROJ_DIR,
                       FALLOUT_TESTS_COL_NAME, FALLOUT_TESTS_PROJ_DIR,
                       FIXED_100_CSV_NAME, FMT_TIME, FMT_Y_D_M, FMT_Y_M_D,
                       HUNTER_CSV_PROJ_DIR, LWT_TEST_RUN_EXEC_TIME,
                       PROSPECTIVE_MODE)


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


def cd_into_proj_dir(project_dir: str = HUNTER_CSV_PROJ_DIR) -> None:  # pragma: no cover
    """
    Change directory based on input path.

    Args:
        project_dir: str
                    The destination directory.
    """
    os.chdir(project_dir)


def get_commit_hash_cass_fall_tests(
        sorted_date: str = None, is_prospective: bool = PROSPECTIVE_MODE
) -> Tuple[str, str]:  # pragma: no cover
    """
    Get the Git commit hash of the Apache Cassandra and the DataStax's repos given a date and
    whether the analysis is prospective.

    Args:
        sorted_date: str
                    A sorted date in the format as per the constant FMT_Y_D_M.
        is_prospective: bool
                    Whether the analysis is prospective (True by default).

    Returns:
        A tuple with the Git commit hash of the Apache Cassandra and the DataStax's repos.
    """
    if is_prospective:
        # cd into cloned Cassandra to then get its latest git SHA
        cd_into_proj_dir(CASSANDRA_PROJ_DIR)
        g = git.cmd.Git(CASSANDRA_PROJ_DIR)
        g.pull('origin', 'trunk')
        cassandra_git_hash = get_git_sha_prospective()

        # cd into cloned fallout-tests to then get its latest git SHA
        cd_into_proj_dir(FALLOUT_TESTS_PROJ_DIR)
        g = git.cmd.Git(FALLOUT_TESTS_PROJ_DIR)
        g.pull('origin', 'main')
        fallout_tests_git_hash = get_git_sha_prospective()
    else:
        # cd into cloned Cassandra to then get its git SHA corresponding to the date of interest
        cd_into_proj_dir(CASSANDRA_PROJ_DIR)
        g = git.cmd.Git(CASSANDRA_PROJ_DIR)
        g.pull('origin', 'trunk')
        cassandra_git_hash = get_git_sha_retrospective(sorted_date)

        # cd into cloned fallout-tests to then get its git SHA corresponding to the date of interest
        cd_into_proj_dir(FALLOUT_TESTS_PROJ_DIR)
        g = git.cmd.Git(FALLOUT_TESTS_PROJ_DIR)
        g.pull('origin', 'main')
        fallout_tests_git_hash = get_git_sha_retrospective(sorted_date)

    return cassandra_git_hash, fallout_tests_git_hash


def get_git_sha_prospective(date_fmt: str = FMT_Y_D_M) -> str:  # pragma: no cover
    """
    Return the Git short hash of a repo given a specific date.

    Args:
        date_fmt: str
                 The chosen date format.

    Returns:
            The Git short hash of the repo of interest (based on the current working directory).

    Note:
        This assumes that the current working directory is where the repo of interest was cloned. If not, cd into it
        before executing this function.
    """

    today_date = datetime.today()
    logging.debug("Retrieving Git sha for today's date: ", today_date)

    yesterday_s_date = get_yesterday_date(date_fmt)
    two_days_ago = (today_date - timedelta(days=2)).strftime(date_fmt)

    gitsha = subprocess.check_output(
        f"git log --since={yesterday_s_date} --pretty=format:'%h %ci' -1", shell=True
    ).decode('ascii').strip()
    if gitsha == '':
        logging.warning(f'No commit was found for the date {yesterday_s_date}; '
                        f'so, the latest commit for the previous day is taken.')
        # The line below gets the commit hash for yesterday.
        gitsha = subprocess.check_output(
            f"git log --since={two_days_ago} --pretty=format:'%h %ci' -1", shell=True
        ).decode('ascii').strip()
        logging.info(f"The git commit '{gitsha}' from yesterday was taken.")
    else:
        logging.info(f"Today's latest commit was found.")
    split_gitsha = gitsha.split()

    # The Git SHAs are already sorted from the most to the least recent, thus getting the
    # first item from the list (expecting the hash as the first element based on the
    # --pretty=format:'%h %ci'" above).
    if len(split_gitsha) > 0:
        final_sha = split_gitsha[0]
        logging.debug(
            f"Retrieved Git sha {final_sha} for today's date '{today_date}'.")
        return final_sha
    # If len == 0, get the latest Git sha from the last row in the previous csv file (any test types as Git shas are the
    # same regardless of them).
    else:
        df_retrospective = pd.read_csv(
            f'{HUNTER_CSV_PROJ_DIR}{os.sep}{FIXED_100_CSV_NAME}')
        # Get current working directory ('pwd' in bash)
        cwd = os.getcwd()
        latest_git_sha = ''
        if cwd == CASSANDRA_PROJ_DIR:
            latest_git_sha = df_retrospective[CASSANDRA_COL_NAME].iloc[-1]
        elif cwd == FALLOUT_TESTS_PROJ_DIR:
            latest_git_sha = df_retrospective[FALLOUT_TESTS_COL_NAME].iloc[-1]
        else:
            logging.error(f"The directory '{cwd} is not one of the two expected Git repos ('{CASSANDRA_PROJ_DIR}' or "
                          f"{FALLOUT_TESTS_PROJ_DIR}). Please provide either one or the other, and retry.")
        return latest_git_sha


def get_git_sha_retrospective(given_date: str) -> str:  # pragma: no cover
    """
    Return the Git short hash of a repo given a specific date but on or before 11pm UTC (the time of the run on the VM).
    If no commits were found for a given date, the Git sha of the latest commit would be taken.

    Args:
        given_date: str
                    The date of the day beyond that of the current test run (to be able to get the current
                    test run's date).

    Returns:
            The Git short hash of the repo of interest (based on the current working directory).

    Note:
        This assumes that the current working directory is where the repo of interest was cloned. If not, cd into it
        before executing this function.
    """

    logging.debug('Retrieving Git sha for the date: ', given_date)

    given_date_list = given_date.split('_')

    # Y-M-D for git compatibility with VM
    given_date_formatted = datetime(
        int(given_date_list[0]), int(
            given_date_list[2]), int(given_date_list[1])
    )

    gitsha_until = subprocess.check_output(
        f"git log --until='{given_date_formatted}' --pretty=format:'%h %ci'", shell=True
    ).decode('ascii').strip()
    gitsha_until_w_commas = gitsha_until.replace('\n', ',').replace(' ', ',')
    gitsha_until_list = gitsha_until_w_commas.split(',')

    gitsha_local = subprocess.check_output(
        f"git log --until='{given_date_formatted}' --pretty=format:'%h %cd' --date=local",
        shell=True
    ).decode('ascii').strip()
    gitsha_local_w_commas = gitsha_local.replace('\n', ',').replace(' ', ',')
    gitsha_local_list = gitsha_local_w_commas.split(',')

    # Remove time zones (e.g., '-0700') from the list 'gitsha_until_list'
    counter_until = 0
    for _ in gitsha_until_list:
        while counter_until < len(gitsha_until_list) - 3:
            counter_until += 3
            gitsha_until_list.pop(counter_until)

    # Get list of local times from the list 'gitsha_local_list'
    counter_local = 0
    list_of_local_times = []
    counter_loop = 0
    for _ in gitsha_local_list:
        while counter_local < len(gitsha_local_list) - 4:
            if counter_loop < 1:
                counter_local += 4
            else:
                counter_local += 6
            list_of_local_times.append(gitsha_local_list[counter_local])
            counter_loop += 1

    # Replace previous time in the list 'gitsha_until_list' (without time zone as popped/removed above) with
    # the correct local time extracted in the list 'list_of_local_times' above
    counter_loop = 0
    counter_until_replace = 0
    for local_time in list_of_local_times:
        if counter_loop < 1:
            counter_until_replace += 2
            gitsha_until_list[counter_until_replace] = local_time
            counter_until_replace += 3
        else:
            gitsha_until_list[counter_until_replace] = local_time
            counter_until_replace += 3
        counter_loop += 1

    # Get the final git sha
    counter_date = 0
    for _ in gitsha_until_list:
        while not counter_date == len(gitsha_until_list) - 1:
            counter_date += 1
            if given_date == gitsha_until_list[counter_date]:
                corresp_time = gitsha_until_list[counter_date + 1]
                # Get git sha of the same date but on or before 11pm UTC
                if datetime.strptime(corresp_time, FMT_TIME) <= datetime.strptime(LWT_TEST_RUN_EXEC_TIME, FMT_TIME):
                    corresp_git_sha = gitsha_until_list[counter_date - 1]
                    logging.debug(
                        f"The Git commit sha {corresp_git_sha} was retrieved for the date '{given_date}'."
                    )
                    return corresp_git_sha
                # As the time is after 11pm UTC, get git sha of the same date but before 11pm UTC
                else:
                    while not counter_date == len(gitsha_until_list) - 3:
                        counter_date += 3
                        if given_date == gitsha_until_list[counter_date]:
                            corresp_time = gitsha_until_list[counter_date + 1]
                            if datetime.strptime(corresp_time, FMT_TIME) <= datetime.strptime(
                                    LWT_TEST_RUN_EXEC_TIME, FMT_TIME
                            ):
                                corresp_git_sha = gitsha_until_list[counter_date - 1]
                                logging.debug(
                                    f"The Git commit sha {corresp_git_sha} was retrieved for the date '{given_date}'."
                                )
                                return corresp_git_sha
            # Get the latest git sha (of a different/previous date)
            else:
                corresp_git_sha = gitsha_until_list[counter_date - 1]
                logging.debug(
                    f"No commits were found for the date '{given_date}'. "
                    f"Thus, the latest Git commit sha {corresp_git_sha} was retrieved."
                )
                return corresp_git_sha


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


def get_yesterday_date(date_fmt: str = FMT_Y_M_D) -> str:
    """
    Get yesterday's date in a chosen format.

    Args:
        date_fmt: str
                The chosen date format.

    Returns:
            Yesterday's date (str) in the chosen format.
    """
    yesterday_date = (datetime.today() - timedelta(days=1)).strftime(date_fmt)
    return yesterday_date
