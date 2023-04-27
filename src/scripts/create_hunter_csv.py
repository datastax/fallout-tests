"""
This Python file seeks to create a csv file for Hunter to detect performance
regressions (if any).
"""

import glob
import json
import logging
import os
import re
from typing import List, Tuple

import pandas as pd

from constants import (DATE_DIR_REGEX_PATTERN, DICT_OF_RENAMED_COLS,
                       FALLOUT_TESTS_COL_NAME, HUNTER_CSV_PROJ_DIR,
                       HUNTER_FILE_FMT, HUNTER_PREFIX, LIST_OF_COLS_TO_EXTRACT,
                       LIST_OF_CSV_NAMES, LWT_TEST_RUN_EXEC_TIME,
                       LWT_TESTS_NAMES, NIGHTLY_RESULTS_DIR, PROSPECTIVE_MODE,
                       SUBSTR_TESTS_NAMES, TUPLE_SUPPORTED_TESTS,
                       TWO_GIT_SHA_SUFFIX)
from utils import (add_cols_to_metrics_df, add_suffix_to_col, get_error_log,
                   get_git_sha_for_cassandra, get_git_sha_for_fallout_tests,
                   get_relevant_dict, save_df_to_csv)


def extract_metrics_df(read_rel_dict: dict, write_rel_dict: dict) -> pd.DataFrame:
    """
    Extracts relevant metrics from 2 dictionaries and convert them into a dataframe.

    Args:
        read_rel_dict: dict
                    A dictionary of values for read phase.
        write_rel_dict: dict
                    A dictionary of values for write phase.

    Returns:
            Relevant metrics as a dataframe.
    """

    read_write_dict = read_rel_dict, write_rel_dict
    raw_read_write_df = pd.DataFrame.from_dict(read_write_dict)
    extracted_col_from_raw_df = raw_read_write_df[LIST_OF_COLS_TO_EXTRACT]

    extracted_col_from_raw_df = extracted_col_from_raw_df.rename(
        columns=DICT_OF_RENAMED_COLS)
    return extracted_col_from_raw_df


def create_hunter_df(combined_columns_df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates a dataframe with only one row of values for both read and write with their respective column names.

    Args:
        combined_columns_df: pd.DataFrame
                    A dataframe pertaining to the read and write rows.

    Returns:
            A dataframe with one row for both read and write values.
    """

    read_row = pd.DataFrame(combined_columns_df.loc[0]).T
    write_row = pd.DataFrame(combined_columns_df.loc[1]).T

    read_row = add_suffix_to_col(read_row, '.read')
    write_row = add_suffix_to_col(write_row, '.write')

    horiz_concat_df_two_rows = pd.concat([read_row, write_row], axis=0)
    horiz_concat_df_one_blended_row = pd.DataFrame(
        horiz_concat_df_two_rows.loc[0].combine_first(
            horiz_concat_df_two_rows.loc[1])
    ).T
    return horiz_concat_df_one_blended_row


def get_paths_to_json(path_w_spec_date: str) -> List[List[str]]:
    """
    Get the paths to the json files of the performance results, e.g., one for each
    type of performance test (100/1000/10000 partitions, fixed or rated).

    Args:
        path_w_spec_date: str
                        The test run's date-related path to the json file.

    Returns:
            A list of paths (List[str]) to the json files of interest.
    """

    # Gets a list of lists of performance-report.json filename for each LWT tests on that given date
    paths_to_each_json = []
    for lwt_test in LWT_TESTS_NAMES:
        each_json_paths_list = glob.glob(
            f"{path_w_spec_date}{os.sep}{lwt_test}{os.sep}{'**/performance-report.json'}",
            recursive=True
        )
        paths_to_each_json.append(each_json_paths_list)

    return paths_to_each_json


def generate_hunter_df(json_paths: List[str]) -> pd.DataFrame:
    """
    Generate the dataframe of test type-specific performance results and the
    corresponding csv file to be fed to hunter.

    Args:
        json_paths: List[str]
                    A list of json paths with performance results and related metrics.

    Returns:
            A dataframe of test type-specific performance results.
    """

    if len(json_paths) == 0:
        logging.error(
            "The 'json_paths' is empty; "
            "thus, an empty dataframe is being returned."
        )
        return pd.DataFrame()

    # Access performance-report.json
    with open(json_paths[0]) as json_file:
        data = json.load(json_file)
        if len(data['stats']) == 0:
            logging.error(
                "The 'stats' list in the 'data' dictionary is empty; "
                "please ensure that the test has output the statistics into it."
            )
            return pd.DataFrame()

        # Get only read/write-result-success dictionaries.
        read_dict = get_relevant_dict(data, 'read')
        write_dict = get_relevant_dict(data, 'write')
        # Get dataframe with relevant read/write column names, rename columns to shorten their names.
        raw_hunter_metrics_df = extract_metrics_df(read_dict, write_dict)
        raw_hunter_metrics_df['opRate'] = raw_hunter_metrics_df['opRate'].str.rstrip(
            ' op/sec')
        for col_name in raw_hunter_metrics_df.columns:
            if col_name not in ['totalOps', 'opRate']:
                raw_hunter_metrics_df[col_name] = raw_hunter_metrics_df[col_name].str.rstrip(
                    ' ms')
        # Get date from the json path (regardless of its positional index) and add time for compatibility with hunter.
        list_of_items_from_json_path = json_paths[0].split(os.sep)

        date_val = ''
        for item_in_json in list_of_items_from_json_path:
            matched_pattern = re.search(DATE_DIR_REGEX_PATTERN, item_in_json)
            if matched_pattern:
                date_val = matched_pattern.group()

        # As at 11pm UTC
        date_val_w_time = f"{date_val.replace('_', '-')}{' '}{LWT_TEST_RUN_EXEC_TIME}{' +0000'}"

        # Get the Git shas for the Cassandra and fallout-tests repos for auditability
        cassandra_git_short_hash = get_git_sha_for_cassandra(date_val)
        fallout_tests_git_short_hash = get_git_sha_for_fallout_tests(date_val)

        combined_read_write_df = create_hunter_df(raw_hunter_metrics_df)

        hunter_df_full = add_cols_to_metrics_df(
            date_val_w_time,
            cassandra_git_short_hash,
            fallout_tests_git_short_hash,
            combined_read_write_df
        )
        return hunter_df_full


def get_hunter_df_w_test_type(json_paths: List[str]) -> Tuple[pd.DataFrame, str]:
    """
    Get the dataframe to feed to hunter with performance results and the corresponding test type (e.g., 100/1000/10000
    partitions and either 'fixed' or 'rated').

    Args:
        json_paths: List[str]
                    A list of json paths with performance results and related metrics.

    Returns:
            A tuple with the dataframe with performance results and the corresponding test type.
    """
    hunter_df_out = generate_hunter_df(json_paths)

    if len(json_paths) == 0 or hunter_df_out.empty:
        logging.error(
            "Either the 'json_paths' or the hunter dataframe is empty; "
            "thus, empty dataframe and string are being returned."
        )
        return pd.DataFrame(), ''

    # Get the test type based on a tuple of supported tests (regardless of their positional index)
    list_of_items_from_json_path = json_paths[0].split(os.sep)
    test_type_str = ''
    for item_in_json in list_of_items_from_json_path:
        if item_in_json.startswith(TUPLE_SUPPORTED_TESTS):
            test_type_str = item_in_json

    if test_type_str != '' and not hunter_df_out.empty:
        return hunter_df_out, test_type_str


if __name__ == '__main__':
    # Get sorted list of dates from output folders with performance tests
    nightly_result_dates = os.listdir(NIGHTLY_RESULTS_DIR)
    nightly_result_dates.sort()

    # Set to False if running this for the first time, then re-run and set to True.
    is_case_prospective = PROSPECTIVE_MODE
    # Get path of previous csv files from retrospective
    if is_case_prospective:
        csv_file_paths = []
        for csv_file_name in LIST_OF_CSV_NAMES:
            csv_file_paths.append(
                f'{HUNTER_CSV_PROJ_DIR}{os.sep}{csv_file_name}{HUNTER_FILE_FMT}')
        # Read the csv files of retrospective run
        hunter_df_fixed_100, hunter_df_rated_100, hunter_df_fixed_1000, \
            hunter_df_rated_1000, hunter_df_fixed_10000, hunter_df_rated_10000 = \
            (pd.read_csv(csv_file_path) for csv_file_path in csv_file_paths)

        # Get date from the latest test run
        test_input_date = nightly_result_dates[-1]

        # Separated by _ to be compared wrt 'test_input_date'
        last_date_from_csv = hunter_df_fixed_100['time'].iloc[-1].split(' ')[
            0].replace('-', '_')

        if test_input_date == last_date_from_csv:
            raise ValueError(f"The test results for the date '{test_input_date}' were already run and summarised "
                             f"in the csv file. Please ensure the date considered is not in the csv file and it is "
                             f"past the latest date in the csv file.")

        # Get path to the latest test run
        path_w_date = f'{NIGHTLY_RESULTS_DIR}{os.sep}{test_input_date}'
        path_to_each_test_json = get_paths_to_json(path_w_date)

        list_of_hunter_df = []
        list_of_type_of_tests = []
        for test_json_path in path_to_each_test_json:
            hunter_df, type_of_test = get_hunter_df_w_test_type(test_json_path)
            if type_of_test != '' and not hunter_df.empty:
                list_of_hunter_df.append(hunter_df)
                list_of_type_of_tests.append(type_of_test)

        # Get concatenated hunter dfs across all test types currently supported
        concat_hunter_data_frames = {
            SUBSTR_TESTS_NAMES[0]: hunter_df_fixed_100,
            SUBSTR_TESTS_NAMES[1]: hunter_df_rated_100,
            SUBSTR_TESTS_NAMES[2]: hunter_df_fixed_1000,
            SUBSTR_TESTS_NAMES[3]: hunter_df_rated_1000,
            SUBSTR_TESTS_NAMES[4]: hunter_df_fixed_10000,
            SUBSTR_TESTS_NAMES[5]: hunter_df_rated_10000
        }

        for i, test_type in enumerate(list_of_type_of_tests):
            for test_partition, hunter_df in concat_hunter_data_frames.items():
                if test_partition in test_type:
                    concat_df = pd.concat([hunter_df, list_of_hunter_df[i]])
                    concat_hunter_data_frames[test_partition] = concat_df
                    break
                else:
                    get_error_log(test_type)

        # Save two versions of the df: 1) with the Cassandra git shas only (for hunter), 2) with two git shas (of the
        # Cassandra and fallout-tests repos) for auditability
        for i, test_name in enumerate(LWT_TESTS_NAMES):
            for substr_test_name in SUBSTR_TESTS_NAMES:
                if substr_test_name in test_name:
                    df_w_two_git_sha = concat_hunter_data_frames[substr_test_name]
                    save_df_to_csv(
                        df_w_two_git_sha,
                        f'{HUNTER_CSV_PROJ_DIR}{os.sep}{HUNTER_PREFIX}{test_name}{TWO_GIT_SHA_SUFFIX}{HUNTER_FILE_FMT}'
                    )
                    df_w_one_git_sha = df_w_two_git_sha.drop(
                        FALLOUT_TESTS_COL_NAME, axis=1)
                    save_df_to_csv(
                        df_w_one_git_sha,
                        f'{HUNTER_CSV_PROJ_DIR}{os.sep}{HUNTER_PREFIX}{test_name}{HUNTER_FILE_FMT}'
                    )

    else:
        hunter_df_100_fixed, hunter_df_100_rated, hunter_df_1000_fixed, hunter_df_1000_rated, hunter_df_10000_fixed, \
            hunter_df_10000_rated, types_of_tests = [], [], [], [], [], [], []
        for input_date in nightly_result_dates:
            for test_json_path in get_paths_to_json(f'{NIGHTLY_RESULTS_DIR}{os.sep}{input_date}'):
                hunter_df, type_of_test = get_hunter_df_w_test_type(
                    test_json_path)
                if type_of_test:
                    types_of_tests.append(type_of_test)
                if SUBSTR_TESTS_NAMES[0] in type_of_test and not hunter_df.empty:
                    hunter_df_100_fixed.append(hunter_df)
                elif SUBSTR_TESTS_NAMES[1] in type_of_test and not hunter_df.empty:
                    hunter_df_100_rated.append(hunter_df)
                elif SUBSTR_TESTS_NAMES[2] in type_of_test and not hunter_df.empty:
                    hunter_df_1000_fixed.append(hunter_df)
                elif SUBSTR_TESTS_NAMES[3] in type_of_test and not hunter_df.empty:
                    hunter_df_1000_rated.append(hunter_df)
                elif SUBSTR_TESTS_NAMES[4] in type_of_test and not hunter_df.empty:
                    hunter_df_10000_fixed.append(hunter_df)
                elif SUBSTR_TESTS_NAMES[5] in type_of_test and not hunter_df.empty:
                    hunter_df_10000_rated.append(hunter_df)
                else:
                    get_error_log(type_of_test)

        # Create a list of lists of dfs from 6 lists of dfs
        hunter_dfs = [hunter_df_100_fixed, hunter_df_100_rated, hunter_df_1000_fixed, hunter_df_1000_rated,
                      hunter_df_10000_fixed, hunter_df_10000_rated]

        # Create a concatenated df from the above list of lists of dfs and extracts it into 6 different dfs
        for i, hunter_df in enumerate(hunter_dfs):
            hunter_dfs[i] = pd.concat(hunter_df)
        hunter_df_100_fixed, hunter_df_100_rated, hunter_df_1000_fixed, hunter_df_1000_rated, hunter_df_10000_fixed, \
            hunter_df_10000_rated = hunter_dfs

        # Save two versions of the df: 1) with the Cassandra git shas only (for hunter), 2) with two git shas (of the
        # Cassandra and fallout-tests repos) for auditability
        unique_types_of_tests = pd.Series(types_of_tests).unique()
        subset_names_w_dfs = {
            SUBSTR_TESTS_NAMES[0]: hunter_df_100_fixed,
            SUBSTR_TESTS_NAMES[1]: hunter_df_100_rated,
            SUBSTR_TESTS_NAMES[2]: hunter_df_1000_fixed,
            SUBSTR_TESTS_NAMES[3]: hunter_df_1000_rated,
            SUBSTR_TESTS_NAMES[4]: hunter_df_10000_fixed,
            SUBSTR_TESTS_NAMES[5]: hunter_df_10000_rated
        }

        for i, unique_test_type in enumerate(unique_types_of_tests):
            for subset_names, hunter_df in subset_names_w_dfs.items():
                if subset_names in unique_test_type:
                    hunter_file_name = f'{HUNTER_PREFIX}{unique_test_type}'
                    df = subset_names_w_dfs[subset_names]
                    save_df_to_csv(df,
                                   f'{HUNTER_CSV_PROJ_DIR}{os.sep}'
                                   f'{hunter_file_name}{TWO_GIT_SHA_SUFFIX}{HUNTER_FILE_FMT}')
                    df_w_one_git_sha = df.drop(FALLOUT_TESTS_COL_NAME, axis=1)
                    save_df_to_csv(df_w_one_git_sha,
                                   f'{HUNTER_CSV_PROJ_DIR}{os.sep}'
                                   f'{hunter_file_name}{HUNTER_FILE_FMT}')
