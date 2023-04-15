"""
This Python file seeks to create a csv file for Hunter to detect performance
regressions (if any).
"""

import glob
import json
import logging
import os
from datetime import datetime, timedelta
from typing import List, Tuple

import pandas as pd
from constants import (DICT_OF_RENAMED_COLS, FIXED_100_CSV_NAME,
                       FIXED_1000_CSV_NAME, FIXED_10000_CSV_NAME, FMT_Y_D_M,
                       FMT_Y_M_D, HUNTER_FILE_FMT, LIST_OF_COLS_TO_EXTRACT,
                       LWT_TEST_RUN_EXEC_TIME, LWT_TESTS_NAMES,
                       NIGHTLY_RESULTS_DIR, PROSPECTIVE_MODE,
                       RATED_100_CSV_NAME, RATED_1000_CSV_NAME,
                       RATED_10000_CSV_NAME)
from utils import (add_cols_to_metrics_df, add_suffix_to_col, cd_into_proj_dir,
                   get_commit_hash_cass_fall_tests, get_error_log,
                   get_relevant_dict, get_yesterday_date)


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


def save_df_to_csv(input_df: pd.DataFrame, path_to_output: str) -> None:
    """
    Save an input dataframe to a csv file in a chosen filename.

    Args:
        input_df: pd.DataFrame
                An input dataframe.
        path_to_output: str
                The filename where to save the input dataframe.
    """

    input_df.to_csv(path_to_output, index=False)


def get_paths_to_six_json(path_w_spec_date: str) -> List[str]:
    """
    Get the paths to the six json files of the performance results, i.e., one for each
    type of performance test (100/1000/10000 partitions, fixed or rated).

    Args:
        path_w_spec_date: str
                        The test run's date-related path to the json file.

    Returns:
            A list of paths (List[str]) to the six json files of interest.
    """

    # Gets a list of lists of performance-report.json filename for each LWT tests on that given date
    paths_to_each_json = []
    for lwt_test in LWT_TESTS_NAMES:
        each_json_paths_list = glob.glob(
            f"{path_w_spec_date}{os.sep}{lwt_test}/{'**/performance-report.json'}",
            recursive=True
        )
        paths_to_each_json.append(each_json_paths_list)

    return paths_to_each_json


def generate_hunter_df(json_paths: List[str], is_prospective: bool = PROSPECTIVE_MODE) -> pd.DataFrame:
    """
    Generate the dataframe of test type-specific performance results and the
    corresponding csv file to be fed to hunter based on whether the analysis is prospective.

    Args:
        json_paths: List[str]
                    A list of json paths with performance results and related metrics.
        is_prospective: bool
                    Whether the analysis is prospective (True by default).

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
        # Get date based on path and add time for compatibility with hunter.
        date_val = json_paths[0].split(os.sep)[4]
        # As at 11pm UTC
        date_val_w_time = f"{date_val.replace('_', '-')}{' '}{LWT_TEST_RUN_EXEC_TIME}{' +0000'}"

        # Prospective case is when running hunter nightly; retrospective is when running it
        # on already output dates-related folders of performance results (one-off analysis).
        if is_prospective:
            cassandra_git_short_hash, fallout_tests_git_short_hash = \
                get_commit_hash_cass_fall_tests(is_prospective=is_prospective)

        else:
            # Swap month and day to match expected format for the git log command below
            list_of_elems = date_val.split('_')
            # Plus one day to then get the date's Git sha (as using 'until' in git log command)
            date_sorted = datetime(
                int(list_of_elems[0]), int(
                    list_of_elems[1]), int(list_of_elems[2])
            ) + timedelta(days=1)
            date_sorted_y_d_m = date_sorted.strftime(FMT_Y_D_M)
            cassandra_git_short_hash, fallout_tests_git_short_hash = \
                get_commit_hash_cass_fall_tests(
                    date_sorted_y_d_m, is_prospective)

        # cd back into hunter_csv
        cd_into_proj_dir()

        combined_read_write_df = create_hunter_df(raw_hunter_metrics_df)

        hunter_df_full = add_cols_to_metrics_df(
            date_val_w_time,
            cassandra_git_short_hash,
            fallout_tests_git_short_hash,
            combined_read_write_df
        )
        return hunter_df_full


def get_hunter_df_w_test_type(json_paths: List[str], is_prospective: bool = PROSPECTIVE_MODE) -> Tuple[pd.DataFrame, str]:
    """
    Get the dataframe to feed to hunter with performance results and the corresponding test type (e.g., 100/1000/10000
    partitions and either 'fixed' or 'rated') based on whether the analysis is prospective.

    Args:
        json_paths: List[str]
                    A list of json paths with performance results and related metrics.
        is_prospective: bool
                    Whether the analysis is prospective (True by default).

    Returns:
            A tuple with the dataframe with performance results and the corresponding test type.
    """
    hunter_df_out = generate_hunter_df(json_paths, is_prospective)

    if len(json_paths) == 0 or hunter_df_out.empty:
        logging.error(
            "Either the 'json_paths' or the hunter dataframe is empty; "
            "thus, empty dataframe and string are being returned."
        )
        return pd.DataFrame(), ''

    test_type = json_paths[0].split(os.sep)[5]

    if test_type != '' and not hunter_df_out.empty:

        return hunter_df_out, test_type


if __name__ == '__main__':

    # cd into nightly_results
    nightly_result_path = NIGHTLY_RESULTS_DIR
    cd_into_proj_dir(nightly_result_path)

    # Get sorted list of dates from output folders with performance tests
    nightly_result_dates = os.listdir(nightly_result_path)
    nightly_result_dates.sort()

    # Set to False if running this for the first time, then re-run and set to True.
    is_case_prospective = PROSPECTIVE_MODE
    if is_case_prospective:
        # cd into hunter_csv
        cd_into_proj_dir()

        hunter_df_fixed_100 = pd.read_csv(FIXED_100_CSV_NAME)
        hunter_df_rated_100 = pd.read_csv(RATED_100_CSV_NAME)
        hunter_df_fixed_1000 = pd.read_csv(FIXED_1000_CSV_NAME)
        hunter_df_rated_1000 = pd.read_csv(RATED_1000_CSV_NAME)
        hunter_df_fixed_10000 = pd.read_csv(FIXED_10000_CSV_NAME)
        hunter_df_rated_10000 = pd.read_csv(RATED_10000_CSV_NAME)

        # Get date from the latest test run
        test_input_date = nightly_result_dates[-1]
        type_of_test = type(test_input_date)

        yesterday_s_date = get_yesterday_date(FMT_Y_M_D)

        # Get path to the latest test run
        path_w_date = f'{nightly_result_path}{os.sep}{test_input_date}'

        # cd into filename with a date-specific test run (path_w_date)
        cd_into_proj_dir(path_w_date)

        path_to_each_test_json = get_paths_to_six_json(path_w_date)

        list_of_hunter_df = []
        list_of_type_of_tests = []
        for test_json_path in path_to_each_test_json:

            hunter_df, type_of_test = get_hunter_df_w_test_type(
                test_json_path, is_case_prospective)
            if type_of_test != '' and not hunter_df.empty:
                list_of_hunter_df.append(hunter_df)
                list_of_type_of_tests.append(type_of_test)

        counter = 0
        for type_of_test in list_of_type_of_tests:
            if '-fixed-100-' in type_of_test:
                concat_df_100_fixed = pd.concat(
                    [hunter_df_fixed_100, list_of_hunter_df[counter]])
            elif '-rated-100-' in type_of_test:
                concat_df_100_rated = pd.concat(
                    [hunter_df_rated_100, list_of_hunter_df[counter]])
            elif '-fixed-1000-' in type_of_test:
                concat_df_1000_fixed = pd.concat(
                    [hunter_df_fixed_1000, list_of_hunter_df[counter]])
            elif '-rated-1000-' in type_of_test:
                concat_df_1000_rated = pd.concat(
                    [hunter_df_rated_1000, list_of_hunter_df[counter]])
            elif '-fixed-10000-' in type_of_test:
                concat_df_10000_fixed = pd.concat(
                    [hunter_df_fixed_10000, list_of_hunter_df[counter]])
            elif '-rated-10000-' in type_of_test:
                concat_df_10000_rated = pd.concat(
                    [hunter_df_rated_10000, list_of_hunter_df[counter]])
            else:
                get_error_log(type_of_test)

            counter += 1

        save_df_to_csv(concat_df_100_fixed, FIXED_100_CSV_NAME)
        save_df_to_csv(concat_df_100_rated, RATED_100_CSV_NAME)
        save_df_to_csv(concat_df_1000_fixed, FIXED_1000_CSV_NAME)
        save_df_to_csv(concat_df_1000_rated, RATED_1000_CSV_NAME)
        save_df_to_csv(concat_df_10000_fixed, FIXED_10000_CSV_NAME)
        save_df_to_csv(concat_df_10000_rated, RATED_10000_CSV_NAME)

    else:
        hunter_df_100_fixed = []
        hunter_df_100_rated = []
        hunter_df_1000_fixed = []
        hunter_df_1000_rated = []
        hunter_df_10000_fixed = []
        hunter_df_10000_rated = []
        types_of_tests = []
        for input_date in nightly_result_dates:

            # Path to the latest test run
            path_w_date = f'{nightly_result_path}{os.sep}{input_date}'

            # cd into filename with a date-specific test run (path_w_date)
            cd_into_proj_dir(path_w_date)

            path_to_each_test_json = get_paths_to_six_json(path_w_date)

            for test_json_path in path_to_each_test_json:

                hunter_df, type_of_test = get_hunter_df_w_test_type(
                    test_json_path, is_case_prospective)

                if type_of_test != '':
                    types_of_tests.append(type_of_test)

                if '-fixed-100-' in type_of_test:
                    if not hunter_df.empty:
                        hunter_df_100_fixed.append(hunter_df)
                elif '-rated-100-' in type_of_test:
                    if not hunter_df.empty:
                        hunter_df_100_rated.append(hunter_df)
                elif '-fixed-1000-' in type_of_test:
                    if not hunter_df.empty:
                        hunter_df_1000_fixed.append(hunter_df)
                elif '-rated-1000-' in type_of_test:
                    if not hunter_df.empty:
                        hunter_df_1000_rated.append(hunter_df)
                elif '-fixed-10000-' in type_of_test:
                    if not hunter_df.empty:
                        hunter_df_10000_fixed.append(hunter_df)
                elif '-rated-10000-' in type_of_test:
                    if not hunter_df.empty:
                        hunter_df_10000_rated.append(hunter_df)
                else:
                    get_error_log(type_of_test)

        hunter_df_100_fixed = pd.concat(hunter_df_100_fixed)
        hunter_df_100_rated = pd.concat(hunter_df_100_rated)
        hunter_df_1000_fixed = pd.concat(hunter_df_1000_fixed)
        hunter_df_1000_rated = pd.concat(hunter_df_1000_rated)
        hunter_df_10000_fixed = pd.concat(hunter_df_10000_fixed)
        hunter_df_10000_rated = pd.concat(hunter_df_10000_rated)

        unique_types_of_tests = pd.Series(types_of_tests).unique()

        for unique_test_type in unique_types_of_tests:
            if '-fixed-100-' in unique_test_type:
                save_df_to_csv(hunter_df_100_fixed,
                               f'hunter-{unique_test_type}{HUNTER_FILE_FMT}')
            elif '-rated-100-' in unique_test_type:
                save_df_to_csv(hunter_df_100_rated,
                               f'hunter-{unique_test_type}{HUNTER_FILE_FMT}')
            elif '-fixed-1000-' in unique_test_type:
                save_df_to_csv(hunter_df_1000_fixed,
                               f'hunter-{unique_test_type}{HUNTER_FILE_FMT}')
            elif '-rated-1000-' in unique_test_type:
                save_df_to_csv(hunter_df_1000_rated,
                               f'hunter-{unique_test_type}{HUNTER_FILE_FMT}')
            elif '-fixed-10000-' in unique_test_type:
                save_df_to_csv(hunter_df_10000_fixed,
                               f'hunter-{unique_test_type}{HUNTER_FILE_FMT}')
            elif '-rated-10000-' in unique_test_type:
                save_df_to_csv(hunter_df_10000_rated,
                               f'hunter-{unique_test_type}{HUNTER_FILE_FMT}')
            else:
                get_error_log(unique_test_type)
