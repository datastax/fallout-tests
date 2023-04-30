"""
This is a python file to send email to the Cassandra mailing list if hunter detects performance regressions.
"""

import logging
import os
import smtplib
import sys
from email.message import EmailMessage
from pathlib import Path
from typing import List

import pandas as pd

from src.scripts.constants import (HUNTER_CLONE_PROJ_DIR,
                                   LIST_OF_HUNTER_RESULTS_JSONS,
                                   LOG_FILE_W_MSG, NEWLINE_SYMBOL,
                                   RECEIVER_EMAIL, TEMPLATE_MSG,
                                   THRESH_PERF_REGRESS, TXT_FILE_W_MSG)
from src.scripts.utils import (get_aws_secrets, get_git_sha_for_cassandra,
                               get_git_sha_for_fallout_tests,
                               get_list_of_dict_from_json)

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


def get_list_of_signif_changes_w_context(
        hunter_results_list_of_dicts: List[dict[List[dict]]],
        threshold: float = THRESH_PERF_REGRESS
) -> List[str]:
    """
    Get a list of highly (above or below on a threshold) bad significant changes detected by hunter
    with context.

    Args:
        hunter_results_list_of_dicts: List[dict[List[dict]]]
                                    A list of dictionaries from hunter results.
        threshold: float
                A threshold above or below (+/-) which highly bad significant changes are detected.

    Returns:
            A list of highly bad significant changes.
    """
    for hunter_dict in hunter_results_list_of_dicts:
        test_type = next(iter(hunter_dict))

        if hunter_dict == {}:
            logging.info(
                'No significant changes were detected for any metrics by hunter')
        # A list of dictionaries, each of which corresponds to one date of significant
        # changes wrt metrics detected by hunter
        list_of_time_and_signif_changes = hunter_dict[test_type]

        # Only bad changes beyond +/- % threshold
        list_of_all_bad_highly_signif_changes_w_context = []
        for dict_of_time_and_changes in list_of_time_and_signif_changes:
            date = dict_of_time_and_changes['time'].replace(
                "-", "_").split(' ')
            cass_git_sha = get_git_sha_for_cassandra(date[0])
            fallout_tests_git_sha = get_git_sha_for_fallout_tests(date[0])
            if dict_of_time_and_changes['changes'] == 0:
                logging.info(f"There are no significant changes detected for the date and time "
                             f"'{dict_of_time_and_changes['time']}'.")

            for change in dict_of_time_and_changes['changes']:
                significant_change_w_context = f"For the test '{test_type}' on date and time " \
                                               f"'{dict_of_time_and_changes['time']}' that ran on cassandra Git " \
                                               f"commit SHA '{cass_git_sha}' and on fallout-tests Git commit " \
                                               f"SHA '{fallout_tests_git_sha}':\n\t " \
                                               f"The metric '{change['metric']}' changed by " \
                                               f"{change['forward_change_percent']}%.\n"
                logging.info(significant_change_w_context)

                # For totalOps, opRate: bad changes would occur if their values decreased (i.e., the lower, the worse).
                # For all other metrics (minLat, avgLat, medianLat, p95, p99, p99.9, maxLat, MAD, and IQR):
                # bad changes would occur if their values increased (i.e., the higher, the worse, as higher latencies
                # and higher variations/spread are detrimental to performance).
                if change['metric'].startswith('totalOps') or change['metric'].startswith('opRate'):
                    if float(change['forward_change_percent']) < -threshold:
                        list_of_all_bad_highly_signif_changes_w_context.append(
                            significant_change_w_context)

                else:  # for all other metrics
                    if float(change['forward_change_percent']) > threshold:
                        list_of_all_bad_highly_signif_changes_w_context.append(
                            significant_change_w_context)

        all_bad_highly_signif_changes_combos_series = pd.Series(
            list_of_all_bad_highly_signif_changes_w_context)
        all_bad_highly_signif_changes_combos_and_freqs = all_bad_highly_signif_changes_combos_series.value_counts()

        list_of_unique_all_bad_highly_signif_changes = []
        for i in range(len(all_bad_highly_signif_changes_combos_and_freqs)):
            if all_bad_highly_signif_changes_combos_and_freqs.iloc[i] == 1:
                list_of_unique_all_bad_highly_signif_changes.append(
                    all_bad_highly_signif_changes_combos_and_freqs.index[i]
                )

        return list_of_unique_all_bad_highly_signif_changes


def create_email_w_hunter_regressions(
        new_changes: str,
        output_email_msg_path: str = TXT_FILE_W_MSG
) -> None:  # pragma: no cover
    """
    Create email with new performance regressions detected by hunter.

    Args:
        new_changes: str
                   New bad/highly bad significant changes detected by hunter with context to be sent by email.
        output_email_msg_path: str
                            The path with file extension (.txt) to save the txt file of the entire email report.
    """
    # Write email content to txt file
    with open(output_email_msg_path, 'w') as text_file:
        # Insert list of performance regression detected into template msg above
        data_to_txt_file = TEMPLATE_MSG.replace(
            '\n\n\n', f'\n\n{new_changes}\n')
        text_file.writelines(data_to_txt_file)


def create_file_w_regressions_sent_by_email(
        list_of_bad_highly_signif_changes_w_context: List[str],
        initial_lines_in_log: List[str],
        output_log_file_path: str = LOG_FILE_W_MSG
) -> str:
    """
    Create log file with performance regressions detected by hunter and sent by email to avoid sending them again,
    and adds new regressions to existing list of regressions if any.

    Args:
        list_of_bad_highly_signif_changes_w_context: List[str]
                                            A list of highly bad significant changes detected by hunter with context.
        initial_lines_in_log: List[str]
                            Initial lines in the log file (if any).
        output_log_file_path: str
                            The path with file extension (.txt) to save the regressions sent by email.

    Returns:
            A string with new change/s detected to then be sent by email too (besides being added to the log file
            for tracking purposes and to avoid sending it again later on).
    """
    # Write log content to txt file (use append/'a' mode not to overwrite the previous contents with mode 'w' otherwise)
    with open(output_log_file_path, 'a') as text_file:
        text_to_add = NEWLINE_SYMBOL.join(
            list_of_bad_highly_signif_changes_w_context)
        initial_lines_in_log_joined = ''.join(initial_lines_in_log)

        # Skip initial newline not to create an unnecessary empty line at the top of the log file
        initial_lines_in_log_joined = initial_lines_in_log_joined.lstrip(
            NEWLINE_SYMBOL)
        text_to_add = text_to_add.lstrip(NEWLINE_SYMBOL)

        # Only add new regressions (not to duplicate the old regressions)
        new_changes = []
        for item_list in text_to_add.split('\n\n'):
            if item_list not in initial_lines_in_log_joined:
                new_changes.append(item_list)

        new_changes = '\n\n'.join(new_changes)

        # Add newline at the beginning of the new changes to then concatenate it with the previous ones consistently
        new_changes = f'{NEWLINE_SYMBOL}{new_changes}'

        if new_changes != NEWLINE_SYMBOL:
            if initial_lines_in_log_joined != text_to_add:
                if text_to_add != '':
                    # If some regressions were already in the log file (from previous runs), add new regressions
                    if len(initial_lines_in_log) != 0 or initial_lines_in_log_joined != '':
                        text_to_add = new_changes

                    # Insert list of performance regression sent by email
                    text_file.writelines(text_to_add)

                    return new_changes


def read_txt_send_email() -> None:  # pragma: no cover
    """
    Read message from a txt file with performance regressions detected by hunter and send it as an email.
    """
    with open(TXT_FILE_W_MSG, 'r') as txt_file:
        # Create a text/plain message
        msg = EmailMessage()
        msg.set_content(txt_file.read())

    keywords_changes = f'The most significant (+/- {THRESH_PERF_REGRESS}%) performance '

    # Set up email details
    secret_creds = get_aws_secrets()
    send_to = RECEIVER_EMAIL

    msg['Subject'] = f"{keywords_changes}{'regressions detected by hunter'}"
    msg['From'] = secret_creds['username']
    msg['To'] = send_to

    # Create a session to connect to 'server location' and 'port number'
    s = smtplib.SMTP('smtp.gmail.com', 587)
    # Start TLS for security
    s.starttls()

    # Authentication and send email
    # (generate 16-digit pwd via Google acct as per
    # https://towardsdatascience.com/how-to-easily-automate-emails-with-python-8b476045c151#:~:text=with%20the%2016%2Dcharacter%20password)
    s.login(secret_creds['username'], secret_creds['password'])
    s.send_message(msg)

    s.quit()


def main():  # pragma: no cover
    """
    Get performance regressions detected by hunter and send them via one email
    """

    list_of_paths_to_json = []
    for hunter_result_name in LIST_OF_HUNTER_RESULTS_JSONS:
        list_of_paths_to_json.append(f'{HUNTER_CLONE_PROJ_DIR}{os.sep}{hunter_result_name}')

    new_changes_strings_list = []
    for orig_json_path in list_of_paths_to_json:
        hunter_list_of_dict = get_list_of_dict_from_json(orig_json_path)

        list_of_bad_highly_signif_changes_w_context = get_list_of_signif_changes_w_context(
            hunter_list_of_dict)

        initial_log_lines = ''
        path_to_log_file = Path(LOG_FILE_W_MSG)
        if path_to_log_file.exists():
            with open(LOG_FILE_W_MSG, 'r') as log_txt_file:
                initial_log_lines = log_txt_file.readlines()

        new_changes_str = create_file_w_regressions_sent_by_email(
            list_of_bad_highly_signif_changes_w_context,
            initial_log_lines
        )

        new_changes_strings_list.append(new_changes_str)

    new_changes_str_concat = ''.join(new_changes_strings_list)
    # Only create and send an email if there were any new changes detected
    if new_changes_str_concat != NEWLINE_SYMBOL:
        if new_changes_str_concat is not None and new_changes_str_concat != '':
            # Strip newline symbol previously added on the left-hand side
            new_changes_str_concat = new_changes_str_concat.lstrip(NEWLINE_SYMBOL)

            create_email_w_hunter_regressions(new_changes_str_concat)
            read_txt_send_email()


if __name__ == '__main__':
    main()
