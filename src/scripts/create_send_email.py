"""
This is a python file to send email to the Cassandra mailing list
if hunter detects performance regressions.
"""

import collections
import logging
import os
import smtplib
import sys
from email.message import EmailMessage

from typing import List

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
    Get a list of significant (above or below on a threshold) changes detected by hunter
    with context.

    Args:
        hunter_results_list_of_dicts: List[dict[List[dict]]]
                                    A list of dictionaries from hunter results.
        threshold: float
                A threshold above or below (+/-) which significant changes are detected.

    Returns:
            A list of significant changes.
    """
    unique_changes = set()
    git_shas = {}

    for hunter_dict in hunter_results_list_of_dicts:
        if hunter_dict == {}:
            logging.info(
                'No significant changes were detected for any metrics by hunter')
            continue
        test_type = next(iter(hunter_dict))

        # A list of dictionaries, each of which corresponds to one
        # date and time (with timezone) of significant changes wrt metrics
        # detected by hunter
        list_of_time_and_signif_changes = hunter_dict[test_type]

        # Keeps only significant changes beyond +/- % threshold
        list_of_signif_changes_w_context = [
            f"For the test '{test_type}' on date and time "
            f"'{dict_of_time_and_changes['time']}' that ran on "
            f"cassandra Git commit SHA "
            f"'{git_shas.get(date[0], get_git_sha_for_cassandra(date[0]))}' "
            f"and on fallout-tests Git commit SHA "
            f"'{git_shas.get(date[0], get_git_sha_for_fallout_tests(date[0]))}': "
            f"The metric '{change['metric']}' changed "
            f"by {change['forward_change_percent']}%.\n"
            for dict_of_time_and_changes in list_of_time_and_signif_changes
            for date in [dict_of_time_and_changes['time'].replace("-", "_").split(' ')]
            for change in dict_of_time_and_changes['changes']
            if abs(float(change['forward_change_percent'])) > threshold
        ]

        unique_changes.update(list_of_signif_changes_w_context)

        for dict_of_time_and_changes in list_of_time_and_signif_changes:
            date = dict_of_time_and_changes['time'].replace(
                "-", "_").split(' ')
            git_shas[date[0]] = git_shas.get(
                date[0], get_git_sha_for_cassandra(date[0]))
            git_shas[date[0]] = git_shas.get(
                date[0], get_git_sha_for_fallout_tests(date[0]))

    counter_changes = collections.Counter(unique_changes)
    dict_counter_changes = dict(counter_changes)
    list_of_signif_changes = [
        key for key, val in dict_counter_changes.items() if val == 1]

    return list_of_signif_changes


def create_email_w_hunter_regressions(
        new_changes: str,
        output_email_msg_path: str = TXT_FILE_W_MSG
) -> None:  # pragma: no cover
    """
    Create email with new performance regressions detected by hunter.

    Args:
        new_changes: str
                   New significant changes detected
                   by hunter with context to be sent by email.
        output_email_msg_path: str
                            The path with file extension (.txt) to
                            save the txt file of the entire email report.
    """
    # Write email content to txt file
    with open(output_email_msg_path, 'w') as text_file:
        # Insert list of performance regression detected into template msg above
        data_to_txt_file = TEMPLATE_MSG.replace(
            '\n\n\n', f'\n\n{new_changes}\n')
        text_file.writelines(data_to_txt_file)


def create_file_w_regressions_sent_by_email(
        signif_changes: List[str],
        initial_lines: List[str],
        output_file: str = LOG_FILE_W_MSG
) -> str:
    """
    Create log file with performance regressions detected by
    hunter and sent by email to avoid sending them again,
    and adds new regressions to existing list of regressions
    if any.

    Args:
        signif_changes: List[str]
            A list of significant changes detected
            by hunter with context.
        initial_lines: List[str]
            Initial lines in the log file (if any).
        output_file: str
            The path with file extension (.txt) to save the
            regressions sent by email.

    Returns:
        A string with new change/s detected to then be sent by
        email too (besides being added to the log file
        for tracking purposes and to avoid sending it again).
    """

    new_changes = set(signif_changes) - set(initial_lines)

    if new_changes:
        with open(output_file, 'a') as file:
            new_changes_str = ''.join(new_changes)
            file.write(new_changes_str)

        return new_changes_str


def read_txt_send_email() -> None:  # pragma: no cover
    """
    Read message from a txt file with performance regressions
    detected by hunter and send it as an email.
    """
    with open(TXT_FILE_W_MSG, 'r') as txt_file:
        # Create a text/plain message
        msg = EmailMessage()
        msg.set_content(txt_file.read())

    keywords_changes = f'The most significant ' \
                       f'(+/- {THRESH_PERF_REGRESS}%) performance '

    # Set up email details
    secret_creds = get_aws_secrets()
    send_to = RECEIVER_EMAIL

    msg['Subject'] = f"{keywords_changes}{'regressions detected by hunter'}"
    msg['From'] = secret_creds['username']
    msg['To'] = send_to

    # Create a session to connect to 'server location' and 'port number'
    session = smtplib.SMTP('smtp.gmail.com', 587)
    # Start TLS for security
    session.starttls()

    # Authentication and send email
    # (generate 16-digit pwd via Google acct as per
    # https://towardsdatascience.com/how-to-easily-automate-emails-with-python-8b476045c151#:~:text=with%20the%2016%2Dcharacter%20password)
    session.login(secret_creds['username'], secret_creds['password'])
    session.send_message(msg)

    session.quit()


def main():  # pragma: no cover
    """
    Get performance regressions detected by hunter and send them via one email
    """
    new_changes_strings_list = []
    for hunter_result_name in LIST_OF_HUNTER_RESULTS_JSONS:
        orig_json_path = f'{HUNTER_CLONE_PROJ_DIR}{os.sep}{hunter_result_name}'
        hunter_list_of_dict = get_list_of_dict_from_json(orig_json_path)
        list_of_signif_changes_w_context = get_list_of_signif_changes_w_context(
            hunter_list_of_dict)
        with open(LOG_FILE_W_MSG, 'r') as log_txt_file:
            initial_log_lines = log_txt_file.readlines()
        new_changes_str = create_file_w_regressions_sent_by_email(
            list_of_signif_changes_w_context,
            initial_log_lines
        )
        if new_changes_str:
            new_changes_strings_list.append(new_changes_str)

    # Only create and send an email if there were any new changes detected
    new_changes_str_concat = ''.join(
        new_changes_strings_list).lstrip(NEWLINE_SYMBOL)
    if new_changes_str_concat:
        create_email_w_hunter_regressions(new_changes_str_concat)
        read_txt_send_email()


if __name__ == '__main__':
    main()
