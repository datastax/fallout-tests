"""
This is a python file to send email to the Cassandra mailing list if hunter detects performance regressions.
"""

import logging
import smtplib
import sys
from email.message import EmailMessage
from typing import List, Tuple

from ..utils import get_git_sha_for_cassandra, get_git_sha_for_fallout_tests
from .constants import (ALL_BAD_SIGNIF_CHANGES, RECEIVER_EMAIL, TEMPLATE_MSG,
                        THRESH_PERF_REGRESS, TXT_FILE_W_MSG)
from .utils import get_aws_secrets, get_list_of_dict_from_json

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

# TODO: To test that this code works correctly with multiple dictionaries in the json
# TODO: Do not re-run code below if results from hunter were already analysed
# This json file contains multiple dictionaries
json_file_path = '/home/ec2-user/hunter_clone/hunter/hunter_result_fixed_100.json'


# TODO: Add function's docstring
def get_lists_of_signif_changes_w_context(
        hunter_results_list_of_dicts: List[dict]
) -> Tuple[List[str], List[str]]:

    for hunter_dict in hunter_results_list_of_dicts:
        test_type = next(iter(hunter_dict))

        if hunter_dict == {}:
            logging.info(f"No significant changes were detected for any metrics by hunter, "
                         f"as per the file named '{json_file_path}'.")
        # A list of dictionaries, each of which corresponds to one date of significant
        # changes wrt metrics detected by hunter
        list_of_time_and_signif_changes = hunter_dict['lwt-fixed-100-partitions']

        list_of_all_signif_changes_w_context = []  # All (significant) changes detected by hunter
        list_of_all_bad_signif_changes_w_context = []  # Only bad changes regardless of their % change
        list_of_all_bad_highly_signif_changes_w_context = []  # Only bad changes beyond +/- % threshold
        for dict_of_time_and_changes in list_of_time_and_signif_changes:
            date = dict_of_time_and_changes['time'].replace("-", "_").split(' ')
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
                list_of_all_signif_changes_w_context.append(significant_change_w_context)

                # For totalOps, opRate: bad changes would occur if their values decreased (i.e., the lower, the worse).
                # For all other metrics (minLat, avgLat, medianLat, p95, p99, p99.9, maxLat, MAD, and IQR):
                # bad changes would occur if their values increased (i.e., the higher, the worse, as higher latencies
                # and higher variations/spread are detrimental to performance).
                if change['metric'].startswith('totalOps') or change['metric'].startswith('opRate'):
                    if float(change['forward_change_percent']) < 0:
                        list_of_all_bad_signif_changes_w_context.append(significant_change_w_context)
                        if float(change['forward_change_percent']) < -THRESH_PERF_REGRESS:
                            list_of_all_bad_highly_signif_changes_w_context.append(significant_change_w_context)

                else:  # for all other metrics
                    if float(change['forward_change_percent']) > 0:
                        list_of_all_bad_signif_changes_w_context.append(significant_change_w_context)
                        if float(change['forward_change_percent']) > THRESH_PERF_REGRESS:
                            list_of_all_bad_highly_signif_changes_w_context.append(significant_change_w_context)
        return list_of_all_bad_signif_changes_w_context, list_of_all_bad_highly_signif_changes_w_context


def create_email_w_hunter_regressions(
    list_of_bad_signif_changes_w_context: List[str],
    list_of_bad_highly_signif_changes_w_context: List[str]
) -> None:
    """
    Create email with performance regressions detected by hunter.
    """

    # Write email content to txt file
    with open(TXT_FILE_W_MSG, 'w') as text_file:

        if ALL_BAD_SIGNIF_CHANGES:
            text_to_add = "\n".join(list_of_bad_signif_changes_w_context)
        else:
            text_to_add = "\n".join(list_of_bad_highly_signif_changes_w_context)

        # Insert list of performance regression detected into template msg above
        data_to_txt_file = TEMPLATE_MSG.replace('\n\n\n', f'\n\n{text_to_add}\n')

        text_file.writelines(data_to_txt_file)


def read_txt_send_email() -> None:
    """
    Read message from a txt file with performance regressions detected by hunter and send it as an email.
    """
    with open(TXT_FILE_W_MSG, 'r') as txt_file:
        # Create a text/plain message
        msg = EmailMessage()
        msg.set_content(txt_file.read())

    if ALL_BAD_SIGNIF_CHANGES:
        keywords_changes = 'All performance '
    else:
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


def main():
    """
    Get performance regressions detected by hunter and send them via email
    """
    hunter_list_of_dicts = get_list_of_dict_from_json(json_file_path)

    list_of_bad_signif_changes_w_context, list_of_bad_highly_signif_changes_w_context = \
        get_lists_of_signif_changes_w_context(hunter_list_of_dicts)

    create_email_w_hunter_regressions(list_of_bad_signif_changes_w_context, list_of_bad_highly_signif_changes_w_context)

    read_txt_send_email()


if __name__ == '__main__':
    main()
