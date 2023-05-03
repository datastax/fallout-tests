import os
import unittest

from src.scripts.create_send_email import (
    create_file_w_regressions_sent_by_email,
    get_list_of_signif_changes_w_context)


class TestCreateSendEmail(unittest.TestCase):

    def setUp(self):
        """Create a temporary log file for testing"""
        self.test_file_path = 'test_log_file.txt'
        with open(self.test_file_path, 'w') as f:
            f.write('')

    def tearDown(self):
        """Remove the temporary file after testing"""
        os.remove(self.test_file_path)

    def test_get_list_of_signif_changes_w_context(self):
        # Create a dummy input list of dictionaries
        dummy_hunter_results_list_of_dicts = [
            {'lwt-fixed-100-partitions': [
                {'time': '2022-01-01 23:00:00', 'changes': [
                    {'metric': 'totalOps', 'forward_change_percent': '-10'},
                    {'metric': 'avgLat', 'forward_change_percent': '20'},
                    {'metric': 'p99', 'forward_change_percent': '30'},
                    {'metric': 'opRate', 'forward_change_percent': '-20'},
                    {'metric': 'p95', 'forward_change_percent': '-25'},
                    {'metric': 'maxLat', 'forward_change_percent': '50'},
                ]},
                {'time': '2022-01-02 23:00:00', 'changes': [
                    {'metric': 'totalOps', 'forward_change_percent': '-5'},
                    {'metric': 'avgLat', 'forward_change_percent': '10'},
                    {'metric': 'p99', 'forward_change_percent': '15'},
                    {'metric': 'opRate', 'forward_change_percent': '15'},
                    {'metric': 'p95', 'forward_change_percent': '15'},
                    {'metric': 'maxLat', 'forward_change_percent': '30'}
                ]},
            ]},
        ]

        # Set a sample threshold
        threshold = 11

        # Define the expected output
        expected_output = ["For the test 'lwt-fixed-100-partitions' on date and time '2022-01-01 23:00:00' that "
                           "ran on cassandra Git commit SHA '' and on fallout-tests Git commit SHA '': "
                           "The metric 'avgLat' changed by 20%.\n",
                           "For the test 'lwt-fixed-100-partitions' on date and time '2022-01-01 23:00:00' that "
                           "ran on cassandra Git commit SHA '' and on fallout-tests Git commit SHA '': "
                           "The metric 'p99' changed by 30%.\n",
                           "For the test 'lwt-fixed-100-partitions' on date and time '2022-01-01 23:00:00' that "
                           "ran on cassandra Git commit SHA '' and on fallout-tests Git commit SHA '': "
                           "The metric 'opRate' changed by -20%.\n",
                           "For the test 'lwt-fixed-100-partitions' on date and time '2022-01-01 23:00:00' that "
                           "ran on cassandra Git commit SHA '' and on fallout-tests Git commit SHA '': "
                           "The metric 'p95' changed by -25%.\n",
                           "For the test 'lwt-fixed-100-partitions' on date and time '2022-01-01 23:00:00' that "
                           "ran on cassandra Git commit SHA '' and on fallout-tests Git commit SHA '': "
                           "The metric 'maxLat' changed by 50%.\n",
                           "For the test 'lwt-fixed-100-partitions' on date and time '2022-01-02 23:00:00' that "
                           "ran on cassandra Git commit SHA '' and on fallout-tests Git commit SHA '': "
                           "The metric 'p99' changed by 15%.\n",
                           "For the test 'lwt-fixed-100-partitions' on date and time '2022-01-02 23:00:00' that "
                           "ran on cassandra Git commit SHA '' and on fallout-tests Git commit SHA '': "
                           "The metric 'opRate' changed by 15%.\n",
                           "For the test 'lwt-fixed-100-partitions' on date and time '2022-01-02 23:00:00' that "
                           "ran on cassandra Git commit SHA '' and on fallout-tests Git commit SHA '': "
                           "The metric 'p95' changed by 15%.\n",
                           "For the test 'lwt-fixed-100-partitions' on date and time '2022-01-02 23:00:00' that "
                           "ran on cassandra Git commit SHA '' and on fallout-tests Git commit SHA '': "
                           "The metric 'maxLat' changed by 30%.\n"]

        # Call the function and check the output
        result_output = get_list_of_signif_changes_w_context(
            dummy_hunter_results_list_of_dicts, threshold)

        self.assertIsInstance(result_output, list)
        self.assertCountEqual(result_output, expected_output)

    def test_create_file_w_regressions_sent_by_email(self):
        # Create sample input lists
        list_of_signif_changes_w_context = [
            "For the test 'test_type_1' on date and time '2022-01-02 23:00:00' that ran on cassandra Git commit SHA "
            "'None' and on fallout-tests Git commit SHA 'None': The metric 'avgLat' changed by 20.0%.\n",
        ]
        initial_lines_in_log = [
            "For the test 'test_type_2' on date and time '2022-01-01 23:00:00' that ran on cassandra Git commit SHA "
            "'None' and on fallout-tests Git commit SHA 'None': The metric 'opRate' changed by -20.0%.\n",
        ]
        output_log_file_path = self.test_file_path

        # Call the function and check the output
        result_output = create_file_w_regressions_sent_by_email(
            list_of_signif_changes_w_context, initial_lines_in_log, output_log_file_path)

        # Check that the log file was updated correctly
        with open(output_log_file_path, 'r') as f:
            log_file_content = f.read()

        first_str = "For the test 'test_type_1' on date and time '2022-01-02 23:00:00' that ran "
        second_str = "on cassandra Git commit SHA 'None' and on fallout-tests Git commit SHA 'None': "
        third_str = "The metric 'avgLat' changed by 20.0%.\n"

        expected_log_file_content = f'{first_str}{second_str}{third_str}'

        self.assertIsInstance(log_file_content, str)
        self.assertEqual(log_file_content, expected_log_file_content)

        # Check that the function output is correct
        expected_output = (
            "For the test 'test_type_1' on date and time '2022-01-02 23:00:00' that ran on cassandra Git commit SHA "
            "'None' and on fallout-tests Git commit SHA 'None': The metric 'avgLat' changed by 20.0%.\n"
        )
        self.assertEqual(result_output, expected_output)
