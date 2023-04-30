import os
import unittest

import pandas as pd
from pandas.testing import assert_frame_equal

from src.scripts.utils import (add_suffix_to_col, get_list_of_dict_from_json,
                               get_relevant_dict)


class TestUtils(unittest.TestCase):

    def setUp(self):
        """Create a dummy JSON file for testing"""
        self.dummy_json_file_path = 'dummy_hunter.json'
        with open(self.dummy_json_file_path, 'w') as f:
            f.write('{"test_type": "100-fixed", "changes": 25}\n')
            f.write('{"test_type": "1000-fixed", "changes": 30}\n')
            f.write('{"test_type": "10000-fixed", "changes": 35}\n')

    def tearDown(self):
        """Delete the dummy JSON file after testing"""
        os.remove(self.dummy_json_file_path)

    def test_add_suffix_to_col(self):

        suffix = '.read'
        data_for_dummy_df = [[6007010, 9966], [6005190, 9962]]
        col_names_for_expected_dummy_df = [
            f'totalOps{suffix}', f'opRate{suffix}']
        col_names_for_input_dummy_df = ['totalOps', 'opRate']
        expected_dummy_df = pd.DataFrame(
            columns=col_names_for_expected_dummy_df,
            data=data_for_dummy_df
        )

        input_dummy_df = pd.DataFrame(
            columns=col_names_for_input_dummy_df,
            data=data_for_dummy_df
        )

        result_dummy_df = add_suffix_to_col(input_dummy_df, suffix)

        self.assertIsInstance(result_dummy_df, pd.DataFrame)
        assert_frame_equal(result_dummy_df, expected_dummy_df)

    def test_get_relevant_dict_w_result_success_and_relevant_phase(self):
        expected_relevant_dict = {
            'test': 'result-success-read', 'metrics': ['Ops/Sec'], 'Op Rate': 9966}
        dummy_dict_of_dicts = {'stats': [expected_relevant_dict]}
        result_relevant_dict = get_relevant_dict(dummy_dict_of_dicts, 'read')
        self.assertIsInstance(result_relevant_dict, dict)
        self.assertEqual(result_relevant_dict, expected_relevant_dict)

    def test_get_relevant_dict_w_result_success_but_no_relevant_phase(self):
        expected_relevant_dict = {}
        dummy_dict_of_dicts = {'stats': [
            {'test': 'result-success-create-ks', 'metrics': ['Ops/Sec'], 'Op Rate': 0}]}
        result_relevant_dict = get_relevant_dict(dummy_dict_of_dicts, 'read')
        self.assertIsInstance(result_relevant_dict, dict)
        self.assertEqual(result_relevant_dict, expected_relevant_dict)

    def test_get_relevant_dict_wo_result_success(self):
        expected_relevant_dict = {}
        dummy_dict_of_dicts = {
            'stats': [{'test': 'failed-read', 'metrics': ['Ops/Sec'], 'Op Rate': 8700}]}
        result_relevant_dict = get_relevant_dict(dummy_dict_of_dicts, 'read')
        self.assertIsInstance(result_relevant_dict, dict)
        self.assertEqual(result_relevant_dict, expected_relevant_dict)

    def test_get_list_of_dict_from_json(self):
        expected_output = [
            {'test_type': '10000-fixed', 'changes': 35},
        ]
        result_output = get_list_of_dict_from_json(self.dummy_json_file_path)
        self.assertIsInstance(result_output, list)
        self.assertEqual(result_output, expected_output)
