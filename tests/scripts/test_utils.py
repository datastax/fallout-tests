import unittest

from freezegun import freeze_time

import pandas as pd

from pandas.testing import assert_frame_equal

from src.scripts.utils import add_suffix_to_col, get_relevant_dict, get_yesterday_date


class TestUtils(unittest.TestCase):

    def test_add_suffix_to_col(self):

        suffix = '.read'
        data_for_dummy_df = [[6007010, 9966], [6005190, 9962]]
        col_names_for_expected_dummy_df = [f'totalOps{suffix}', f'opRate{suffix}']
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
        expected_relevant_dict = {'test': 'result-success-read', 'metrics': ['Ops/Sec'], 'Op Rate': 9966}
        dummy_dict_of_dicts = {'stats': [expected_relevant_dict]}
        result_relevant_dict = get_relevant_dict(dummy_dict_of_dicts, 'read')
        self.assertIsInstance(result_relevant_dict, dict)
        self.assertEqual(result_relevant_dict, expected_relevant_dict)

    def test_get_relevant_dict_w_result_success_but_no_relevant_phase(self):
        expected_relevant_dict = {}
        dummy_dict_of_dicts = {'stats': [{'test': 'result-success-create-ks', 'metrics': ['Ops/Sec'], 'Op Rate': 0}]}
        result_relevant_dict = get_relevant_dict(dummy_dict_of_dicts, 'read')
        self.assertIsInstance(result_relevant_dict, dict)
        self.assertEqual(result_relevant_dict, expected_relevant_dict)

    def test_get_relevant_dict_wo_result_success(self):
        expected_relevant_dict = {}
        dummy_dict_of_dicts = {'stats': [{'test': 'failed-read', 'metrics': ['Ops/Sec'], 'Op Rate': 8700}]}
        result_relevant_dict = get_relevant_dict(dummy_dict_of_dicts, 'read')
        self.assertIsInstance(result_relevant_dict, dict)
        self.assertEqual(result_relevant_dict, expected_relevant_dict)

    @freeze_time('2023-04-05')
    def test_get_yesterday_date(self):
        expected_yesterday_date = '2023_04_04'
        result_yesterday_date = get_yesterday_date()
        self.assertIsInstance(result_yesterday_date, str)
        self.assertEqual(result_yesterday_date, expected_yesterday_date)
