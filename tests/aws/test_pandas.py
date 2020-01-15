from unittest import TestCase
from datetime import datetime, timezone
from pandas.util.testing import assert_frame_equal
import pandas as pd
import numpy as np
from pathlib import Path

from hip_data_tools.aws.pandas import PandasUtil
import pandas.api.types as ptypes


class TestCommonUtil(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.pu = PandasUtil(context="")

    # def test_return_dataframe_xcom_exists(self):
    #     test_df = pd.DataFrame(np.random.randn(100, 2))
    #     df_name = "test_df"
    #     self.pu.return_dataframe(test_df, df_name, s3_backed=False)
    #     actual = self.test_context['ti'].xcom_pull(task_ids='initialize_test', key=df_name)
    #     expected = "/data/dags/test_dag/test_run_unit_test_dag_20150102/initialize_test/initialize_test-test_df.pkl"
    #     self.assertEqual(actual, expected)
    #
    # def test_return_dataframe_file_exists(self):
    #     test_df = pd.DataFrame(np.random.randn(100, 2))
    #     df_name = "test_df"
    #     self.pu.return_dataframe(test_df, df_name, s3_backed=False)
    #     actual_path = self.test_context['ti'].xcom_pull(task_ids='initialize_test', key=df_name)
    #     actual = Path(actual_path).is_file()
    #     self.assertEqual(actual, True)
    #
    # def test_return_dataframe_match(self):
    #     test_df = pd.DataFrame(np.random.randn(100, 2))
    #     df_name = "test_df"
    #     self.pu.return_dataframe(test_df, df_name, s3_backed=False)
    #     actual_path = self.test_context['ti'].xcom_pull(task_ids='initialize_test', key=df_name)
    #     actual_df = pd.read_pickle(actual_path)
    #     assert_frame_equal(actual_df, test_df)
    #
    # def test_retrive_dataframe_fail(self):
    #     df_name = "test_df"
    #     with self.assertRaises(AttributeError):
    #         self.pu.retrive_dataframe(task_id=self.core.task_id, dataframe_name=df_name, s3_backed=False)
    #
    # def test_retrive_dataframe_pass(self):
    #     test_df = pd.DataFrame(np.random.randn(100, 2))
    #     df_name = "test_df"
    #     self.pu.return_dataframe(test_df, df_name, s3_backed=False)
    #     actual_df = self.pu.retrive_dataframe(task_id=self.core.task_id, dataframe_name=df_name, s3_backed=False)
    #     assert_frame_equal(actual_df, test_df)

    def test__transform_date_datatype__converts_date_string(self):
        test_df = pd.DataFrame({"ABC": ["foo"], "Date": ["2018-01-01"]})
        actual_df = self.pu.transform_date_datatype(df=test_df)
        self.assertTrue(ptypes.is_datetime64_any_dtype(actual_df['Date']))

    def test__transform_date_datatype__converts_datetime_string(self):
        test_df = pd.DataFrame({"Date": ["2018-01-01 12:12:12"]}, index=[0])
        result_df = self.pu.transform_date_datatype(df=test_df)
        self.assertEquals(result_df.dtypes["Date"], np.dtype('<M8[ns]'), "Date type converted to datetime")

    def test__transform_remove_pii__Performs_a_correct_sha_hash(self):
        test_df = pd.DataFrame({"EmailAddress": "test@coldmail.org"}, index=[0])
        result_df = self.pu.transform_remove_pii(df=test_df)
        self.assertEquals(
            result_df["SHA256EmailAddress"][0],
            "e74589573a625b55fe4a79b5d4e7fde67f6af19a6b3cae497903b3455ba10ae1",
            "One EMail has been sha256 hashed"
        )

    def test__json_to_dataframe__converts_an_array_of_json(self):
        json_data = [{"EmailAddress": "test@coldmail.org"}, {"EmailAddress": "test@coldmail.org"}]
        expected_df = pd.DataFrame([{"EmailAddress": "test@coldmail.org"}, {"EmailAddress": "test@coldmail.org"}],
                                   index=[0, 1])
        actual_df = self.pu.json_to_dataframe(json_data)

        assert_frame_equal(expected_df, actual_df)

    def test__json_to_dataframe__converts_an_array_of_nested_json(self):
        json_data = [{"EmailAddress": "test@coldmail.org"}, {"EmailAddress": "test@coldmail.org"}]
        expected_df = pd.DataFrame([{"EmailAddress": "test@coldmail.org"}, {"EmailAddress": "test@coldmail.org"}],
                                   index=[0, 1])
        actual_df = self.pu.json_to_dataframe(json_data)
        assert_frame_equal(expected_df, actual_df)

    def test__transform_column_to_numeric__converts_stringified_numbers(self):
        test_df = pd.DataFrame([{"TestField": "12345"}, {"TestField": "6789"}], index=[0, 1])
        expected_df = pd.DataFrame([{"TestField": 12345.0}, {"TestField": 6789.0}], index=[0, 1])
        actual_df = self.pu.transform_column_dtypes(df=test_df, dtype_transformations={"TestField": np.float64})
        assert_frame_equal(actual_df, expected_df)

    def test__transform_column_to_numeric__converts_floats(self):
        test_df = pd.DataFrame([{"TestField": 12345}, {"TestField": 6789}], index=[0, 1])
        expected_df = pd.DataFrame([{"TestField": 12345.0}, {"TestField": 6789.0}], index=[0, 1])
        actual_df = self.pu.transform_column_dtypes(df=test_df, dtype_transformations={"TestField": np.float64})
        assert_frame_equal(actual_df, expected_df)
