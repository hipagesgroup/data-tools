# from unittest import TestCase
# import numpy as np
# import pandas as pd
# from pandas._libs.tslibs.timestamps import Timestamp
# from hip_data_tools.aws.s3 import S3Util
# from hip_data_tools.etl.adwords_to_s3 import AdWordsReportsToS3, AdWordsReportToS3Settings
#
#
# class TestAdwordsReportsToS3(TestCase):
#
#     def test__should__return_dataframe_with_correct_types__when__a_dataframe_is_given(self):
#         adwords_reports_to_s3_util = AdWordsReportsToS3(
#             AdWordsReportToS3Settings(
#                 source_query="",
#                 source_include_zero_impressions=True,
#                 source_connection_settings=None,
#                 target_bucket="test-target-bucket",
#                 target_key_prefix="",
#                 target_file_prefix="",
#                 target_connection_settings=None,
#                 transformation_field_type_mask={"ad_group_id": np.dtype(int),
#                                                 "labels": np.dtype(str),
#                                                 "tuple_field": np.dtype(object),
#                                                 "bool_field": np.dtype(bool),
#                                                 "array_field": np.dtype(object),
#                                                 "num_array_filed": np.dtype(object),
#                                                 "time_field": np.dtype(object),
#                                                 "complex_field": np.dtype(object)}))
#
#         input_value_dict = {'ad_group_id': {0: 94823864785, 1: 34523864785},
#                             'labels': {0: 'Hello_1', 1: 'Hello_2'},
#                             'tuple_field': {0: ('field_1', 'val_1'), 1: ('field_2', 'val_2')},
#                             'bool_field': {0: True, 1: False},
#                             'array_field': {0: ['["v1", "v2"]'], 1: ['[["v1", "v2"], []]']},
#                             'num_array_filed': {0: ['1', '3', '4', '5'], 1: ['1', '3', '4', '5']},
#                             'time_field': {0: Timestamp('2020-06-18 00:00:00'),
#                                            1: Timestamp('2020-06-18 00:00:00')},
#                             'complex_field': {0: [], 1: ['{"x": "hello", "y": "world"}']}}
#         input_value = pd.DataFrame(data=input_value_dict)
#
#         adwords_reports_to_s3_util._mask_field_types(input_value)
#
#         actual = input_value.dtypes.to_string()
#         expected = """ad_group_id         int64
# labels             object
# tuple_field        object
# bool_field           bool
# array_field        object
# num_array_filed    object
# time_field         object
# complex_field      object"""
#         self.assertEqual(expected, actual)
#
#     def integration_test__should__transfer_data__when__the_method_is_called(self):
#         adwords_reports_to_s3_util = AdWordsReportsToS3(
#             AdWordsReportToS3Settings(
#                 source_query="",
#                 source_include_zero_impressions=True,
#                 source_connection_settings=None,
#                 target_bucket="test-target-bucket",
#                 target_key_prefix="",
#                 target_file_prefix="",
#                 target_connection_settings=None,
#                 transformation_field_type_mask=None))
#
#         input_value_dict = {'ad_group_id': {0: 94823864785, 1: 34523864785},
#                             'labels': {0: 'Hello_1', 1: 'Hello_2'},
#                             'tuple_field': {0: ('field_1', 'val_1'), 1: ('field_2', 'val_2')},
#                             'bool_field': {0: True, 1: False},
#                             'array_field': {0: ['["v1", "v2"]'], 1: ['[["v1", "v2"], []]']},
#                             'num_array_filed': {0: ['1', '3', '4', '5'], 1: ['1', '3', '4', '5']},
#                             'time_field': {0: Timestamp('2020-06-18 00:00:00'),
#                                            1: Timestamp('2020-06-18 00:00:00')},
#                             'complex_field': {0: [], 1: ['{"x": "hello", "y": "world"}']}}
#         data_frame = pd.DataFrame(data=input_value_dict)
#
#         def _get_report_data(self):
#             return data_frame
#
#         AdWordsReportsToS3._get_report_data = _get_report_data
#
#         def upload_dataframe_as_parquet(self, dataframe, key, file_name):
#             pass
#
#         S3Util.upload_dataframe_as_parquet = upload_dataframe_as_parquet
#
#         kwargs = {}
#         AdWordsReportsToS3.transfer(adwords_reports_to_s3_util, **kwargs)
