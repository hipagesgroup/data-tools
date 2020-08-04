import datetime
from unittest import TestCase

import pandas as pd
from pandas._libs.tslibs.timestamps import Timestamp
from pandas._testing import assert_frame_equal
from py._builtin import execfile

from hip_data_tools.aws.common import AwsConnectionSettings, AwsSecretsManager
from hip_data_tools.etl.adwords_to_s3 import AdWordsReportsToS3, AdWordsReportToS3Settings


class TestAdwordsReportsToS3(TestCase):

    def test__should__return_dataframe_with_correct_types__when__a_dataframe_is_given(self):
        # Load secrets via env vars
        execfile("../../secrets.py")

        aws_setting = AwsConnectionSettings(
            region="ap-southeast-2",
            secrets_manager=AwsSecretsManager(),
            profile=None)

        adwords_reports_to_s3_util = AdWordsReportsToS3(
            AdWordsReportToS3Settings(
                source_query="",
                source_include_zero_impressions=True,
                source_connection_settings=None,
                target_bucket="test-target-bucket",
                target_key_prefix="",
                target_file_prefix="",
                target_connection_settings=None,
                transformation_field_type_mask={"ad_group_id": int,
                                                "labels": str,
                                                "tuple_field": object,
                                                "bool_field": bool,
                                                "array_field": object,
                                                "num_array_filed": object,
                                                "time_field": datetime,
                                                "complex_field": object}))

        input_value_dict = {'ad_group_id': {0: 94823864785, 1: 34523864785},
                            'labels': {0: 'Hello_1', 1: 'Hello_2'},
                            'tuple_field': {0: ('field_1', 'val_1'), 1: ('field_2', 'val_2')},
                            'bool_field': {0: True, 1: False},
                            'array_field': {0: ['["v1", "v2"]'], 1: ['[["v1", "v2"], []]']},
                            'num_array_filed': {0: ['1', '3', '4', '5'], 1: ['1', '3', '4', '5']},
                            'time_field': {0: Timestamp('2020-06-18 00:00:00'),
                                           1: Timestamp('2020-06-18 00:00:00')},
                            'complex_field': {0: [], 1: ['{"x": "hello", "y": "world"}']}}
        input_value = pd.DataFrame(data=input_value_dict)

        actual = adwords_reports_to_s3_util._mask_field_types(input_value)

        expected = pd.DataFrame(data={})
        assert_frame_equal(expected, actual)
