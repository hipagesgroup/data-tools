import os
from unittest import TestCase

from pandas import DataFrame
from py.builtin import execfile

from hip_data_tools.aws.common import AwsConnectionSettings
from hip_data_tools.etl.athena_to_adwords import AthenaToAdWordsOfflineConversion, \
    AthenaToAdWordsOfflineConversionSettings
from hip_data_tools.google.adwords import GoogleAdWordsConnectionSettings, \
    GoogleAdWordsSecretsManager


class TestAthenaToAdWordsOfflineConversion(TestCase):
    def test_the_transformation_works(self):
        aws_conn = AwsConnectionSettings(
            region="ap-southeast-2",
            secrets_manager=None,
            profile="default")
        execfile("../../secrets.py")

        settings = AthenaToAdWordsOfflineConversionSettings(
            source_database="long_lake",
            source_table="dim_date",
            source_connection_settings=aws_conn,
            etl_identifier="dfsdf",
            destination_batch_size=100,
            transformation_column_mapping={'abc': 'googleClickId',
                                           'def1': 'conversionName',
                                           'def2': 'conversionTime',
                                           'def4': 'conversionValue'},
            destination_connection_settings=GoogleAdWordsConnectionSettings(
                client_id=os.getenv("adwords_client_id"),
                user_agent="Tester",
                client_customer_id=os.getenv("adwords_client_customer_id"),
                secrets_manager=GoogleAdWordsSecretsManager()
            ),
        )
        etl = AthenaToAdWordsOfflineConversion(settings)

        df = DataFrame([
            {
                "abc": "123",
                "def1": "123",
                "def2": "123",
                "def3": "123",
                "def4": "123",
                "def5": "123",
            },
            {
                "abc": "222",
                "def1": "333",
                "def2": "444",
                "def3": "333",
                "def4": "333",
                "def5": "333",
            }
        ])
        result = etl._data_frame_to_destination_dict(df)
        expected = [
            {'conversionName': '123',
             'conversionTime': '123',
             'conversionValue': '123',
             'googleClickId': '123'},
            {'conversionName': '333',
             'conversionTime': '444',
             'conversionValue': '333',
             'googleClickId': '222'}
        ]
        self.assertListEqual(result, expected)
