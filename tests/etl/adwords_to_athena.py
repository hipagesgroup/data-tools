import os
from collections import OrderedDict
from unittest import TestCase
from unittest.mock import Mock
from googleads.adwords import ServiceQueryBuilder, ReportQueryBuilder
from pandas import np, DataFrame
from py._builtin import execfile
from hip_data_tools.aws.athena import AthenaUtil
from hip_data_tools.aws.common import AwsConnectionManager, AwsConnectionSettings, AwsSecretsManager
from hip_data_tools.etl.adwords_to_athena import AdWordsToAthenaSettings, AdWordsToAthena, \
    AdWordsReportsToAthena, AdWordsReportsToAthenaSettings
from hip_data_tools.google.adwords import GoogleAdWordsConnectionSettings, \
    GoogleAdWordsSecretsManager


class TestAdwordsToS3(TestCase):

    def test__should__get_correct_estimations__with__etl_get_parallel_payloads(self):
        aws_setting = AwsConnectionSettings(
            region="us-east-1",
            secrets_manager=AwsSecretsManager(),
            profile=None)
        target_bucket = os.getenv('S3_TEST_BUCKET')
        target_key_prefix = "something/test"

        # Load secrets via env vars
        execfile("../../secrets.py")
        adwords_settings = GoogleAdWordsConnectionSettings(
            client_id=os.getenv("adwords_client_id"),
            user_agent="Tester",
            client_customer_id=os.getenv("adwords_client_customer_id"),
            secrets_manager=GoogleAdWordsSecretsManager())
        target_table = "test_adwords_to_athena_table_creation"
        etl_settings = AdWordsToAthenaSettings(
            source_query_fragment=ServiceQueryBuilder().Select('Id').OrderBy('Id'),
            source_service="AdGroupAdService",
            source_service_version="v201809",
            source_connection_settings=adwords_settings,
            target_bucket=target_bucket,
            target_key_prefix=target_key_prefix,
            target_connection_settings=aws_setting,
            target_database="dev",
            target_table=target_table,
            target_table_ddl_progress=True,
            is_partitioned_table=True,
            partition_values=[("abc", "def"), ("pqr", 123)]
        )
        etl = AdWordsToAthena(etl_settings)

        actual_payloads = etl.get_parallel_payloads(page_size=1000, number_of_workers=3)
        expected_payloads = [
            {
                'number_of_pages': 393,
                'page_size': 1000,
                'start_index': 0,
                'worker': 0
            },
            {
                'number_of_pages': 393,
                'page_size': 1000,
                'start_index': 393000,
                'worker': 1
            },
            {
                'number_of_pages': 393,
                'page_size': 1000,
                'start_index': 786000,
                'worker': 2
            }
        ]
        self.assertListEqual(expected_payloads, actual_payloads)
        etl.create_athena_table()
        conn = AwsConnectionManager(aws_setting)
        au = AthenaUtil("dev", conn)
        actual = au.get_glue_table_metadata(target_table)
        print(actual)

    def test__should__create_table__with__a_general_report(self):
        aws_setting = AwsConnectionSettings(
            region="us-east-1",
            secrets_manager=AwsSecretsManager(),
            profile=None)
        target_bucket = os.getenv('S3_TEST_BUCKET')
        target_key_prefix = "something/test"

        # Load secrets via env vars
        execfile("../../secrets.py")
        adwords_settings = GoogleAdWordsConnectionSettings(
            client_id=os.getenv("adwords_client_id"),
            user_agent="Tester",
            client_customer_id=os.getenv("adwords_client_customer_id"),
            secrets_manager=GoogleAdWordsSecretsManager())
        target_table = "test_adwords_negative_report"
        etl_settings = AdWordsReportsToAthenaSettings(
            source_query=(ReportQueryBuilder()
                          .Select('AccountDescriptiveName',
                                  'CampaignId',
                                  'CampaignName',
                                  'CampaignStatus',
                                  'Id',
                                  'KeywordMatchType',
                                  'Criteria'
                                  )
                          .From('CAMPAIGN_NEGATIVE_KEYWORDS_PERFORMANCE_REPORT')
                          .Build()),
            source_include_zero_impressions=True,
            source_connection_settings=adwords_settings,
            target_bucket=target_bucket,
            target_key_prefix=target_key_prefix,
            target_connection_settings=aws_setting,
            target_database="dev",
            target_table=target_table,
            target_table_ddl_progress=True,
            is_partitioned_table=True,
            partition_values=[("abc", "def"), ("pqr", 123)],
            target_file_prefix="data",
            transformation_field_type_mask=None
        )
        etl = AdWordsReportsToAthena(etl_settings)
        etl.transfer()
        etl.create_athena_table()
        etl.add_partitions()

        au = AthenaUtil(database="dev", conn=AwsConnectionManager(aws_setting),
                        output_bucket=os.environ["S3_TEST_BUCKET"])
        actual = au.run_query(query_string="""
        select * from dev.test_adwords_negative_report limit 10
        """, return_result=True)
        print(actual)
        expected = 11

        self.assertEqual(expected, len(actual["ResultSet"]["Rows"]))

    def test__should__create_table__with__geo_performance_report(self):
        aws_setting = AwsConnectionSettings(
            region="ap-southeast-2",
            secrets_manager=AwsSecretsManager(
                access_key_id_var="SOME_CUSTOM_AWS_ACCESS_KEY_ID",
                secret_access_key_var="SOME_CUSTOM_AWS_SECRET_ACCESS_KEY",
                use_session_token=True,
                aws_session_token_var="SOME_CUSTOM_AWS_SESSION_TOKEN"),
            profile=None
        )
        target_bucket = "test-bucket"
        target_key_prefix = "something/test"

        # Load secrets via env vars
        execfile("../../secrets.py")
        adwords_settings = GoogleAdWordsConnectionSettings(
            client_id=os.getenv("adwords_client_id"),
            user_agent="Tester",
            client_customer_id="1111111111",
            secrets_manager=GoogleAdWordsSecretsManager())
        target_table = "test_adwords_geo_performance_report"
        etl_settings = AdWordsReportsToAthenaSettings(
            source_query=(ReportQueryBuilder()
                          .Select(
                # Attributes
                'AccountDescriptiveName',
                'CampaignId',
                'CityCriteriaId',
                'CountryCriteriaId',
                'CustomerDescriptiveName',
                'ExternalCustomerId',
                'IsTargetingLocation',
                'MetroCriteriaId',
                'MostSpecificCriteriaId',
                'RegionCriteriaId',

                # Segments
                'Date',

                # Metrics
                'Impressions',
                'Clicks',
                'ConversionRate',
                'Conversions',
                'ConversionValue',
                'Cost',
                'CostPerConversion')
                          .From('GEO_PERFORMANCE_REPORT')
                          .During(
                start_date="20200601",
                end_date="20200701"
            )
                          .Build()),
            source_include_zero_impressions=False,
            source_connection_settings=adwords_settings,
            target_bucket=target_bucket,
            target_key_prefix=target_key_prefix,
            target_connection_settings=aws_setting,
            target_database="dev",
            target_table=target_table,
            target_table_ddl_progress=True,
            is_partitioned_table=True,
            partition_values=[("abc", "def"), ("pqr", 123)],
            target_file_prefix="data",
            transformation_field_type_mask={"country__territory": np.int,
                                            "region": np.int,
                                            "most_specific_location": np.int}
        )
        etl = AdWordsReportsToAthena(etl_settings)
        etl.transfer()
        etl.create_athena_table()
        etl.add_partitions()

        au = AthenaUtil(database="dev", conn=AwsConnectionManager(aws_setting),
                        output_bucket=os.environ["S3_TEST_BUCKET"])
        actual = au.run_query(query_string="""
            select * from dev.test_adwords_geo_performance_report limit 10
            """, return_result=True)
        print(actual)
        expected = 11

        self.assertEqual(expected, len(actual["ResultSet"]["Rows"]))

    def test__should__create_athena_table_settings__when__the_athena_columns_are_given(self):
        mock_util = Mock()
        mock_util._AdWordsToAthena__settings.is_partitioned_table = False
        mock_util._AdWordsToAthena__settings.target_table = 'test_athena_table'
        mock_util._AdWordsToAthena__settings.athena_columns = [
            {'column': 'field_1', 'type': 'STRING'},
            {'column': 'field_2', 'type': 'BIGINT'},
            {'column': 'field_3', 'type': 'STRING'},
            {'column': 'field_4', 'type': 'BOOLEAN'},
            {'column': 'field_5', 'type': 'array<int>'},
            {'column': 'field_6', 'type': 'DOUBLE'}]
        mock_util._AdWordsToAthena__settings.target_bucket = 'test-bucket'
        mock_util.base_dir = 'data/'
        actual = AdWordsToAthena._construct_athena_table_settings(mock_util, data=DataFrame(
            data=[
                {
                    "field_1": "sample str value",
                    "field_2": 343,
                    "field_3": None,
                    "field_4": True,
                    "field_5": [1, 2, 3],
                    "field_6": 2.3434,
                }
            ]
        ))
        expected = {'columns': [{'column': 'field_1', 'type': 'STRING'},
                                {'column': 'field_2', 'type': 'BIGINT'},
                                {'column': 'field_3', 'type': 'STRING'},
                                {'column': 'field_4', 'type': 'BOOLEAN'},
                                {'column': 'field_5', 'type': 'array<int>'},
                                {'column': 'field_6', 'type': 'DOUBLE'}],
                    'encryption': False,
                    'exists': True,
                    'partitions': [],
                    's3_bucket': 'test-bucket',
                    's3_dir': 'data/',
                    'storage_format_selector': 'parquet',
                    'table': 'test_athena_table'}

        self.assertEqual(expected, actual)

    def test__should__create_athena_table_settings__when__the_athena_columns_are_not_given(self):
        mock_util = Mock()
        mock_util._AdWordsToAthena__settings.is_partitioned_table = False
        mock_util._AdWordsToAthena__settings.target_table = 'test_athena_table'
        mock_util._AdWordsToAthena__settings.athena_columns = None
        mock_util._AdWordsToAthena__settings.target_bucket = 'test-bucket'
        mock_util.base_dir = 'data/'
        actual = AdWordsToAthena._construct_athena_table_settings(mock_util, data=DataFrame(
            data=[
                {
                    "field_1": "sample str value",
                    "field_2": 343,
                    "field_3": None,
                    "field_4": True,
                    "field_5": [1, 2, 3],
                    "field_6": 2.3434,
                }
            ]
        ))
        expected = {'columns': [{'column': 'field_1', 'type': 'STRING'},
                                {'column': 'field_2', 'type': 'BIGINT'},
                                {'column': 'field_3', 'type': 'STRING'},
                                {'column': 'field_4', 'type': 'BOOLEAN'},
                                {'column': 'field_5', 'type': 'STRING'},
                                {'column': 'field_6', 'type': 'DOUBLE'}],
                    'encryption': False,
                    'exists': True,
                    'partitions': [],
                    's3_bucket': 'test-bucket',
                    's3_dir': 'data/',
                    'storage_format_selector': 'parquet',
                    'table': 'test_athena_table'}

        self.assertEqual(expected, actual)