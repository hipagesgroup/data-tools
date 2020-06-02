import os
from unittest import TestCase

from googleads.adwords import ServiceQueryBuilder, ReportQueryBuilder
from py._builtin import execfile

from hip_data_tools.aws.athena import AthenaUtil
from hip_data_tools.aws.common import AwsConnectionManager, AwsConnectionSettings, AwsSecretsManager
from hip_data_tools.etl.adwords_to_athena import AdWordsToAthena, \
    AdWordsReportsToAthena
from hip_data_tools.etl.common import AdWordsServiceSource, AthenaTableDirectorySink, \
    AdWordsReportSource
from hip_data_tools.google.adwords import GoogleAdWordsConnectionSettings, \
    GoogleAdWordsSecretsManager


class TestAdwordsToS3(TestCase):

    def test__should__get_correct_estimations__with__etl_get_parallel_payloads(self):
        aws_setting = AwsConnectionSettings(
            region="ap-southeast-2",
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
        source = AdWordsServiceSource(
            query_fragment=ServiceQueryBuilder().Select('Id').OrderBy('Id'),
            service="AdGroupAdService",
            service_version="v201809",
            connection_settings=adwords_settings,
        )
        sink = AthenaTableDirectorySink(
            database="dev",
            query_result_bucket=target_bucket,
            query_result_key="result_key/1/",
            connection_settings=aws_setting,
            table=target_table,
            table_ddl_progress=True,
            s3_data_location_bucket=target_bucket,
            s3_data_location_directory_key=target_key_prefix,
            partition_value=[("abc", "def"), ("pqr", 123)],
        )
        etl = AdWordsToAthena(
            source=source,
            sink=sink,
        )

        actual_payloads = etl.get_parallel_payloads(page_size=1000, number_of_workers=3)
        expected_payloads = [
            {
                'number_of_pages': 435,
                'page_size': 1000,
                'start_index': 0,
                'worker': 0
            },
            {
                'number_of_pages': 435,
                'page_size': 1000,
                'start_index': 435000,
                'worker': 1
            },
            {
                'number_of_pages': 435,
                'page_size': 1000,
                'start_index': 870000,
                'worker': 2
            }
        ]
        self.assertListEqual(expected_payloads, actual_payloads)
        etl.create_athena_table()
        conn = AwsConnectionManager(aws_setting)
        au = AthenaUtil(sink, conn)
        actual = au.get_glue_table_metadata(target_table)
        print(actual)

    def test__should__create_table__with__a_general_report(self):
        aws_setting = AwsConnectionSettings(
            region="ap-southeast-2",
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
        source = AdWordsReportSource(
            query=(ReportQueryBuilder()
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
            connection_settings=adwords_settings,
            include_zero_impressions=True,
        )
        sink = AthenaTableDirectorySink(
            database="dev",
            query_result_bucket=target_bucket,
            query_result_key="result_key/1/",
            connection_settings=aws_setting,
            table=target_table,
            table_ddl_progress=True,
            s3_data_location_bucket=target_bucket,
            s3_data_location_directory_key=target_key_prefix,
            partition_value=[("abc", "def"), ("pqr", 123)],
            file_prefix="data",
        )
        etl = AdWordsReportsToAthena(source=source, sink=sink)
        etl.transfer()
        etl.create_athena_table()
        etl.add_partitions()

        au = AthenaUtil(settings=sink, conn=AwsConnectionManager(aws_setting))
        actual = au.run_query(query_string="""
        select * from dev.test_adwords_negative_report limit 10
        """, return_result=True)
        print(actual)
        expected = 11

        self.assertEqual(expected, len(actual["ResultSet"]["Rows"]))
