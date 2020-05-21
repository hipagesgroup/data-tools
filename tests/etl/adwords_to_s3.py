import os
from unittest import TestCase

from googleads.adwords import ServiceQueryBuilder
from py._builtin import execfile

from hip_data_tools.aws.common import AwsConnectionManager, AwsConnectionSettings, AwsSecretsManager
from hip_data_tools.aws.s3 import S3Util
from hip_data_tools.etl.adwords_to_s3 import AdWordsToS3Settings, AdWordsToS3
from hip_data_tools.google.adwords import GoogleAdWordsConnectionSettings, \
    GoogleAdWordsSecretsManager


class TestAdwordsToS3(TestCase):

    def test__should__get_correct_estimations__with__etl_get_parallel_payloads(self):
        # Load secrets via env vars
        execfile("../../secrets.py")

        aws_setting = AwsConnectionSettings(
            region="ap-southeast-2",
            secrets_manager=AwsSecretsManager(),
            profile=None)
        target_bucket = os.getenv('S3_TEST_BUCKET')
        target_key_prefix = "something/test"

        adwords_settings = GoogleAdWordsConnectionSettings(
            client_id=os.getenv("adwords_client_id"),
            user_agent="Tester",
            client_customer_id=os.getenv("adwords_client_customer_id"),
            secrets_manager=GoogleAdWordsSecretsManager())

        etl_settings = AdWordsToS3Settings(
            source_query_fragment=ServiceQueryBuilder().Select('Id').OrderBy('Id'),
            source_service="AdGroupAdService",
            source_service_version="v201809",
            source_connection_settings=adwords_settings,
            target_bucket=target_bucket,
            target_key_prefix=target_key_prefix,
            target_file_prefix=None,
            target_connection_settings=aws_setting
        )
        etl = AdWordsToS3(etl_settings)

        actual_payloads = etl.get_parallel_payloads(page_size=1000, number_of_workers=3)
        expected_payloads = [
            {'worker': 0, 'start_index': 0, 'number_of_pages': 0, 'page_size': 1000},
            {'worker': 1, 'start_index': 0, 'number_of_pages': 0, 'page_size': 1000},
            {'worker': 2, 'start_index': 0, 'number_of_pages': 0, 'page_size': 1000}]

        self.assertListEqual(expected_payloads, actual_payloads)

    def test__should__transfer_correct_amount_of_files__with__one_parallel_fragment(self):
        # Load secrets via env vars
        execfile("../../secrets.py")
        aws_setting = AwsConnectionSettings(
            region="ap-southeast-2",
            secrets_manager=AwsSecretsManager(),
            profile=None)
        target_bucket = os.getenv('S3_TEST_BUCKET')
        target_key_prefix = "tmp/test/hip_data_tools/adwords_to_s3/test"

        adwords_settings = GoogleAdWordsConnectionSettings(
            client_id=os.getenv("adwords_client_id"),
            user_agent="Tester",
            client_customer_id=os.getenv("adwords_client_customer_id"),
            secrets_manager=GoogleAdWordsSecretsManager())

        etl_settings = AdWordsToS3Settings(
            source_query_fragment=ServiceQueryBuilder().Select('Id').OrderBy('Id'),
            source_service="AdGroupAdService",
            source_service_version="v201809",
            source_connection_settings=adwords_settings,
            target_bucket=target_bucket,
            target_key_prefix=target_key_prefix,
            target_file_prefix=None,
            target_connection_settings=aws_setting
        )
        etl = AdWordsToS3(etl_settings)
        etl.build_query(start_index=786000, page_size=5, num_iterations=2)

        etl.transfer_all()
        conn = AwsConnectionManager(aws_setting)
        s3u = S3Util(conn=conn, bucket=target_bucket)
        actual = s3u.get_keys(target_key_prefix)
        print(actual)
        expected = ['tmp/test/hip_data_tools/adwords_to_s3/test/348937894index_0__99.parquet',
                    'tmp/test/hip_data_tools/adwords_to_s3/test/index_786000__786004.parquet',
                    'tmp/test/hip_data_tools/adwords_to_s3/test/index_786005__786009.parquet']
        self.assertListEqual(expected, actual)
