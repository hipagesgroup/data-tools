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
            {'worker': 0, 'start_index': 0, 'number_of_pages': 435, 'page_size': 1000},
            {'worker': 1, 'start_index': 435000, 'number_of_pages': 435, 'page_size': 1000},
            {'worker': 2, 'start_index': 870000, 'number_of_pages': 435, 'page_size': 1000}]

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
        conn = AwsConnectionManager(aws_setting)
        s3u = S3Util(conn=conn, bucket=target_bucket)
        s3u.delete_recursive(target_key_prefix)
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
        etl.build_query(start_index=0, page_size=5, num_iterations=2)

        etl.transfer_all()

        actual = s3u.get_keys(target_key_prefix)
        print(actual)
        expected = ['tmp/test/hip_data_tools/adwords_to_s3/test/index_0__4.parquet',
                    'tmp/test/hip_data_tools/adwords_to_s3/test/index_5__9.parquet']
        self.assertListEqual(expected, actual)

    def test__should__add_file_name_prefix__when__file_name_prefix_is_provided(self):
        # Load secrets via env vars
        execfile("../../secrets.py")
        aws_setting = AwsConnectionSettings(
            region="ap-southeast-2",
            secrets_manager=AwsSecretsManager(),
            profile=None)
        target_bucket = os.getenv('S3_TEST_BUCKET')
        target_key_prefix = "tmp/test/hip_data_tools/adwords_to_s3/test"
        conn = AwsConnectionManager(aws_setting)
        s3u = S3Util(conn=conn, bucket=target_bucket)
        s3u.delete_recursive(target_key_prefix)
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
            target_file_prefix="12345678",
            target_connection_settings=aws_setting
        )
        etl = AdWordsToS3(etl_settings)
        etl.build_query(start_index=786000, page_size=5, num_iterations=2)

        etl.transfer_all()

        actual = s3u.get_keys(target_key_prefix)
        expected = [
            'tmp/test/hip_data_tools/adwords_to_s3/test/12345678index_786000__786004.parquet',
            'tmp/test/hip_data_tools/adwords_to_s3/test/12345678index_786005__786009.parquet']

        self.assertListEqual(expected, actual)

    def test__should__create_s3_file_for_the_given_indices(self):
        # Load secrets via env vars
        execfile("../../secrets.py")

        aws_setting = AwsConnectionSettings(
            region="ap-southeast-2",
            secrets_manager=AwsSecretsManager(),
            profile=None)
        target_bucket = os.getenv('S3_TEST_BUCKET')
        target_key_prefix = "something/test"
        conn = AwsConnectionManager(aws_setting)
        s3u = S3Util(conn=conn, bucket=target_bucket)
        s3u.delete_recursive(target_key_prefix)
        adwords_settings = GoogleAdWordsConnectionSettings(
            client_id=os.getenv("adwords_client_id"),
            user_agent="Tester",
            client_customer_id=os.getenv("adwords_client_customer_id"),
            secrets_manager=GoogleAdWordsSecretsManager())

        adword_to_s3_util = AdWordsToS3(
            settings=AdWordsToS3Settings(
                source_query_fragment=ServiceQueryBuilder().Select(
                    # Attributes
                    'BaseAdGroupId',
                    'Id',
                    'CampaignId',
                    'CampaignName',
                    'Name',
                    'Status',
                    'ContentBidCriterionTypeGroup',
                    'BaseCampaignId',
                    'TrackingUrlTemplate',
                    'FinalUrlSuffix',
                    'UrlCustomParameters',
                    'AdGroupType').OrderBy('Id'),
                source_service="AdGroupService",
                source_service_version="v201809",
                source_connection_settings=adwords_settings,
                target_bucket=target_bucket,
                target_key_prefix=target_key_prefix,
                target_file_prefix=None,
                target_connection_settings=aws_setting
            )
        )
        adword_to_s3_util.build_query(
            start_index=35000,
            page_size=1000,
            num_iterations=1
        )
        adword_to_s3_util.transfer_all()
        actual = s3u.get_keys(target_key_prefix)
        expected = ['tmp/test/hip_data_tools/adwords_to_s3/test/index_35000__35999.parquet']

        self.assertListEqual(expected, actual)
