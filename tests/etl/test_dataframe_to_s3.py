from unittest import TestCase

from moto import mock_s3
from pandas import DataFrame

from hip_data_tools.aws.common import AwsConnectionManager, AwsConnectionSettings, AwsSecretsManager
from hip_data_tools.aws.s3 import S3Util
from hip_data_tools.etl.dataframe_to_s3 import DataFrameToS3, DataFrameToS3Settings


class TestDataFrameToS3(TestCase):

    @mock_s3
    def test__should_be_able_to_work_with_dataframe(self):
        aws_setting = AwsConnectionSettings(
            region="ap-southeast-2",
            secrets_manager=AwsSecretsManager(),
            profile=None)
        target_bucket = "TEST_TARGET_BUCKET"
        conn = AwsConnectionManager(aws_setting)
        s3_util_for_destination = S3Util(conn=conn, bucket=target_bucket)
        s3_util_for_destination.create_bucket()
        target_prefix = "target/prefix"
        DataFrameToS3(
            DataFrameToS3Settings(
                source_dataframe=DataFrame(),
                target_bucket=target_bucket,
                target_key_prefix=target_prefix,
                target_file_name="data",
                target_connection_settings=aws_setting
            )
        ).upload()
        actual = s3_util_for_destination.get_all_keys(key_prefix=target_prefix)
        expected_source_list = ["target/prefix/data.parquet"]
        self.assertListEqual(actual, expected_source_list)
