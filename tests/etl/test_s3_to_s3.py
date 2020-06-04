from tempfile import NamedTemporaryFile
from unittest import TestCase

from moto import mock_s3

from hip_data_tools.aws.common import AwsConnectionManager, AwsConnectionSettings, AwsSecretsManager
from hip_data_tools.aws.s3 import S3Util
from hip_data_tools.etl.common import S3DirectorySuffixSource, S3DirectorySink
from hip_data_tools.etl.s3_to_s3 import S3ToS3


class TestS3ToS3(TestCase):

    @mock_s3
    def test__list_source_files__should_work_without_suffix(self):
        aws_setting = AwsConnectionSettings(
            region="ap-southeast-2",
            secrets_manager=AwsSecretsManager(),
            profile=None)
        source_bucket = "TEST_SOURCE_BUCKET"
        target_bucket = "TEST_TARGET_BUCKET"
        conn = AwsConnectionManager(aws_setting)
        s3_util_for_destination = S3Util(conn=conn, bucket=target_bucket)
        s3_util_for_source = S3Util(conn=conn, bucket=source_bucket)

        s3_util_for_source.create_bucket()
        s3_util_for_destination.create_bucket()

        file = NamedTemporaryFile("w+", delete=False)
        file.write(str("Test file content"))

        s3_util_for_source.upload_file(file.name, "source/prefix/test_file.txt")
        source = S3DirectorySuffixSource(
            connection_settings=aws_setting,
            bucket=source_bucket,
            directory_key="source/prefix",
            suffix=None
        )
        sink = S3DirectorySink(
            bucket=target_bucket,
            connection_settings=aws_setting,
            directory_key="target/prefix",
            file_prefix=None
        )
        etl = S3ToS3(source=source, sink=sink)
        expected_source_list = ['source/prefix/test_file.txt']
        self.assertListEqual(etl.list_source_files(), expected_source_list)

    @mock_s3
    def test__list_source_files__should_work_with_suffix(self):
        aws_setting = AwsConnectionSettings(
            region="ap-southeast-2",
            secrets_manager=AwsSecretsManager(),
            profile=None)
        source_bucket = "TEST_SOURCE_BUCKET"
        target_bucket = "TEST_TARGET_BUCKET"
        conn = AwsConnectionManager(aws_setting)
        s3_util_for_destination = S3Util(conn=conn, bucket=target_bucket)
        s3_util_for_source = S3Util(conn=conn, bucket=source_bucket)

        s3_util_for_source.create_bucket()
        s3_util_for_destination.create_bucket()

        file = NamedTemporaryFile("w+", delete=False)
        file.write(str("Test file content"))

        s3_util_for_source.upload_file(file.name, "source/prefix/test_file.txt")

        file2 = NamedTemporaryFile("w+", delete=False)
        file2.write(str("Test file content"))
        s3_util_for_source.upload_file(file2.name, "source/prefix/txt_file.parquet")

        source = S3DirectorySuffixSource(
            connection_settings=aws_setting,
            bucket=source_bucket,
            directory_key="source/prefix",
            suffix=".txt"
        )
        sink = S3DirectorySink(
            bucket=target_bucket,
            connection_settings=aws_setting,
            directory_key="target/prefix",
            file_prefix=None
        )
        etl = S3ToS3(source=source, sink=sink)
        expected_source_list = ['source/prefix/test_file.txt']
        self.assertListEqual(etl.list_source_files(), expected_source_list)

    @mock_s3
    def test__transfer_file__should_work(self):
        aws_setting = AwsConnectionSettings(
            region="ap-southeast-2",
            secrets_manager=AwsSecretsManager(),
            profile=None)
        source_bucket = "TEST_SOURCE_BUCKET"
        target_bucket = "TEST_TARGET_BUCKET"
        conn = AwsConnectionManager(aws_setting)
        s3_util_for_destination = S3Util(conn=conn, bucket=target_bucket)
        s3_util_for_source = S3Util(conn=conn, bucket=source_bucket)

        s3_util_for_source.create_bucket()
        s3_util_for_destination.create_bucket()

        file = NamedTemporaryFile("w+", delete=False)
        file.write(str("Test file content"))

        s3_util_for_source.upload_file(file.name, "source/prefix/test_file.txt")

        file2 = NamedTemporaryFile("w+", delete=False)
        file2.write(str("Test file content"))
        s3_util_for_source.upload_file(file2.name, "source/prefix/txt_file.parquet")

        source = S3DirectorySuffixSource(
            connection_settings=aws_setting,
            bucket=source_bucket,
            directory_key="source/prefix",
            suffix=".txt"
        )
        sink = S3DirectorySink(
            bucket=target_bucket,
            connection_settings=aws_setting,
            directory_key="target/prefix",
            file_prefix=None
        )
        etl = S3ToS3(source=source, sink=sink)
        etl.transfer_file("source/prefix/test_file.txt")
        actual = s3_util_for_destination.get_keys("")
        expected_destination_keys = ['target/prefix/test_file.txt']
        self.assertListEqual(expected_destination_keys, actual)

    @mock_s3
    def test__transfer_all_files__should_work(self):
        aws_setting = AwsConnectionSettings(
            region="ap-southeast-2",
            secrets_manager=AwsSecretsManager(),
            profile=None)
        source_bucket = "TEST_SOURCE_BUCKET"
        target_bucket = "TEST_TARGET_BUCKET"
        conn = AwsConnectionManager(aws_setting)
        s3_util_for_destination = S3Util(conn=conn, bucket=target_bucket)
        s3_util_for_source = S3Util(conn=conn, bucket=source_bucket)

        s3_util_for_source.create_bucket()
        s3_util_for_destination.create_bucket()

        for itr in range(10):
            file = NamedTemporaryFile("w+", delete=False)
            file.write(str("Test file content"))
            s3_util_for_source.upload_file(file.name, f"source/prefix/txt_file{itr}.parquet")

        source = S3DirectorySuffixSource(
            connection_settings=aws_setting,
            bucket=source_bucket,
            directory_key="source/prefix",
            suffix=None
        )
        sink = S3DirectorySink(
            bucket=target_bucket,
            connection_settings=aws_setting,
            directory_key="target/prefix",
            file_prefix=None
        )
        etl = S3ToS3(source=source, sink=sink)
        etl.transfer_all_files()
        actual = s3_util_for_destination.get_keys("")
        expected_destination_keys = [f'target/prefix/txt_file{itr}.parquet' for itr in range(10)]
        self.assertListEqual(expected_destination_keys, actual)
