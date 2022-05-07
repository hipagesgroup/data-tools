from tempfile import NamedTemporaryFile
from unittest import TestCase
from moto import mock_s3
from hip_data_tools.aws.common import AwsConnectionManager, AwsConnectionSettings, AwsSecretsManager
from hip_data_tools.aws.s3 import S3Util
from hip_data_tools.etl import s3
from hip_data_tools.etl.s3 import AddTargetS3KeyTransformer
from hip_data_tools.etl.s3_to_s3 import S3ToS3FileCopy


class TestS3ToS3(TestCase):

    @mock_s3
    def test__list_source_files__should_work_without_suffix(self):
        aws_setting = AwsConnectionSettings(
            region="us-east-1",
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
        etl = S3ToS3FileCopy(
            source=s3.S3SourceSettings(
                bucket=source_bucket,
                key_prefix="source/prefix",
                suffix=None,
                connection_settings=aws_setting,
            ),
            sink=s3.S3SinkSettings(
                bucket=target_bucket,
                connection_settings=aws_setting,
            ),
            transformers=[AddTargetS3KeyTransformer(target_key_prefix="target/prefix")],
        )
        expected_source_list = ['source/prefix/test_file.txt']
        self.assertListEqual(etl.list_source_files(), expected_source_list)

    @mock_s3
    def test__list_source_files__should_work_with_suffix(self):
        aws_setting = AwsConnectionSettings(
            region="us-east-1",
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
        etl = S3ToS3FileCopy(
            source=s3.S3SourceSettings(
                bucket=source_bucket,
                key_prefix="source/prefix",
                suffix=".txt",
                connection_settings=aws_setting,
            ),
            sink=s3.S3SinkSettings(
                bucket=target_bucket,
                connection_settings=aws_setting,
            ),
            transformers=[AddTargetS3KeyTransformer(target_key_prefix="target/prefix")],
        )
        expected_source_list = ['source/prefix/test_file.txt']
        self.assertListEqual(etl.list_source_files(), expected_source_list)

    @mock_s3
    def test__transfer_file__should_work(self):
        aws_setting = AwsConnectionSettings(
            region="us-east-1",
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
        etl = S3ToS3FileCopy(
            source=s3.S3SourceSettings(
                bucket=source_bucket,
                key_prefix="source/prefix",
                suffix=".txt",
                connection_settings=aws_setting,
            ),
            sink=s3.S3SinkSettings(
                bucket=target_bucket,
                connection_settings=aws_setting,
            ),
            transformers=[AddTargetS3KeyTransformer(target_key_prefix="target/prefix")],
        )
        etl.execute_next()
        actual = s3_util_for_destination.get_keys("")
        expected_destination_keys = ['target/prefix/test_file.txt']
        self.assertListEqual(expected_destination_keys, actual)

    @mock_s3
    def test__transfer_all_files__should_work(self):
        aws_setting = AwsConnectionSettings(
            region="us-east-1",
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

        etl = S3ToS3FileCopy(
            source=s3.S3SourceSettings(
                bucket=source_bucket,
                key_prefix="source/prefix",
                suffix=None,
                connection_settings=aws_setting,
            ),
            sink=s3.S3SinkSettings(
                bucket=target_bucket,
                connection_settings=aws_setting,
            ),
            transformers=[AddTargetS3KeyTransformer(target_key_prefix="target/prefix")],
        )
        etl.execute_all()
        actual = s3_util_for_destination.get_keys("")
        expected_destination_keys = [f'target/prefix/txt_file{itr}.parquet' for itr in range(10)]
        self.assertListEqual(expected_destination_keys, actual)

    @mock_s3
    def test__transfer_all_files__should_work_without_transformers(self):
        aws_setting = AwsConnectionSettings(
            region="us-east-1",
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

        etl = S3ToS3FileCopy(
            source=s3.S3SourceSettings(
                bucket=source_bucket,
                key_prefix="source/prefix",
                suffix=None,
                connection_settings=aws_setting,
            ),
            sink=s3.S3SinkSettings(
                bucket=target_bucket,
                connection_settings=aws_setting,
            ),
        )
        etl.execute_all()
        actual = s3_util_for_destination.get_keys("")
        expected_destination_keys = [f'source/prefix/txt_file{itr}.parquet' for itr in range(10)]
        self.assertListEqual(expected_destination_keys, actual)
