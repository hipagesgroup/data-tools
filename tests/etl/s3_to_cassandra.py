import uuid
from unittest import TestCase
from unittest.mock import Mock

from moto import mock_s3

from hip_data_tools.aws.common import AwsConnectionManager, AwsConnectionSettings, AwsSecretsManager
from hip_data_tools.aws.s3 import S3Util
from hip_data_tools.etl.common import S3DirectorySource, CassandraTableSink
from hip_data_tools.etl.s3_to_cassandra import S3ToCassandra


class TestS3ToCassandra(TestCase):

    @mock_s3
    def test__list_source_files__should_list_the_correct_source_files_as_an_array(self):
        sample_file_content = [{"sample": str(uuid.uuid4())}]
        test_bucket = "test"
        test_source_key_prefix = "test_for_s3_to_cassandra"
        aws_conn_settings = AwsConnectionSettings(
            region="ap-southeast-2",
            secrets_manager=AwsSecretsManager(),
            profile=None)
        s3 = S3Util(conn=AwsConnectionManager(aws_conn_settings), bucket=test_bucket)
        s3.create_bucket()
        for itr in range(3):
            s3.upload_json(
                key=f"{test_source_key_prefix}/{itr}.abc",
                json_list=sample_file_content)
        expected = [
            f"{test_source_key_prefix}/0.abc",
            f"{test_source_key_prefix}/1.abc",
            f"{test_source_key_prefix}/2.abc", ]

        util = S3ToCassandra(
            source=S3DirectorySource(
                connection_settings=aws_conn_settings,
                bucket=test_bucket,
                directory_key=test_source_key_prefix
            ),
            sink=CassandraTableSink(
                connection_settings=Mock(),
                keyspace="test",
                table="test",
                table_primary_keys=[],
                destination_table_options_statement="",
                destination_batch_size=2
            ),
        )

        actual = util.list_source_files()
        self.assertListEqual(actual, expected)
