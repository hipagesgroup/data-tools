import os
import uuid
from unittest import TestCase
from unittest.mock import Mock

from moto import mock_s3

from hip_data_tools.aws.common import AwsConnectionManager, AwsConnectionSettings, AwsSecretsManager
from hip_data_tools.aws.s3 import S3Util
from hip_data_tools.etl.s3_to_cassandra import S3ToCassandra, S3ToCassandraSettings


class TestS3ToCassandra(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sample_file_location = "./test_sample_TestS3ToCassandra.txt"
        cls.sample_file_content = str(uuid.uuid4())
        with open(cls.sample_file_location, 'w+') as f:
            f.write(cls.sample_file_content)

    @classmethod
    def tearDownClass(cls):
        os.remove(cls.sample_file_location)

    @mock_s3
    def test__list_source_files__should_list_the_correct_source_files_as_an_array(self):
        test_bucket = "test"
        test_source_key_prefix = "test_for_s3_to_cassandra"
        aws_conn_settings = AwsConnectionSettings(
            region="ap-southeast-2",
            secrets_manager=AwsSecretsManager(),
            profile=None)
        s3 = S3Util(conn=AwsConnectionManager(aws_conn_settings), bucket=test_bucket)
        s3.create_bucket()
        for itr in range(3):
            s3.upload_file(self.sample_file_location, f"{test_source_key_prefix}/{itr}.abc")
        expected = [
            f"{test_source_key_prefix}/0.abc",
            f"{test_source_key_prefix}/1.abc",
            f"{test_source_key_prefix}/2.abc", ]

        util = S3ToCassandra(S3ToCassandraSettings(
            source_bucket=test_bucket,
            source_key_prefix=test_source_key_prefix,
            source_connection_settings=aws_conn_settings,
            destination_keyspace="test",
            destination_table="test",
            destination_table_primary_keys=[],
            destination_table_options_statement="",
            destination_batch_size=2,
            destination_connection_settings=Mock(),
        ))

        actual = util.list_source_files()
        self.assertListEqual(actual, expected)
