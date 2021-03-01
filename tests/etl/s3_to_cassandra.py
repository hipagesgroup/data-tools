import os
import uuid
from unittest import TestCase
from unittest.mock import Mock

from moto import mock_s3
import pandas as pd

from hip_data_tools.aws.common import AwsConnectionManager, AwsConnectionSettings, AwsSecretsManager
from hip_data_tools.aws.s3 import S3Util
from hip_data_tools.etl.s3_to_cassandra import S3ToCassandra, S3ToCassandraSettings, CassandraUtil, CassandraConnectionManager, CassandraSecretsManager, CassandraConnectionSettings
from cassandra.policies import DCAwareRoundRobinPolicy

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
            region="us-east-1",
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
            destination_table_partition_key=[],
            destination_table_clustering_keys=[],
            destination_table_options_statement="",
            destination_batch_size=2,
            destination_connection_settings=Mock(),
        ))

        actual = util.list_source_files()
        self.assertListEqual(actual, expected)


    @mock_s3
    def test__create_table_statement__should_generate_create_table_cql(self):
        load_balancing_policy = DCAwareRoundRobinPolicy(local_dc='AWS_VPC_AP_SOUTHEAST_2')
        cassandra_conn_settings= CassandraConnectionManager(
            settings = CassandraConnectionSettings(
                cluster_ips=['127.0.0.1'],
                port=9042,
                load_balancing_policy=load_balancing_policy,
                secrets_manager=CassandraSecretsManager(),
                ssl_options = None
            )
        )
        df = pd.DataFrame({'col1': ['a', 'a', 'b', 'b'], 'col2': [1, 2, 3, 4], 'col3': [5, 6, 7, 8],
                           'col4': ['x', 'x', 'y', 'y'], 'col5': [1, 2, 1, 2], 'col6': [1, 2, 1, 2]})
        test_table = "test"
        test_partition_key = ['col1', 'col3']
        test_cluster_keys = ['col2', 'col4']
        test_columns = ['col5', 'col6']
        test_source_key_prefix = "test_for_s3_to_cassandra"
        util = CassandraUtil(keyspace= test_source_key_prefix, conn=cassandra_conn_settings)
        expected = f"""
        CREATE TABLE IF NOT EXISTS {test_source_key_prefix}.{test_table} (
            {", ".join(test_columns)},
                PRIMARY KEY (({", ".join(test_partition_key)}),{", ".join(test_cluster_keys)})
            )"""
        actual = util._dataframe_to_cassandra_ddl(data_frame=df,
                                                  partition_key_column_list=test_partition_key,
                                                  clustering_key_column_list=test_cluster_keys,
                                                  table_name=test_table)
        self.assertListEqual(actual, expected)
