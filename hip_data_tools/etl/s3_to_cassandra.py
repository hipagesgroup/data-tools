"""
Module to deal with data transfer from S3 to Cassandra
"""
from typing import List

from cassandra.datastax.graph import Result

from hip_data_tools.apache.cassandra import CassandraUtil, CassandraConnectionManager
from hip_data_tools.aws.common import AwsConnectionManager
from hip_data_tools.aws.s3 import S3Util
from hip_data_tools.common import LOG
from hip_data_tools.etl.common import CassandraTableSink, S3DirectorySource
from hip_data_tools.etl.s3_to_dataframe import S3ToDataFrame


class S3ToCassandra(S3ToDataFrame):
    """
    Class to transfer parquet data from s3 to Cassandra
    """

    def __init__(self, source: S3DirectorySource, sink: CassandraTableSink):
        self.__source = source
        self.__sink = sink
        self.keys_to_transfer = None
        super().__init__(source)

    def _get_cassandra_util(self):
        return CassandraUtil(
            keyspace=self.__sink.keyspace,
            conn=CassandraConnectionManager(
                settings=self.__sink.connection_settings),
        )

    def _get_s3_util(self):
        return S3Util(
            bucket=self.__source.bucket,
            conn=AwsConnectionManager(self.__source.connection_settings),
        )

    def create_table(self):
        """
        Creates the destination cassandra table if not exists
        Returns: None
        """
        files = self.list_source_files()
        data_frame = self._get_s3_util().download_parquet_as_dataframe(
            key=files[0])
        self._get_cassandra_util().create_table_from_dataframe(
            data_frame=data_frame,
            table_name=self.__sink.table,
            primary_key_column_list=self.__sink.table_primary_keys,
            table_options_statement=self.__sink.table_options_statement,
        )

    def create_and_upsert_all(self) -> List[List[Result]]:
        """
        First creates the table and then upsert all s3 files to the table
        Returns: None
        """
        self.create_table()
        return self.upsert_all_files()

    def upsert_all_files(self) -> List[List[Result]]:
        """
        Upsert all files from s3 sequentially into cassandra
        Returns: None
        """
        return [self._upsert_data_frame(df) for df in self._get_iterator()]

    def _upsert_data_frame(self, data_frame):
        if self.__sink.batch_size > 1:
            LOG.info("Going to upsert batches of size %s", self.__sink.batch_size)
            result = self._get_cassandra_util().upsert_dataframe_in_batches(
                dataframe=data_frame,
                table=self.__sink.table,
                batch_size=self.__sink.batch_size)
        else:
            LOG.info("Going to upsert one row at a time")
            result = self._get_cassandra_util().upsert_dataframe(
                dataframe=data_frame,
                table=self.__sink.table)
        return result

    def upsert_file(self, key: str) -> List[Result]:
        """
        Read a parquet file from s3 and upsert the records to Cassandra
        Args:
            key: s3 key for the parquet file
        Returns: None
        """
        data_frame = self.get_data_frame(key)
        return self._upsert_data_frame(data_frame)
