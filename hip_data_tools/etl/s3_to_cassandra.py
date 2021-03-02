"""
Module to deal with data transfer from S3 to Cassandra
"""
from typing import Optional, List

from attr import dataclass
from cassandra.datastax.graph import Result

from hip_data_tools.apache.cassandra import CassandraUtil, CassandraConnectionManager, \
    CassandraConnectionSettings
from hip_data_tools.aws.common import AwsConnectionManager
from hip_data_tools.aws.s3 import S3Util
from hip_data_tools.common import LOG
from hip_data_tools.etl.s3_to_dataframe import S3ToDataFrame, S3ToDataFrameSettings


@dataclass
class S3ToCassandraSettings(S3ToDataFrameSettings):
    """S3 to Cassandra ETL settings"""
    destination_keyspace: str
    destination_table: str
    destination_table_primary_keys: List[str]
    destination_table_partition_key: Optional[List[str]]
    destination_connection_settings: CassandraConnectionSettings
    destination_table_options_statement: str = ""
    destination_batch_size: int = 1


class S3ToCassandra(S3ToDataFrame):
    """
    Class to transfer parquet data from s3 to Cassandra
    Args:
        settings (S3ToCassandraSettings): the settings around the etl to be executed
    """

    def __init__(self, settings: S3ToCassandraSettings):
        self.__settings = settings
        super().__init__(self.__settings)
        self.keys_to_transfer = None

    def _get_cassandra_util(self):
        return CassandraUtil(
            keyspace=self.__settings.destination_keyspace,
            conn=CassandraConnectionManager(
                settings=self.__settings.destination_connection_settings),
        )

    def _get_s3_util(self):
        return S3Util(
            bucket=self.__settings.source_bucket,
            conn=AwsConnectionManager(self.__settings.source_connection_settings),
        )

    def create_table(self):
        """
        Creates the destination cassandra table if not exists
        Returns: None
        """
        files = self.list_source_files()
        data_frame = self._get_s3_util().download_parquet_as_dataframe(
            key=files[0])
        # use specified partition and clustering keys
        self._get_cassandra_util().create_table_from_dataframe(
            data_frame=data_frame,
            table_name=self.__settings.destination_table,
            primary_key_column_list=self.__settings.destination_table_primary_keys,
            partition_key_column_list=self.__settings.destination_table_partition_key,
            table_options_statement=self.__settings.destination_table_options_statement,
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
        if self.__settings.destination_batch_size > 1:
            LOG.info("Going to upsert batches of size %s", self.__settings.destination_batch_size)
            result = self._get_cassandra_util().upsert_dataframe_in_batches(
                dataframe=data_frame,
                table=self.__settings.destination_table,
                batch_size=self.__settings.destination_batch_size)
        else:
            LOG.info("Going to upsert one row at a time")
            result = self._get_cassandra_util().upsert_dataframe(
                dataframe=data_frame,
                table=self.__settings.destination_table)
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
