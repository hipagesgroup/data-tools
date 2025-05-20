"""
Module to deal with data transfer from S3 to Cassandra
"""
from typing import Optional, List

from dataclasses import dataclass

import pandas as pd
from cassandra.datastax.graph import Result
from cassandra import ConsistencyLevel

from hip_data_tools.apache.cassandra import CassandraUtil, CassandraConnectionManager, \
    CassandraConnectionSettings
from hip_data_tools.common import LOG


@dataclass
class DataFrameToCassandraSettings:
    """DataFrame to Cassandra ETL settings"""
    data_frame: pd.DataFrame
    destination_keyspace: str
    destination_table: str
    destination_table_primary_keys: List[str]
    destination_table_partition_key: Optional[List[str]]
    destination_connection_settings: CassandraConnectionSettings
    destination_table_options_statement: str = ""
    destination_batch_size: int = 1


class DataFrameToCassandra:
    """
    Class to transfer parquet data from s3 to Cassandra
    Args:
        settings (DataFrameToCassandraSettings): the settings around the etl to be executed
    """

    def __init__(self, settings: DataFrameToCassandraSettings):
        self.__settings = settings
        self.keys_to_transfer = None

    def _get_cassandra_util(self):
        return CassandraUtil(
            keyspace=self.__settings.destination_keyspace,
            conn=CassandraConnectionManager(
                settings=self.__settings.destination_connection_settings,consistency_level=ConsistencyLevel.LOCAL_QUORUM),
        )

    def create_table(self):
        """
        Creates the destination cassandra table if not exists
        Returns: None
        """
        # use specified partition and clustering keys
        self._get_cassandra_util().create_table_from_dataframe(
            data_frame=self.__settings.data_frame,
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
            return self._get_cassandra_util().upsert_dataframe_in_batches(
                dataframe=data_frame, table=self.__settings.destination_table, batch_size=self.__settings.destination_batch_size)

        else:
            LOG.info("Going to upsert one row at a time")
            return self._get_cassandra_util().upsert_dataframe(
                dataframe=data_frame, table=self.__settings.destination_table)

    def upsert_dataframe(self) -> List[Result]:
        """
        Read a parquet file from s3 and upsert the records to Cassandra
        Args:
            key: s3 key for the parquet file
        Returns: None
        """
        return self._upsert_data_frame(self.__settings.data_frame)
