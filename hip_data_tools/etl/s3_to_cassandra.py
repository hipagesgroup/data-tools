"""
Module to deal with data transfer from S3 to Cassandra
"""
import logging as log

from attr import dataclass

from hip_data_tools.apache.cassandra import CassandraUtil, CassandraConnectionManager, \
    CassandraConnectionSettings
from hip_data_tools.aws.common import AwsConnectionSettings, AwsConnectionManager
from hip_data_tools.aws.s3 import S3Util


@dataclass
class S3ToCassandraSettings:
    """S3 to Cassandra ETL settings"""
    source_bucket: str
    source_key_prefix: str
    source_connection_settings: AwsConnectionSettings
    destination_keyspace: str
    destination_table: str
    destination_table_primary_keys: list
    destination_table_options_statement: str
    destination_connection_settings: CassandraConnectionSettings


class S3ToCassandra:
    """
    Class to transfer parquet data from s3 to Cassandra
    Args:
        settings (S3ToCassandraSettings): the settings around the etl to be executed
    """

    def __init__(self, settings: S3ToCassandraSettings):
        self.settings = settings
        self._cassandra = CassandraUtil(
            keyspace=self.settings.destination_keyspace,
            conn=CassandraConnectionManager(settings=self.settings.destination_connection_settings),
        )
        self._s3 = S3Util(
            bucket=self.settings.source_bucket,
            conn=AwsConnectionManager(self.settings.source_connection_settings),
        )
        self.keys_to_transfer = None

    def create_table(self):
        """
        Creates the destination cassandra table if not exists
        Returns: None
        """
        data_frame = self._s3.download_df_parquet(s3_key=self._list_source_files()[0])
        self._cassandra.create_table_from_dataframe(
            data_frame=data_frame,
            table_name=self.settings.destination_table,
            primary_key_column_list=self.settings.destination_table_primary_keys,
            table_options_statement=self.settings.destination_table_options_statement,
        )

    def create_and_upsert_all(self):
        """
        First creates the table and then upserts all s3 files to the table
        Returns: None
        """
        self.create_table()
        self.upsert_all()

    def upsert_all(self):
        """
        Upsert all files from s3 sequentially into cassandra
        Returns: None
        """
        for key in self._list_source_files():
            self._upsert_object(key)

    def _upsert_object(self, key):
        data_frame = self._s3.download_df_parquet(s3_key=key)
        self._cassandra.upsert_dataframe(dataframe=data_frame,
                                         table=self.settings.destination_table)

    def _list_source_files(self):
        if self.keys_to_transfer is None:
            self.keys_to_transfer = self._s3.list_objects(self.settings.source_key_prefix)
            log.info("Listed and cached %s source files", len(self.keys_to_transfer))
        return self.keys_to_transfer
