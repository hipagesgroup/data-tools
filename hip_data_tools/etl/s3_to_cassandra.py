"""
Module to deal with data transfer from S3 to Cassandra
"""
import logging as log

from attr import dataclass
from cassandra.query import dict_factory

from hip_data_tools.apache.cassandra import CassandraUtil, CassandraConnectionManager, \
    CassandraConnectionSettings
from hip_data_tools.aws.common import AwsConnectionSettings, AwsConnectionManager
from hip_data_tools.aws.s3 import S3Util


@dataclass
class S3ToCassandraSettings:
    source_bucket: str
    source_key_prefix: str
    source_connection_settings: AwsConnectionSettings
    destination_keyspace: str
    destination_table: str
    destination_table_primary_keys: list
    destination_table_options_statement: str
    destination_connection_settings: CassandraConnectionSettings


class S3ToCassandra:
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

        df = self._s3.download_df_parquet(s3_key=self.list_source_files()[0])
        self._cassandra.create_table_from_dataframe(
            data_frame=df,
            table_name=self.settings.destination_table,
            primary_key_column_list=self.settings.destination_table_primary_keys,
            table_options_statement=self.settings.destination_table_options_statement,
        )

    def create_and_upsert_all(self):
        self.create_table()
        for key in self.list_source_files():
            self.upsert_object(key)
            result = self._cassandra.read_dict(f"""
            SELECT COUNT(1) 
            FROM {self.settings.destination_keyspace}.{self.settings.destination_table}""")
            log.info("The target table now contains %s rows", result)

    def upsert_object(self, key):
        # TODO: Verify if file is parquet and throw error if it is not
        df = self._s3.download_df_parquet(s3_key=key)
        self._cassandra.upsert_dataframe(dataframe=df, table=self.settings.destination_table)

    def list_source_files(self):
        if self.keys_to_transfer is None:
            self.keys_to_transfer = self._s3.list_objects(self.settings.source_key_prefix)
            log.info("Listed and cached %s source files", len(self.keys_to_transfer))
        return self.keys_to_transfer
