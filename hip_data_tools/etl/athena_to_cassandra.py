"""
handle ETL of data from Athena to Cassandra
"""
from attr import dataclass

from hip_data_tools.apache.cassandra import CassandraConnectionSettings
from hip_data_tools.aws.athena import AthenaUtil
from hip_data_tools.aws.common import AwsConnectionSettings, AwsConnectionManager
from hip_data_tools.etl.s3_to_cassandra import S3ToCassandraSettings, S3ToCassandra


@dataclass
class AthenaToCassandraSettings:
    """S3 to Cassandra ETL settings"""
    source_database: str
    source_table: str
    source_connection_settings: AwsConnectionSettings
    destination_keyspace: str
    destination_table: str
    destination_table_primary_keys: list
    destination_table_options_statement: str
    destination_batch_size: int
    destination_connection_settings: CassandraConnectionSettings


class AthenaToCassandra(S3ToCassandra):
    """
    Class to transfer parquet data from s3 to Cassandra
    Args:
        settings (AthenaToCassandraSettings): the settings around the etl to be executed
    """

    def __init__(self, settings: AthenaToCassandraSettings):
        self.settings = settings
        self._athena = AthenaUtil(
            database=self.settings.source_database,
            conn=AwsConnectionManager(self.settings.source_connection_settings))
        (bucket, key) = self._athena.get_table_data_location(self.settings.source_table)
        self.s3_settings = S3ToCassandraSettings(
            source_bucket=bucket,
            source_key_prefix=key,
            source_connection_settings=self.settings.source_connection_settings,
            destination_keyspace=self.settings.destination_keyspace,
            destination_table=self.settings.destination_table,
            destination_table_primary_keys=self.settings.destination_table_primary_keys,
            destination_table_options_statement=self.settings.destination_table_options_statement,
            destination_batch_size=self.settings.destination_batch_size,
            destination_connection_settings=self.settings.destination_connection_settings,
        )
        super().__init__(self.s3_settings)
