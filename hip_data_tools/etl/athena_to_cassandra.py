"""
handle ETL of data from Athena to Cassandra
"""

from hip_data_tools.aws.athena import AthenaUtil
from hip_data_tools.aws.common import AwsConnectionManager
from hip_data_tools.etl.common import AthenaTableSource, CassandraTableSink, S3DirectorySource
from hip_data_tools.etl.s3_to_cassandra import S3ToCassandra


class AthenaToCassandra(S3ToCassandra):
    """
    Class to transfer parquet data from s3 to Cassandra
    Example -

    Args:
        settings (AthenaToCassandraSettings): the settings around the etl to be executed
    """

    def __init__(self, source: AthenaTableSource, sink: CassandraTableSink):
        self.__sink = sink
        self.__source = source
        self._athena = self._get_athena_util()
        (bucket, key) = self._athena.get_table_data_location(self.__source.table)
        super().__init__(
            source=S3DirectorySource(
                connection_settings=source.connection_settings,
                bucket=bucket,
                directory_key=key,
            ),
            sink=sink)

    def _get_athena_util(self):
        return AthenaUtil(
            settings=self.__source,
            conn=AwsConnectionManager(settings=self.__source.connection_settings),
        )
