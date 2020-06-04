"""
handle ETL of data from Athena to Cassandra
"""
from attr import dataclass

from hip_data_tools.aws.athena import AthenaUtil
from hip_data_tools.aws.common import AwsConnectionSettings, AwsConnectionManager
from hip_data_tools.etl.common import AthenaTableSource, S3DirectorySource
from hip_data_tools.etl.s3_to_dataframe import S3ToDataFrame


@dataclass
class AthenaToDataFrameSettings:
    """S3 to Cassandra ETL settings"""
    source_database: str
    source_table: str
    source_connection_settings: AwsConnectionSettings


class AthenaToDataFrame(S3ToDataFrame):
    """
    Class to transfer parquet data from s3 to Cassandra
    """

    def __init__(self, source: AthenaTableSource):
        self.__source = source
        self._athena = None
        (bucket, key) = self._get_athena_util().get_table_data_location(source.table)
        super().__init__(source=S3DirectorySource(
            connection_settings=self.__source.connection_settings,
            bucket=bucket,
            directory_key=key,
        ))

    def _get_athena_util(self):
        if self._athena is None:
            self._athena = AthenaUtil(
                settings=self.__source,
                conn=AwsConnectionManager(settings=self.__source.connection_settings),
            )
        return self._athena
