"""
handle ETL of data from Athena to Cassandra
"""
from attr import dataclass

from hip_data_tools.aws.athena import AthenaUtil
from hip_data_tools.aws.common import AwsConnectionSettings, AwsConnectionManager
from hip_data_tools.etl.s3_to_dataframe import S3ToDataFrameSettings, S3ToDataFrame


@dataclass
class AthenaToDataFrameSettings:
    """S3 to Cassandra ETL settings"""
    source_database: str
    source_table: str
    source_connection_settings: AwsConnectionSettings


class AthenaToDataFrame(S3ToDataFrame):
    """
    Class to transfer parquet data from s3 to Cassandra
    Args:
        settings (AthenaToCassandraSettings): the settings around the etl to be executed
    """

    def __init__(self, settings: AthenaToDataFrameSettings):
        self._athena = None
        self.__settings = settings
        (bucket, key) = self._get_athena_util().get_table_data_location(settings.source_table)
        self.s3_settings = S3ToDataFrameSettings(
            source_bucket=bucket,
            source_key_prefix=key,
            source_connection_settings=settings.source_connection_settings,
        )
        super().__init__(self.s3_settings)

    def _get_athena_util(self):
        if self._athena is None:
            self._athena = AthenaUtil(
                database=self.__settings.source_database,
                conn=AwsConnectionManager(self.__settings.source_connection_settings))
        return self._athena
