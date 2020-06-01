"""
handle ETL of data from Athena to S3
"""
from typing import Optional, List

from attr import dataclass

from hip_data_tools.aws.athena import AthenaUtil
from hip_data_tools.aws.common import AwsConnectionManager
from hip_data_tools.etl.athena_to_athena import AthenaToAthena, AthenaCTASSink
from hip_data_tools.etl.common import AthenaQuerySource


@dataclass
class S3CTASSink:
    temporary_database: str
    temporary_table: Optional[str]
    data_format: str
    s3_data_location_bucket: str
    s3_data_location_directory_key: str
    partition_columns: Optional[List[str]]


class AthenaToS3:
    """
    ETL To transfer data from an Athena sql to an s3 location
    Args:
        source (AthenaToS3Settings): Settings for the etl
    """

    def __init__(self, source: AthenaQuerySource, sink: S3CTASSink):
        self.__source = source
        self.__sink = sink

    def _drop_temporary_table(self) -> None:
        au = AthenaUtil(
            settings=self.__source,
            conn=AwsConnectionManager(self.__source.connection_settings)
        )
        au.drop_table(self.__sink.temporary_table)

    def execute(self) -> None:
        """
        Execute the ETL to transfer data from Athena to S3
        Returns: None
        """
        etl = AthenaToAthena(
            source=self.__source,
            sink=self._s3_to_athena_sink()
        )
        etl.execute()
        # Delete temporary table
        self._drop_temporary_table()

    def _s3_to_athena_sink(self):
        return AthenaCTASSink(
            database=self.__sink.temporary_database,
            table=self.__sink.temporary_table,
            data_format=self.__sink.data_format,
            s3_data_location_bucket=self.__sink.s3_data_location_bucket,
            s3_data_location_directory_key=self.__sink.s3_data_location_directory_key,
            partition_columns=self.__sink.partition_columns,
        )
