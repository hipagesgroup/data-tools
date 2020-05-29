"""
handle ETL of data from Athena to S3
"""
from typing import Optional, List

from attr import dataclass

from hip_data_tools.aws.athena import AthenaUtil
from hip_data_tools.aws.common import AwsConnectionSettings, AwsConnectionManager
from hip_data_tools.etl.athena_to_athena import AthenaToAthenaSettings, AthenaToAthena
from hip_data_tools.etl.common import current_epoch, get_random_string


@dataclass
class AthenaToS3Settings:
    """Athena To S3 ETL settings"""
    source_sql: str
    source_database: str
    temporary_database: Optional[str]
    temporary_table: Optional[str]
    target_data_format: str
    target_s3_bucket: str
    target_s3_dir: str
    target_partition_columns: Optional[List[str]]
    connection_settings: AwsConnectionSettings


class AthenaToS3:
    """
    ETL To transfer data from an Athena sql to an s3 location
    Args:
        settings (AthenaToS3Settings): Settings for the etl
    """

    def __init__(self, settings: AthenaToS3Settings):
        self.__settings = settings
        if self.__settings.temporary_database:
            self.__settings.temporary_database = self.__settings.source_database
        if self.__settings.temporary_table:
            self.__settings.temporary_table = f"temp__{current_epoch()}__{get_random_string(5)}"

    def _drop_temporary_table(self) -> None:
        au = AthenaUtil(
            database=self.__settings.temporary_database,
            conn=AwsConnectionManager(self.__settings.connection_settings)
        )
        au.drop_table(self.__settings.temporary_table)

    def execute(self) -> None:
        """
        Execute the ETL to transfer data from Athena to S3
        Returns: None
        """
        etl = AthenaToAthena(
            AthenaToAthenaSettings(
                source_sql=self.__settings.source_sql,
                source_database=self.__settings.source_database,
                target_database=self.__settings.temporary_database,
                target_table=self.__settings.temporary_table,
                target_data_format=self.__settings.target_data_format,
                target_s3_bucket=self.__settings.target_s3_bucket,
                target_s3_dir=self.__settings.target_s3_dir,
                target_partition_columns=self.__settings.target_partition_columns,
                connection_settings=self.__settings.connection_settings
            )
        )
        etl.execute()
        # Delete temporary table
        self._drop_temporary_table()
