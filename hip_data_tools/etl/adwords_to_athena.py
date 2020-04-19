"""
Module to deal with data transfer from Adwords to Athena
"""
from typing import List, Any, Optional, Tuple

from attr import dataclass

from hip_data_tools.aws.athena import AthenaUtil, get_athena_columns_from_dataframe, \
    extract_athena_type_from_value
from hip_data_tools.aws.common import AwsConnectionManager
from hip_data_tools.etl.adwords_to_s3 import AdWordsToS3Settings, AdWordsToS3


@dataclass
class AdWordsToAthenaSettings(AdWordsToS3Settings):
    """Settings container for Adwords to Athena ETL"""
    target_database: str
    target_table: str
    target_table_ddl_progress: bool
    is_partitioned_table: bool
    partition_values: Optional[List[Tuple[str, Any]]]


class AdWordsToAthena(AdWordsToS3):
    """
    ETL Class to handle the transfer of data from adwords based on AWQL to S3 as parquet files
    Args:
        settings (AdWordsToS3Settings): the etl settings to be used
    """

    def __init__(self, settings: AdWordsToAthenaSettings):
        self.__settings = settings
        self.base_dir = settings.target_key_prefix
        if self.__settings.is_partitioned_table:
            partition_dirs = "/".join([f"{k}={v}" for k, v in settings.partition_values])
            settings.target_key_prefix = f"{settings.target_key_prefix}/{partition_dirs}"
        super().__init__(settings)

    def _get_athena_util(self):
        return AthenaUtil(
            database=self.__settings.target_database,
            conn=AwsConnectionManager(
                settings=self.__settings.target_connection_settings),
            output_bucket=self.__settings.target_bucket)

    def create_athena_table(self) -> None:
        """
        Creates an athena table on top of the transferred data
        Returns: None
        """
        self.build_query(start_index=0, page_size=1, num_iterations=1)
        data = self._get_next_page()
        athena_util = self._get_athena_util()
        if self.__settings.target_table_ddl_progress:
            athena_util.drop_table(self.__settings.target_table)
        athena_table_settings = self._construct_athena_table_settings(data)
        athena_util.create_table(table_settings=athena_table_settings)
        # Reset query state
        self.query = None

    def _construct_athena_table_settings(self, data: DataFrame) -> dict:
        partition_settings = []
        if self.__settings.is_partitioned_table:
            partition_settings = [{"column": k, "type": extract_athena_type_from_value(v)}
                                  for k, v in self.__settings.partition_values]
        athena_table_settings = {
            "exists": True,
            "partitions": partition_settings,
            "storage_format_selector": "parquet",
            "encryption": False,
            "table": self.__settings.target_table,
            "columns": get_athena_columns_from_dataframe(data),
            "s3_bucket": self.__settings.target_bucket,
            "s3_dir": self.base_dir,
        }
        return athena_table_settings
