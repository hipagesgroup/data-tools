"""

"""
from typing import Optional, List, Tuple

from attr import dataclass
from pandas import DataFrame

from hip_data_tools.aws.athena import AthenaUtil, get_athena_columns_from_dataframe
from hip_data_tools.aws.common import AwsConnectionSettings, AwsConnectionManager
from hip_data_tools.aws.s3 import S3Util
from hip_data_tools.common import LOG


@dataclass
class AthenaOnS3Settings:
    bucket: str
    base_key_prefix: str
    target_database: str
    target_table: str
    target_is_partitioned_table: bool
    partition_columns: Optional[List[Tuple[str, str]]]
    target_connection_settings: AwsConnectionSettings


class AthenaOnS3:

    def __init__(self, settings: AthenaOnS3Settings):
        self.__settings = settings
        self._athena = None
        self._s3_util = None

    def _get_s3_util(self) -> S3Util:
        if self._s3_util is None:
            self._s3_util = S3Util(
                bucket=self.__settings.bucket,
                conn=AwsConnectionManager(self.__settings.target_connection_settings),
            )
        return self._s3_util

    def _get_athena_util(self):
        if self._athena is None:
            self._athena = AthenaUtil(
                database=self.__settings.target_database,
                conn=AwsConnectionManager(self.__settings.target_connection_settings),
                output_bucket=self.__settings.bucket)
        return self._athena

    def _get_sample_file_from_s3(self):
        s3_util = self._get_s3_util()
        keys = s3_util.get_keys(key_prefix=self.__settings.base_key_prefix)
        LOG.debug("gathered files present on S3 %s", keys)
        if keys:
            # TODO: Make it more intelligent by sampling multiple files
            data = s3_util.download_parquet_as_dataframe(keys[0])
            LOG.info("Downloaded parquet file as dataframe %s", data.info(verbose=True))
            return data
        else:
            raise ValueError(
                "No Data has been uploaded to target directory, please load data , "
                "before creating Athena table")

    def create_athena_table(self, drop_before_create=True) -> None:
        """
        Creates an athena table on top of the transferred data
        Returns: None
        """
        athena_util = self._get_athena_util()
        data = self._get_sample_file_from_s3()
        if drop_before_create:
            athena_util.drop_table(self.__settings.target_table)
        athena_table_settings = self._construct_athena_table_settings(data)
        athena_util.create_table(table_settings=athena_table_settings)

    def _construct_athena_table_settings(self, data: DataFrame) -> dict:
        partition_settings = []
        if self.__settings.target_is_partitioned_table:
            partition_settings = [{"column": k, "type": v} for k, v in
                                  self.__settings.partition_columns]
        athena_table_settings = {
            "exists": True,
            "partitions": partition_settings,
            "storage_format_selector": "parquet",
            "encryption": False,
            "table": self.__settings.target_table,
            "columns": get_athena_columns_from_dataframe(data),
            "s3_bucket": self.__settings.bucket,
            "s3_dir": self.__settings.base_key_prefix,
        }
        LOG.debug("Prepared Athena Table settings as %s", athena_table_settings)
        return athena_table_settings
