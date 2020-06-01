"""
Module to deal with data transfer from Adwords to Athena
"""

from pandas import DataFrame

from hip_data_tools.aws.athena import AthenaUtil, get_athena_columns_from_dataframe, \
    extract_athena_type_from_value
from hip_data_tools.aws.common import AwsConnectionManager
from hip_data_tools.common import LOG
from hip_data_tools.etl.adwords_to_s3 import AdWordsToS3, AdWordsReportsToS3
from hip_data_tools.etl.common import AthenaTableDirectorySink, AdWordsServiceSource, \
    S3DirectorySink, AdWordsReportSource


class AdWordsToAthena(AdWordsToS3):
    """
    ETL Class to handle the transfer of data from adwords based on AWQL to S3 as parquet files
    Args:
        settings (AdWordsToS3Settings): the etl settings to be used
    """

    def __init__(self, source: AdWordsServiceSource, sink: AthenaTableDirectorySink):
        self.__source = source
        self.__sink = sink

        directory_key = sink.s3_data_location_directory_key
        if self.__sink.is_partitioned_table:
            partition_dirs = "/".join([f"{k}={v}" for k, v in sink.partition_value])
            directory_key = f"{sink.s3_data_location_directory_key}/{partition_dirs}"
        super().__init__(source=source, sink=S3DirectorySink(
            bucket=self.__sink.s3_data_location_bucket,
            connection_settings=self.__sink.connection_settings,
            directory_key=directory_key,
            file_prefix=self.__sink.file_prefix))

    def _get_athena_util(self):
        return AthenaUtil(
            settings=self.__sink,
            conn=AwsConnectionManager(settings=self.__sink.connection_settings),
        )

    def create_athena_table(self) -> None:
        """
        Creates an athena table on top of the transferred data
        Returns: None
        """
        self.build_query(start_index=0, page_size=1, num_iterations=1)
        data = self._get_next_page()
        athena_util = self._get_athena_util()
        if self.__sink.table_ddl_progress:
            athena_util.drop_table(self.__sink.table)
        athena_table_settings = self._construct_athena_table_settings(data)
        athena_util.create_table(table_settings=athena_table_settings)
        # Reset query state
        self.query = None

    def _construct_athena_table_settings(self, data: DataFrame) -> dict:
        partition_settings = []
        if self.__sink.is_partitioned_table:
            partition_settings = [{"column": k, "type": extract_athena_type_from_value(v)}
                                  for k, v in self.__sink.partition_value]
        athena_table_settings = {
            "exists": True,
            "partitions": partition_settings,
            "storage_format_selector": "parquet",
            "encryption": False,
            "table": self.__sink.table,
            "columns": get_athena_columns_from_dataframe(data),
            "s3_bucket": self.__sink.bucket,
            "s3_dir": self.__sink.s3_data_location_directory_key,
        }
        return athena_table_settings


class AdWordsReportsToAthena(AdWordsReportsToS3):
    """
    ETL Class to handle the transfer of data from adwords based on AWQL to S3 as parquet files
    Args:
        settings (AdWordsToS3Settings): the etl settings to be used
    """

    def __init__(self, source: AdWordsReportSource, sink: AthenaTableDirectorySink):
        self.__source = source
        self.__sink = sink

        directory_key = sink.s3_data_location_directory_key
        if self.__sink.is_partitioned_table:
            partition_dirs = "/".join([f"{k}={v}" for k, v in sink.partition_value])
            directory_key = f"{sink.s3_data_location_directory_key}/{partition_dirs}"
        self._final_target_prefix = directory_key
        super().__init__(source=source, sink=S3DirectorySink(
            bucket=self.__sink.s3_data_location_bucket,
            connection_settings=self.__sink.connection_settings,
            directory_key=directory_key,
            file_prefix=self.__sink.file_prefix))

    def _get_athena_util(self):
        return AthenaUtil(
            settings=self.__sink,
            conn=AwsConnectionManager(settings=self.__sink.connection_settings),
        )

    def add_partitions(self):
        """
        Add the current Data Transfer's partition to Athena's Metadata
        Returns: None
        """
        if self.__sink.partition_value:
            athena_util = self._get_athena_util()
            athena_util.add_partitions(
                table=self.__sink.table,
                partition_keys=[key for (key, value) in self.__sink.partition_value],
                partition_values=[value for (key, value) in self.__sink.partition_value]
            )
        else:
            LOG.warning("The table is not partitioned, this is a NOOP")

    def create_athena_table(self) -> None:
        """
        Creates an athena table on top of the transferred data
        Returns: None
        """
        athena_util = self._get_athena_util()
        s3_util = self._get_s3_util()
        keys = s3_util.get_keys(key_prefix=self._final_target_prefix)
        LOG.debug("gathered files transferred under this ETL %s", keys)
        if keys:
            data = s3_util.download_parquet_as_dataframe(keys[0])
            LOG.info(
                "Downloaded parquet file from s3 to construct Athena create table statement: %s "
                "\n made dataframe of shape %s", keys[0], data.shape)
            if self.__sink.table_ddl_progress:
                athena_util.drop_table(self.__sink.table)
            athena_table_settings = self._construct_athena_table_settings(data)
            athena_util.create_table(table_settings=athena_table_settings)
        else:
            raise ValueError(
                "No Data has been uploaded to target directory, please load data first, "
                "before creating Athena table")

    def _construct_athena_table_settings(self, data: DataFrame) -> dict:
        partition_settings = []
        if self.__sink.partition_value:
            partition_settings = [{"column": k, "type": extract_athena_type_from_value(v)}
                                  for k, v in self.__sink.partition_value]
        athena_table_settings = {
            "exists": True,
            "partitions": partition_settings,
            "storage_format_selector": "parquet",
            "encryption": False,
            "table": self.__sink.table,
            "columns": get_athena_columns_from_dataframe(data),
            "s3_bucket": self.__sink.s3_data_location_bucket,
            "s3_dir": self.__sink.s3_data_location_directory_key,
        }
        return athena_table_settings
