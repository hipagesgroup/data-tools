"""
Module to deal with data transfer from Google sheets to Athena
"""

from hip_data_tools.aws.athena import AthenaUtil, get_athena_columns_from_dataframe
from hip_data_tools.aws.common import AwsConnectionManager
from hip_data_tools.etl.common import GoogleSheetsTableSource, \
    AthenaTableDirectorySink, S3DirectorySink
from hip_data_tools.etl.google_sheet_to_s3 import GoogleSheetToS3


class GoogleSheetToAthena(GoogleSheetToS3):
    """
    Class to transfer data from google sheet to athena
    """

    def __init__(self, source: GoogleSheetsTableSource, sink: AthenaTableDirectorySink):
        self.__source = source
        self.__sink = sink
        target_dir = self._calculate_s3_key()
        super().__init__(
            source=source,
            sink=S3DirectorySink(
                bucket=self.__sink.s3_data_location_bucket,
                connection_settings=self.__sink.connection_settings,
                directory_key=target_dir,
                file_prefix=self.__sink.file_prefix,
            )
        )
        self.keys_to_transfer = None

    def _get_athena_util(self):
        return AthenaUtil(settings=self.__sink,
                          conn=AwsConnectionManager(settings=self.__sink.connection_settings))

    def load_sheet_to_athena(self):
        """
        Load google sheet into Athena
        :return: None
        """
        self.write_sheet_data_to_s3()

        athena_util = self._get_athena_util()
        if self.__sink.table_ddl_progress:
            athena_util.drop_table(self.__sink.table)

        athena_util.create_table(table_settings={
            "exists": True,
            "partitions": [{"column": k, "type": "STRING"} for k, v in self.__sink.partition_value],
            "storage_format_selector": "parquet",
            "encryption": False,
            "table": self.__sink.table,
            "columns": get_athena_columns_from_dataframe(self.get_sheet_as_dataframe()),
            "s3_bucket": self.__sink.s3_data_location_bucket,
            "s3_dir": self.__sink.s3_data_location_directory_key,
        })

        if self.__sink.table_ddl_progress:
            athena_util.repair_table_partitions(table=self.__sink.table)
        else:
            athena_util.add_partitions(
                table=self.__sink.table,
                partition_keys=[k for k, v in self.__sink.partition_value],
                partition_values=[v for k, v in self.__sink.partition_value]
            )

    def _calculate_s3_key(self):
        s3_key_with_partition = self.__sink.s3_data_location_directory_key
        if self.__sink.partition_value is not None:
            partition_path = "/".join([f"{k}={v}" for (k, v) in self.__sink.partition_value])
            s3_key_with_partition += partition_path
        return s3_key_with_partition
