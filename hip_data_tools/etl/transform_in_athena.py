"""
handle ETL of data from Athena to Cassandra
"""
from typing import List

import dataclasses as dataclasses
from attr import dataclass

from hip_data_tools.aws.athena import AthenaUtil
from hip_data_tools.aws.common import AwsConnectionSettings, AwsConnectionManager
from hip_data_tools.aws.s3 import S3Util


@dataclass
class TransformInAthenaSettings:
    """ETL Settings"""
    connection_settings: AwsConnectionSettings
    source_query: str
    source_database: str
    source_athena_result_bucket: str
    source_athena_result_key_prefix: str
    destination_database: str
    destination_table: str
    destination_s3_bucket: str
    destination_s3_key_prefix: str
    destination_partition_columns: List[str] = dataclasses.field(default_factory=list)
    destination_file_format: str = dataclasses.field(default="PARQUET")
    destination_file_delimiter: str = dataclasses.field(default=None)


class TransformInAthena:
    """
    Use a valid athena SQL to create a new table
    Args:
        settings (AthenaToCassandraSettings): the settings around the etl to be executed
    """

    def __init__(self, settings: TransformInAthenaSettings):
        self.settings = settings
        self._athena = AthenaUtil(
            database=self.settings.source_database,
            conn=AwsConnectionManager(self.settings.connection_settings),
            output_key=self.settings.source_athena_result_key_prefix,
            output_bucket=self.settings.source_athena_result_bucket,
        )
        self._s3 = S3Util(
            conn=AwsConnectionManager(self.settings.connection_settings),
            bucket=self.settings.destination_s3_bucket)

    def clean_destination(self):
        if self.settings.destination_s3_key_prefix.endswith("/"):
            key = self.settings.destination_s3_key_prefix
        else:
            key = f"{self.settings.destination_s3_key_prefix}/"
        self._s3.delete_recursive(key_prefix=key)

    def create_table_from_sql(self):
        self.clean_destination()

    def generate_ctas(self):
        partitioned_by_statement = ""
        if self.settings.destination_partition_columns:
            partitioned_by_statement = f"""
            , partitioned_by = ARRAY[{','.join(self.settings.destination_partition_columns)}] """

        field_delimiter_statement = ""
        if self.settings.destination_file_delimiter:
            field_delimiter_statement = f"""
            , field_delimiter='{self.settings.destination_file_delimiter}'"""
        return f"""
        CREATE TABLE {self.settings.destination_database}.{self.settings.destination_table}
        WITH (
            format='{self.settings.destination_file_format}'
            {field_delimiter_statement}
            , external_location='s3://{self.settings.destination_s3_bucket}/
{self.settings.destination_s3_key_prefix}'
            {partitioned_by_statement}
        ) AS
        {self.settings.source_query}
        """
