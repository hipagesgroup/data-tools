"""
handle ETL of data from Athena to Cassandra
"""
from typing import Optional, List

from attr import dataclass

from hip_data_tools.aws.athena import AthenaUtil
from hip_data_tools.aws.common import AwsConnectionSettings, AwsConnectionManager


@dataclass
class AthenaToAthenaSettings:
    """Athena To S3 ETL settings"""
    source_sql: str
    source_database: str
    target_database: str
    target_table: str
    target_data_format: str
    target_s3_bucket: str
    target_s3_dir: str
    target_partition_columns: Optional[List[str]]
    connection_settings: AwsConnectionSettings


class AthenaToAthena:

    def __init__(self, settings: AthenaToAthenaSettings):
        self.__settings = settings

    def generate_create_table_statement(self) -> str:
        partition_statement = ""
        if self.__settings.target_partition_columns:
            col_list = ','.join([f"'{c}'" for c in self.__settings.target_partition_columns])
            partition_statement = f", partitioned_by = ARRAY[{col_list}] "
        external_location = f"'s3://{self.__settings.target_s3_bucket}/" \
                            f"{self.__settings.target_s3_dir}/'"
        return f"""
            CREATE TABLE {self.__settings.target_database}.{self.__settings.target_table}
            WITH (
                format = '{self.__settings.target_data_format}'
                , external_location = {external_location}
                {partition_statement}
            ) AS 
            {self.__settings.source_sql}
            """

    def _get_athena_util(self)->AthenaUtil:
        if self._athena is None:
            self._athena = AthenaUtil(
                database=self.__settings.source_database,
                conn=AwsConnectionManager(self.__settings.connection_settings))
        return self._athena

    def execute(self):
        self._get_athena_util().run_query(self.generate_create_table_statement())
