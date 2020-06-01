"""
handle ETL of data from Athena to Athena
"""
from typing import Optional, List

from attr import dataclass

from hip_data_tools.aws.athena import AthenaUtil
from hip_data_tools.aws.common import AwsConnectionManager
from hip_data_tools.etl.common import AthenaQuerySource


@dataclass
class AthenaCTASSink:
    database: str
    table: str
    data_format: str
    s3_data_location_bucket: str
    s3_data_location_directory_key: str
    partition_columns: Optional[List[str]]


class AthenaToAthena:
    """
    ETL To transfer data from an Athena SQL into an Athena Table

    Args:
        source:
        sink:
    """

    def __init__(self, source: AthenaQuerySource, sink: AthenaCTASSink):
        self.__source = source
        self.__sink = sink
        self._athena = None

    def generate_create_table_statement(self) -> str:
        """
        Generates an Athena compliant ctas sql statement for the ETL
        Returns: str
        """
        partition_statement = ""
        if self.__sink.partition_columns:
            col_list = ','.join([f"'{c}'" for c in self.__sink.partition_columns])
            partition_statement = f", partitioned_by = ARRAY[{col_list}] "
        external_location = f"'s3://{self.__sink.s3_data_location_bucket}/" \
                            f"{self.__sink.s3_data_location_bucket}/'"
        return f"""
            CREATE TABLE {self.__sink.database}.{self.__sink.table}
            WITH (
                format = '{self.__sink.data_format}'
                , external_location = {external_location}
                {partition_statement}
            ) AS 
            {self.__source.sql}
            """

    def _get_athena_util(self) -> AthenaUtil:
        if self._athena is None:
            self._athena = AthenaUtil(
                settings=self.__source,
                conn=AwsConnectionManager(self.__source.connection_settings),
            )
        return self._athena

    def execute(self) -> None:
        """
        Execute the ETL by running an Athena CTAS statement
        Returns: None
        """
        self._get_athena_util().run_query(self.generate_create_table_statement())
