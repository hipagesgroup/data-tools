from pandas import DataFrame

from hip_data_tools.connect.cassandra import CassandraConnectionManager


class CassandraUtil:
    """
    Class to connect to and then retrieve, transform and upload data from and to cassandra
    """

    def __init__(self, keyspace: str, conn: CassandraConnectionManager):
        self._conn = conn
        self.keyspace = keyspace

    def upsert_dataframe(self, df: DataFrame, table: str, column_mapping=None, ) -> None:
        """
        upload all data from a dataframe onto a cassandra table
        Args:
            df (DataFrame): a dataframe to upsert
            table (str): the table to upsert data into
            column_mapping (dict): a mapping between column names of the dataframe to cassandra
            table. If None then the dataframe column names that match cassandra table anme will be
            upserted else ignored
        Returns: None
        """
        pass

    def create_table_from_dataframe(self, df: DataFrame, table: str) -> None:
        """
        create a table based on a given pandas DataFrame 's schema
        Args:
            df (DataFrame):
            table (str):
        Returns: None
        """
        pass

    def upsert_dict(self, row: dict, table: str) -> None:
        """
        Upsert a row into a given cassandra table based on the dictionary key values
        Args:
            row (dict):
            table (str):
        Returns: None
        """
