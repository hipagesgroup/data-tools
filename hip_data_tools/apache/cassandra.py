"""
Utility for connecting to and transforming data in Cassandra clusters
"""
import pandas as pd
from cassandra import ConsistencyLevel
from cassandra.cluster import ResultSet
from cassandra.cqlengine.management import sync_table
from cassandra.query import dict_factory, BatchStatement
from pandas import DataFrame
from pandas._libs.tslibs.nattype import NaT
from pandas._libs.tslibs.timestamps import Timestamp

from hip_data_tools.connect.cassandra import CassandraConnectionManager


def _pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)


def _prepare_batch(prepared_statement, rows) -> BatchStatement:
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    for row in rows:
        batch.add(prepared_statement, row)
    return batch


def _extract_rows_from_dataframe(dataframe):
    return [tuple([_clean_outgoing_values(val) for val in row]) for index, row in
            dataframe.iterrows()]


def _extract_rows_from_list_of_dict(data):
    return [tuple(dct.values()) for dct in data]


def _clean_outgoing_values(val):
    if isinstance(val, Timestamp):
        return val.to_pydatetime()
    if val is NaT:
        return None
    return val


class CassandraUtil:
    """
    Class to connect to and then retrieve, transform and upload data from and to cassandra
    """

    def __init__(self, keyspace: str, conn: CassandraConnectionManager):
        self.keyspace = keyspace
        self._conn = conn
        self._session = self._conn.get_session(self.keyspace)

    def _cql_upsert_from_dict(self, data, table):
        upsert_sql = f"""
        INSERT INTO {self.keyspace}.{table} 
        ({", ".join(data[0])}) 
        VALUES ({", ".join(['?' for key in data[0]])});
            """
        print(upsert_sql)
        return upsert_sql

    def _cql_upsert_from_dataframe(self, dataframe, table):
        upsert_sql = f"""
        INSERT INTO {self.keyspace}.{table} 
        ({", ".join(list(dataframe.columns.values))}) 
        VALUES ({", ".join(['?' for key in dataframe.columns.values])});
            """
        print(upsert_sql)
        return upsert_sql

    def upsert_dataframe(self, dataframe: DataFrame, table: str) -> None:
        """
        upload all data from a DataFrame onto a cassandra table
        Args:
            dataframe (DataFrame): a DataFrame to upsert
            table (str): the table to upsert data into
            column_mapping (dict): a mapping between column names of the dataframe to cassandra
            table. If None then the DataFrame column names that match cassandra table anme will be
            upserted else ignored
        Returns: None
        """
        prepared_statement = self._session.prepare(
            self._cql_upsert_from_dataframe(dataframe, table))
        batch = _prepare_batch(prepared_statement,
                               _extract_rows_from_dataframe(dataframe))
        return self._session.execute(batch)

    def create_table_from_dataframe(self, dataframe: DataFrame, table: str) -> None:
        """
        create a table based on a given pandas DataFrame 's schema
        Args:
            dataframe (DataFrame):
            table (str):
        Returns: None
        """
        pass

    def upsert_dict(self, data: list, table: str) -> ResultSet:
        """
        Upsert a row into a given cassandra table based on the dictionary key values
        Args:
            data (list[dict]):
            table (str):
        Returns: None
        """
        prepared_statement = self._session.prepare(self._cql_upsert_from_dict(data, table))
        batch = _prepare_batch(prepared_statement, _extract_rows_from_list_of_dict(data))
        return self._session.execute(batch)

    def create_table_from_model(self, model_class):
        """
        create a table if not exists from the given Model Class
        Args:
            model_class (class): class for the Model
        Returns: None
        """
        self._conn.setup_connection(default_keyspace=self.keyspace)
        sync_table(model_class)

    def create_table(self, columns: dict, primary_keys: list, table: str) -> ResultSet:
        """
        Create a table if not already exists based on data from a dictionary
        Args:
            columns (dict):
            primary_keys (list[str]):
            table (str):
        Returns: ResultSet
        """
        pass

    def read_dict(self, query, **kwargs) -> list:
        """
        Read the results of a query in form of a list of dict
        Args:
            query (str):
        Returns: list[dict]
        """
        return self.execute(query, dict_factory, **kwargs).current_rows

    def read_dataframe(self, query, **kwargs) -> DataFrame:
        """
        read the result of a query in form of a pandas DataFrame
        Args:
            query (str):
        Returns: DataFrame
        """
        # Since Pandas DataFrame does not have a boolean truth value will need to access
        # protected _current_rows
        return self.execute(query, _pandas_factory, **kwargs)._current_rows

    def execute(self, query, row_factory, **kwargs):
        """
        Execute a cql command and retrieve data with a given row factory
        Args:
            query (str):
            row_factory (callable):
            **kwargs: Kwargs to match the session.execute command in cassandra
        Returns: ResultSet
        """
        if row_factory is not None:
            self._session.row_factory = row_factory
        return self._session.execute(query, **kwargs)
