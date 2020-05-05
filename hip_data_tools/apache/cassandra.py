"""
Utility for connecting to and transforming data in Cassandra clusters
"""
import os
from typing import List

import pandas as pd
from attr import dataclass
from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, Session
from cassandra.cqlengine import connection
from cassandra.cqlengine.management import sync_table
from cassandra.datastax.graph import Result
from cassandra.policies import LoadBalancingPolicy
from cassandra.query import dict_factory, BatchStatement, PreparedStatement
from pandas import DataFrame
from pandas._libs.tslibs.nattype import NaT
from pandas._libs.tslibs.timestamps import Timestamp
from retrying import retry

from hip_data_tools.common import KeyValueSource, ENVIRONMENT, SecretsManager, LOG

_RETRY_WAIT_MULTIPLIER_MS: int = int(os.getenv("CASSANDRA_RETRY_WAIT_MULTIPLIER_MS", "1000"))
"""Exponential backoff settings for connections to cassandra"""

_RETRY_WAIT_MAX_MS: int = int(os.getenv("CASSANDRA_RETRY_WAIT_MAX_MS", "100000"))
"""Exponential backoff settings for connections to cassandra"""

_PYTHON_TO_CASSANDRA_DATA_TYPE_MAP = {
    "Timestamp": "timestamp",
    "str": "varchar",
    "int64": "bigint",
    "int32": "int",
    "dict": "map",
    "float64": "double",
    "UUID": "UUID",
}
"""Dictionary mapping of python and pandas data types to Cassandra data types"""


def _get_data_frame_column_types(data_frame):
    data_frame_col_dict = {}
    for col in data_frame:
        data_frame_col_dict[col] = type(data_frame[col][0]).__name__
    return data_frame_col_dict


def get_cql_columns_from_dataframe(data_frame):
    """
    Extracts a dictionary of column names and their cassandra data types from the dataframe
    Args:
        data_frame (DataFrame): the dataframe whose columns need to be extracted
    Returns: dict
    """
    column_dtype = _get_data_frame_column_types(data_frame)
    return {key: _PYTHON_TO_CASSANDRA_DATA_TYPE_MAP[value] for (key, value) in column_dtype.items()}


def _pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)


def _chunk_list(lst: list, size: int) -> list:
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def dataframe_to_cassandra_tuples(dataframe: DataFrame) -> list:
    """
    The Cassandra api uses tuples to send data for it's prepared statements, this method converts
    the rows of a dataframe into a list of tuples. It also converts the Pandas specific datatypes
    like Timestamp and NaN to python datatypes like datetime and None
    Args:
        dataframe (DataFrame): the dataframe to be converted to a list of tuples
    Returns: list[tuple]
    """
    return [tuple([_standardize_datatype(val) for val in row]) for index, row in
            dataframe.iterrows()]


def dicts_to_cassandra_tuples(data: list) -> list:
    """
    The Cassandra api uses tuples to send data for it's prepared statements, this method converts
    the list of dictionaries into a list of tuples
    Args:
        data (list[dict]): the list of dictonaries to be converted into a list of tuples
    Returns: list[tuple]
    """
    return [tuple(dct.values()) for dct in data]


def _standardize_datatype(val):
    if isinstance(val, Timestamp):
        return val.to_pydatetime()
    if val is NaT:
        return None
    return val


class ValidationError(Exception):
    """
    Exception class to raise validation issues
    Args:
        message (str): the error message
    """

    def __init__(self, message):
        super().__init__(message)


def _cql_manage_column_lists(data_frame, primary_key_column_list):
    column_dict = get_cql_columns_from_dataframe(data_frame)
    column_list = [f"{k} {v}" for (k, v) in column_dict.items()]
    _validate_primary_key_list(column_dict, primary_key_column_list)
    return column_list


def _validate_primary_key_list(column_dict, primary_key_column_list):
    if primary_key_column_list is None or not primary_key_column_list:
        raise ValidationError("please provide at least one primary key column")
    for key in primary_key_column_list:
        if key not in column_dict.keys():
            raise ValidationError(
                f"The column {key} is not in the column list, it cannot be specified as a primary "
                "key",
            )


class CassandraSecretsManager(SecretsManager):
    """
    Secrets manager for Cassandra configuration
    Args:
        source (KeyValueSource): a kv source that has secrets
        username_var (str): the variable name or the key for finding username
        password_var (str): the variable name or the key for finding password
    """

    def __init__(self, source: KeyValueSource = ENVIRONMENT,
                 username_var: str = "CASSANDRA_USERNAME",
                 password_var: str = "CASSANDRA_PASSWORD"):
        super().__init__([username_var, password_var], source)

        self.username = self.get_secret(username_var)
        self.password = self.get_secret(password_var)


@dataclass
class CassandraConnectionSettings:
    """Encapsulates the Cassandra connection settings"""
    cluster_ips: list
    port: int
    load_balancing_policy: LoadBalancingPolicy
    secrets_manager: CassandraSecretsManager
    ssl_options: dict = None


class CassandraConnectionManager:
    """
    Creates and manages connection to the cassandra cluster.
    Example -
    >>> from cassandra.policies import DCAwareRoundRobinPolicy
    >>> from cassandra.cqlengine import columns
    >>> from cassandra.cqlengine.management import sync_table
    >>> from cassandra.cqlengine.models import Model

    >>> load_balancing_policy = DCAwareRoundRobinPolicy(local_dc='AWS_VPC_AP_SOUTHEAST_2')

    >>> conn = CassandraConnectionManager(
    ...     settings = CassandraConnectionSettings(
    ...         cluster_ips=["1.1.1.1", "2.2.2.2"],
    ...         port=9042,
    ...         load_balancing_policy=load_balancing_policy,
    ...     )
    ... )

    >>> conn = CassandraConnectionManager(
    ...     CassandraConnectionSettings(
    ...         cluster_ips=["1.1.1.1", "2.2.2.2"],
    ...         port=9042,
    ...         load_balancing_policy=load_balancing_policy,
    ...         secrets_manager=CassandraSecretsManager(
    ...         username_var="MY_CUSTOM_USERNAME_ENV_VAR"),
    ...     )
    ... )

    For running Cassandra model operations
    >>> conn.setup_connection("dev_space")
    >>> class ExampleModel(Model):
    ...     example_type    = columns.Integer(primary_key=True)
    ...     created_at      = columns.DateTime()
    ...     description     = columns.Text(required=False)
    >>> sync_table(ExampleModel)

    Args:
        settings (hip_data_tools.apache.cassandra.CassandraConnectionSettings): settings to use
        for connecting to a cluster
    """

    def __init__(self, settings: CassandraConnectionSettings):
        self._settings = settings
        self.cluster = None
        self.session = None
        self._auth = PlainTextAuthProvider(
            username=self._settings.secrets_manager.username,
            password=self._settings.secrets_manager.password,
        )

    def get_cluster(self) -> Cluster:
        """
        get the cassandra Cluster object if it already exists or create a new one
        Returns: Cluster
        """
        if self.cluster is None:
            self.cluster = Cluster(
                contact_points=self._settings.cluster_ips,
                load_balancing_policy=self._settings.load_balancing_policy,
                port=self._settings.port,
                auth_provider=self._auth,
                ssl_options=self._settings.ssl_options,
            )
        return self.cluster

    def get_session(self, keyspace) -> Session:
        """
        get the cassandra Cluster's Session object if it already exists or create a new one
        Returns: Session
        """
        if self.session is None:
            self.session = self.get_cluster().connect(keyspace)

        return self.session

    def setup_connection(self, default_keyspace) -> None:
        """
        setups an implicit connection object for cassandra using the cassandra settings in the
        connection manager
        Args:
            default_keyspace (str): the keyspace to use as default for this implicit connection
        Returns: None
        """
        connection.setup(
            self._settings.cluster_ips,
            load_balancing_policy=self._settings.load_balancing_policy,
            auth_provider=self._auth,
            port=self._settings.port,
            ssl_options=self._settings.ssl_options,
            default_keyspace=default_keyspace)


class CassandraUtil:
    """
    Class to connect to and then retrieve, transform and upload data from and to cassandra
    """

    def __init__(self, keyspace: str, conn: CassandraConnectionManager):
        self.keyspace = keyspace
        self._conn = conn
        self.consistency_level = ConsistencyLevel.QUORUM
        self._session = self._conn.get_session(self.keyspace)

    def _cql_upsert_from_dict(self, data: dict, table: str):
        upsert_sql = f"""
        INSERT INTO {self.keyspace}.{table} 
        ({", ".join(data)}) 
        VALUES ({", ".join(['?' for key in data])});
            """
        LOG.debug(upsert_sql)
        return upsert_sql

    def _cql_upsert_from_dataframe(self, dataframe: DataFrame, table: str):
        upsert_sql = f"""
        INSERT INTO {self.keyspace}.{table} 
        ({", ".join(list(dataframe.columns.values))}) 
        VALUES ({", ".join(['?' for key in dataframe.columns.values])});
            """
        LOG.debug(upsert_sql)
        return upsert_sql

    def upsert_dataframe_in_batches(self,
                                    dataframe: DataFrame,
                                    table: str,
                                    batch_size: int = 2
                                    ) -> List[Result]:
        """
        Upload all data from a DataFrame onto a cassandra table
        Args:
            dataframe (DataFrame): a DataFrame to upsert
            table (str): the table to upsert data into
            table. If None then the DataFrame column names that match cassandra table anme will be
            upserted else ignored
            batch_size (int): limit on the number of prepared statements in the batch
        Returns: ResultSet
        """
        prepared_statement = self._session.prepare(
            self._cql_upsert_from_dataframe(dataframe, table))

        batches = self.prepare_batches(prepared_statement, dataframe_to_cassandra_tuples(dataframe),
                                       batch_size)
        return self._execute_batches(batches)

    def upsert_dataframe(self,
                         dataframe: DataFrame,
                         table: str
                         ) -> List[Result]:
        """
        Upload all data from a DataFrame onto a cassandra table
        Args:
            dataframe (DataFrame): a DataFrame to upsert
            table (str): the table to upsert data into
            table. If None then the DataFrame column names that match cassandra table anme will be
            upserted else ignored
            batch_size (int): limit on the number of prepared statements in the batch
        Returns: ResultSet
        """
        prepared_statement = self._session.prepare(
            self._cql_upsert_from_dataframe(dataframe, table))
        data_tuples = dataframe_to_cassandra_tuples(dataframe)
        return [self._execute_prepared_statement(data_tuple, prepared_statement)
                for data_tuple in data_tuples]

    def _execute_prepared_statement(self, data_tuple, prepared_statement):
        return self.execute(prepared_statement.bind(data_tuple), row_factory=dict_factory)

    def _execute_batches(self, batches: List):
        results = []
        LOG.info("Executing cassandra batches")
        for batch in batches:
            results.append(self._execute_batch(batch))
        LOG.info("finished %s batches", len(results))
        return results

    @retry(wait_exponential_multiplier=_RETRY_WAIT_MULTIPLIER_MS,
           wait_exponential_max=_RETRY_WAIT_MAX_MS,
           )
    def _execute_batch(self, batch):
        LOG.debug("Executing query: %s", batch)
        return self._session.execute(batch, timeout=300.0)

    def upsert_dictonary_list_in_batches(self,
                                         data: List[dict],
                                         table: str,
                                         batch_size: int = 2
                                         ) -> List[Result]:
        """
        Upsert a row into the cassandra table based on the dictionary key values
        Args:
            data (list[dict]): the data to be upserted
            table (str): the table to upsert data into
            batch_size (int): limit on the number of prepared statements in the batch
        Returns: None
        """
        prepared_statement = self._session.prepare(self._cql_upsert_from_dict(data[0], table))
        batches = self.prepare_batches(prepared_statement, dicts_to_cassandra_tuples(data),
                                       batch_size)
        return self._execute_batches(batches)

    def upsert_dictonary_list(self,
                              data: List[dict],
                              table: str,
                              ) -> List[Result]:
        """
        Upsert a row into the cassandra table based on the dictionary key values
        Args:
            data (list[dict]): the data to be upserted
            table (str): the table to upsert data into
        Returns: None
        """
        prepared_statement = self._session.prepare(self._cql_upsert_from_dict(data[0], table))
        data_tuples = dicts_to_cassandra_tuples(data)
        return [self._execute_prepared_statement(data_tuple, prepared_statement)
                for data_tuple in data_tuples]

    def create_table_from_model(self, model_class):
        """
        Create a table if not exists from the Model Class
        Args:
            model_class (class): class for the Model
        Returns: None
        """
        self._conn.setup_connection(default_keyspace=self.keyspace)
        sync_table(model_class)

    def create_table_from_dataframe(self,
                                    data_frame: DataFrame,
                                    table_name: str,
                                    primary_key_column_list: List[str],
                                    table_options_statement=""):
        """
        Create a new table in cassandra based on a pandas DataFrame
        Args:
            data_frame (DataFrame): the data frame to be synced
            table_name (str): name of the table to create
            primary_key_column_list (lost[str]): list of columns in the data frame that constitute
            primary key for new table
            table_options_statement (str): a cql valid WITH statement to specify table options as
            specified in https://docs.datastax.com/en/dse/6.0/cql/cql/cql_reference/cql_commands
            /cqlCreateTable.html#table_optionsŒ

        Returns: ResultSet

        """
        cql = self._dataframe_to_cassandra_ddl(
            data_frame,
            primary_key_column_list,
            table_name,
            table_options_statement)
        return self.execute(cql, row_factory=dict_factory)

    def _dataframe_to_cassandra_ddl(self,
                                    data_frame: DataFrame,
                                    primary_key_column_list: List[str],
                                    table_name: str,
                                    table_options_statement: str = ""):
        column_list = _cql_manage_column_lists(data_frame, primary_key_column_list)
        cql = f"""
        CREATE TABLE IF NOT EXISTS {self.keyspace}.{table_name} (
            {", ".join(column_list)},
            PRIMARY KEY ({", ".join(primary_key_column_list)}))
        {table_options_statement};
        """
        LOG.debug(cql)
        return cql

    def read_as_dictonary_list(self, query: str, **kwargs) -> List[dict]:
        """
        Read the results of a query in form of a list of dict
        Args:
            query (str):
        Returns: list[dict]
        """
        return self.execute(query, dict_factory, **kwargs).current_rows

    def read_as_dataframe(self, query: str, **kwargs) -> DataFrame:
        """
        Read the result of a query in form of a pandas DataFrame
        Args:
            query (str):
        Returns: DataFrame
        """
        # Since pandas DataFrame does not have a boolean truth value will need to access
        # protected _current_rows
        return self.execute(query, _pandas_factory, **kwargs)._current_rows

    def execute(self, query: str, row_factory: callable, **kwargs) -> Result:
        """
        Execute a cql command and retrieve data with the row factory
        Args:
            query (str):
            row_factory (callable):
            **kwargs: Kwargs to match the session.execute command in cassandra
        Returns: ResultSet
        """
        LOG.debug("Executing query: %s", query)
        if row_factory is not None:
            self._session.row_factory = row_factory
        return self._session.execute(query, **kwargs)

    def prepare_batches(self,
                        prepared_statement: PreparedStatement,
                        tuples: List[tuple],
                        batch_size: int) -> List:
        """
        Prepares a list of cassandra batched Statements out of a list of tuples and prepared
        statement
        Args:
            prepared_statement (PreparedStatement): the statement to be used for batching.
            tuples (list[tuple]): the data to be inserted.
            batch_size (int): limit on the number of prepared statements in the batch.
        Returns: list[BatchStatement]
        """
        batches = []
        LOG.debug("Preparing cassandra batches out of rows")
        batches_of_tuples = _chunk_list(tuples, batch_size)
        for tpl in batches_of_tuples:
            batch = self._prepare_batch(prepared_statement, tpl)
            batches.append(batch)
        LOG.info("created %s batches out of list of %s tuples", len(batches), len(tuples))
        return batches

    def _prepare_batch(self, prepared_statement, tuples):
        batch = BatchStatement(consistency_level=self.consistency_level)
        for tpl in tuples:
            batch.add(prepared_statement, tpl)
        return batch
