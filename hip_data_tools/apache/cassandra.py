"""
Utility for connecting to and transforming data in Cassandra clusters
"""
import logging as log
import math
from ssl import SSLContext, PROTOCOL_TLSv1, CERT_REQUIRED

import pandas as pd
from attr import dataclass
from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import ResultSet, Cluster, Session
from cassandra.cqlengine import connection
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.models import Model
from cassandra.policies import LoadBalancingPolicy
from cassandra.query import dict_factory, BatchStatement
from pandas import DataFrame
from pandas._libs.tslibs.nattype import NaT
from pandas._libs.tslibs.timestamps import Timestamp

from hip_data_tools.common import KeyValueSource, ENVIRONMENT, SecretsManager

CASSANDRA_BATCH_LIMIT = 20

def get_ssl_context(cert_path: str) -> SSLContext:
    """
    Creates an ssl context if required for connecting to cassandra
    Args:
        cert_path (str): path where the ssl cetificate pem file is stored
    Returns: SSLContext
    """
    ssl_context = SSLContext(PROTOCOL_TLSv1)
    ssl_context.load_verify_locations(cert_path)
    ssl_context.verify_mode = CERT_REQUIRED
    return ssl_context


PYTHON_TO_CASSANDRA_DATA_TYPE_MAP = {
    "Timestamp": "timestamp",
    "str": "varchar",
    "int64": "bigint",
    "int32": "int",
    "dict": "map",
    "float64": "double",
    "UUID": "UUID",
}


def _get_data_frame_column_types(df):
    df_col_dict = {}
    for col in df:
        df_col_dict[col] = type(df[col][0]).__name__
    return df_col_dict


def convert_dataframe_columns_to_cassandra(df):
    column_dtype = _get_data_frame_column_types(df)
    cassandra_columns = {key: PYTHON_TO_CASSANDRA_DATA_TYPE_MAP[value] for (key, value) in
                         column_dtype.items()}
    return cassandra_columns


def _pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)


def _prepare_batches(prepared_statement, rows) -> list:
    batches = []
    itr = 0
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    for row in rows:
        if itr >= CASSANDRA_BATCH_LIMIT:
            batches.append(batch)
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
            itr = 0
        batch.add(prepared_statement, row)
        itr += 1
    batches.append(batch)
    log.warning("created %s batches out of df of %s rows", len(batches), len(rows))
    return batches


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


def _cql_manage_column_lists(data_frame, primary_key_column_list):
    if primary_key_column_list is None or len(primary_key_column_list) < 1:
        raise Exception("please provide at least one primary key column")
    column_dict = convert_dataframe_columns_to_cassandra(data_frame)
    for pk in primary_key_column_list:
        if pk not in column_dict.keys():
            raise Exception(
                "The column %s is not in the column list, it cannot be specified as a primary "
                "key",
                pk)
    column_list = [f"{k} {v}" for (k, v) in column_dict.items()]
    return column_list


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
    ssl_context: SSLContext = None


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
                ssl_context=self._settings.ssl_context,
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
            ssl_context=self._settings.ssl_context,
            default_keyspace=default_keyspace, )


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
        log.info(upsert_sql)
        return upsert_sql

    def _cql_upsert_from_dataframe(self, dataframe, table):
        upsert_sql = f"""
        INSERT INTO {self.keyspace}.{table} 
        ({", ".join(list(dataframe.columns.values))}) 
        VALUES ({", ".join(['?' for key in dataframe.columns.values])});
            """
        log.info(upsert_sql)
        return upsert_sql

    def upsert_dataframe(self, dataframe: DataFrame, table: str) -> list:
        """
        Upload all data from a DataFrame onto a cassandra table
        Args:
            dataframe (DataFrame): a DataFrame to upsert
            table (str): the table to upsert data into
            table. If None then the DataFrame column names that match cassandra table anme will be
            upserted else ignored
        Returns: ResultSet
        """
        prepared_statement = self._session.prepare(
            self._cql_upsert_from_dataframe(dataframe, table))
        # Batch statement cannot contain more than 65535 statements, create sub batches
        results = []
        batches = _prepare_batches(prepared_statement, _extract_rows_from_dataframe(dataframe))
        for batch in batches:
            results.append(self._session.execute(batch, timeout=300.0))
        log.warning("finished %s batches", len(results))
        return results

    def upsert_dict(self, data: list, table: str) -> ResultSet:
        """
        Upsert a row into the cassandra table based on the dictionary key values
        Args:
            data (list[dict]):
            table (str):
        Returns: None
        """
        prepared_statement = self._session.prepare(self._cql_upsert_from_dict(data, table))
        batch = _prepare_batches(prepared_statement, _extract_rows_from_list_of_dict(data))
        return self._session.execute(batch)

    def create_table_from_model(self, model_class):
        """
        Create a table if not exists from the Model Class
        Args:
            model_class (class): class for the Model
        Returns: None
        """
        self._conn.setup_connection(default_keyspace=self.keyspace)
        sync_table(model_class)

    def create_table_from_dataframe(self, data_frame, table_name, primary_key_column_list,
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
        cql = self._dataframe_to_cassandra_ddl(data_frame, primary_key_column_list, table_name,
                                               table_options_statement)
        return self.execute(cql, row_factory=dict_factory)

    def _dataframe_to_cassandra_ddl(self, data_frame, primary_key_column_list, table_name,
                                    table_options_statement):
        column_list = _cql_manage_column_lists(data_frame, primary_key_column_list)
        cql = f"""
        CREATE TABLE IF NOT EXISTS {self.keyspace}.{table_name} (
            {", ".join(column_list)},
            PRIMARY KEY ({", ".join(primary_key_column_list)}))
        {table_options_statement};
        """
        log.info(cql)
        return cql

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
        Read the result of a query in form of a pandas DataFrame
        Args:
            query (str):
        Returns: DataFrame
        """
        # Since pandas DataFrame does not have a boolean truth value will need to access
        # protected _current_rows
        return self.execute(query, _pandas_factory, **kwargs)._current_rows

    def execute(self, query, row_factory, **kwargs):
        """
        Execute a cql command and retrieve data with the row factory
        Args:
            query (str):
            row_factory (callable):
            **kwargs: Kwargs to match the session.execute command in cassandra
        Returns: ResultSet
        """
        if row_factory is not None:
            self._session.row_factory = row_factory
        return self._session.execute(query, **kwargs)
