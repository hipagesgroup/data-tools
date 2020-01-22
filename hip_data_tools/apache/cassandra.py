"""
Utility for connecting to and transforming data in Cassandra clusters
"""
from ssl import SSLContext, PROTOCOL_TLSv1, CERT_REQUIRED

import pandas as pd
from attr import dataclass
from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import ResultSet, Cluster, Session
from cassandra.cqlengine import connection
from cassandra.cqlengine.management import sync_table
from cassandra.policies import LoadBalancingPolicy
from cassandra.query import dict_factory, BatchStatement
from pandas import DataFrame
from pandas._libs.tslibs.nattype import NaT
from pandas._libs.tslibs.timestamps import Timestamp

from hip_data_tools.common import KeyValueSource, ENVIRONMENT, SecretsManager


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
        batch = _prepare_batch(prepared_statement,
                               _extract_rows_from_dataframe(dataframe))
        return self._session.execute(batch)

    def upsert_dict(self, data: list, table: str) -> ResultSet:
        """
        Upsert a row into the cassandra table based on the dictionary key values
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
        Create a table if not exists from the Model Class
        Args:
            model_class (class): class for the Model
        Returns: None
        """
        self._conn.setup_connection(default_keyspace=self.keyspace)
        sync_table(model_class)

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
