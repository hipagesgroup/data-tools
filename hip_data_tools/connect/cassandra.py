from ssl import SSLContext, PROTOCOL_TLSv1, CERT_REQUIRED

from attr import dataclass
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, Session
from cassandra.cqlengine import connection
from cassandra.policies import LoadBalancingPolicy
from hip_data_tools.connect.secrets import CassandraSecretsManager


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


@dataclass
class CassandraConnectionSettings:
    """Encapsulates the Cassandra connection settings"""
    cluster_ips: list[str]
    port: int
    load_balancing_policy: LoadBalancingPolicy
    secrets_manager: CassandraSecretsManager = CassandraSecretsManager()
    ssl_context: SSLContext = None


class CassandraConnectionManager:
    """
    Creates and manages connection to a given cassandra cluster.
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
        settings (CassandraConnectionSettings): settings to use for connecting to a cluster
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

    def get_session(self) -> Session:
        """
        get the cassandra Cluster's Session object if it already exists or create a new one
        Returns: Session
        """
        if self.session is None:
            self.session = self.get_cluster().connect()

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
