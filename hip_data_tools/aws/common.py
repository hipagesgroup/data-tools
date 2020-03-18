from abc import ABC
from typing import Any, Optional

import boto3 as boto
from attr import dataclass
from botocore.client import BaseClient

from hip_data_tools.common import KeyValueSource, ENVIRONMENT, SecretsManager


class AwsSecretsManager(SecretsManager):
    """

    Args:
        source (KeyValueSource): a kv source that has secrets
        access_key_id_var (str): the variable name or the key for finding access_key_id
        secret_access_key_var (str): the variable name or the key for finding secret_access_key
        use_session_token (bool): flag to check the session token is required or not
        aws_session_token_var (str): the variable name or the key for finding aws_session_token
    """

    def __init__(self,
                 source: KeyValueSource = ENVIRONMENT,
                 access_key_id_var: str = "AWS_ACCESS_KEY_ID",
                 secret_access_key_var: str = "AWS_SECRET_ACCESS_KEY",
                 use_session_token: bool = False,
                 aws_session_token_var: str = "AWS_SESSION_TOKEN"):
        self._required_keys = [access_key_id_var, secret_access_key_var, ]
        if use_session_token:
            self._required_keys.append(aws_session_token_var)
        super().__init__(self._required_keys, source)

        self.aws_access_key_id = self.get_secret(access_key_id_var)
        self.aws_secret_access_key = self.get_secret(secret_access_key_var)
        self.aws_session_token = self.get_secret(aws_session_token_var)


@dataclass
class AwsConnectionSettings:
    """Encapsulates the Cassandra connection settings"""
    region: str
    profile: Optional[str]
    secrets_manager: Optional[AwsSecretsManager]


class AwsConnectionManager:
    """
    utility class to connect to a database and perform some basic operations
    example -
    to connect using an aws cli profile
    >>> conn = AwsConnectionManager(
    ...     AwsConnectionSettings(region_name="ap-southeast-2", profile="default",
    secrets_manager=None))

    # OR if you want to connect using the standard aws environment variables
    (aws_access_key_id, aws_secret_access_key):
    >>> conn = AwsConnectionManager(settings=AwsConnectionSettings(region_name="ap-southeast-2",
    profile=None, secrets_manager=AwsSecretsManager()))

    # OR if you want custom set of env vars to connect
    >>> conn = AwsConnectionManager(
    ...     settings=AwsConnectionSettings(
    ...         region_name="ap-southeast-2",
    ...         secrets_manager=AwsSecretsManager(
    ...             access_key_id_var="SOME_CUSTOM_AWS_ACCESS_KEY_ID",
    ...             secret_access_key_var="SOME_CUSTOM_AWS_SECRET_ACCESS_KEY",
    ...             use_session_token=True,
    ...             aws_session_token_var="SOME_CUSTOM_AWS_SESSION_TOKEN"
    ...             ),
    ...         profile=None,
    ...         )
    ...     )

    Args:
        settings (AwsConnectionSettings): settings to use for connecting to aws
    """

    def __init__(self, settings: AwsConnectionSettings):
        self.settings = settings
        self._session = None

    def client(self, client_type):
        """
        Get a client for specific aws service
        Args:
            client_type (string): choice of aws service like s3, athena, etc. based on boto3:
            session.client(...)

        Returns (client): boto3 client

        """
        return self._get_session().client(client_type, region_name=self.settings.region)

    def resource(self, resource_type):
        """
        Get a resource for specific aws service
        Args:
            resource_type (string): choice of aws service like s3, athena, etc. based on boto3:
            session.client(...)
        Returns (resource): boto3 of type resource_type
        """
        return self._get_session().resource(resource_type, region_name=self.settings.region)

    def _get_session(self):
        """
        Connect and provide an aws session object
        Returns: Session object
        """
        if self._session is None:
            if self.settings.profile is not None:
                self._session = boto.Session(profile_name=self.settings.profile)
            else:
                self._session = boto.Session(
                    aws_access_key_id=self.settings.secrets_manager.aws_access_key_id,
                    aws_secret_access_key=self.settings.secrets_manager.aws_secret_access_key,
                    aws_session_token=self.settings.secrets_manager.aws_session_token,
                )
        return self._session


class AwsUtil(ABC):
    """
    Common Aws class to use boto connection and resources
    Args:
        conn (AwsConnectionManager): Connection to use for accessing aws resources
        boto_type: the type of boto client / resource to instantiate
    """

    def __init__(self, conn: AwsConnectionManager, boto_type: str):
        self.conn = conn
        self._client = None
        self._resource = None
        self.boto_type = boto_type

    def get_client(self) -> BaseClient:
        """
        returns a boto client and creates one if not present
        Returns: BaseClient
        """
        if self._client is None:
            self._client = self.conn.client(self.boto_type)
        return self._client

    def get_resource(self) -> Any:
        """
        returns a boto resporce and creates one if not present
        Returns: Any
        """
        if self._resource is None:
            self._resource = self.conn.resource(self.boto_type)
        return self._resource
