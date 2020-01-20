import boto3 as boto
from attr import dataclass

from hip_data_tools.connect.secrets import AwsSecretsManager


@dataclass
class AwsConnectionSettings:
    """Encapsulates the Cassandra connection settings"""
    region: str
    profile: str = None
    secrets_manager: AwsSecretsManager = AwsSecretsManager()


class AwsConnectionManager:
    """
    utility class to connect to a database and perform some basic operations
    example -
    to connect using an aws cli profile
    >>> conn = AwsConnectionManager(
    ...     AwsConnectionSettings(region_name="ap-southeast-2", profile="default"))

    # OR if you want to connect using the standard aws environment variables
    (aws_access_key_id, aws_secret_access_key):
    >>> conn = AwsConnectionManager(settings=AwsConnectionSettings(region_name="ap-southeast-2"))

    # OR if you want custom set of env vars to connect
    >>> conn = AwsConnectionManager(
    ...     settings=AwsConnectionSettings(
    ...         region_name="ap-southeast-2",
    ...         secrets_manager=AwsSecretsManager(
    ...             access_key_id_var="SOME_CUSTOM_AWS_ACCESS_KEY_ID",
    ...             secret_access_key_var="SOME_CUSTOM_AWS_SECRET_ACCESS_KEY",
    ...             use_session_token=True,
    ...             aws_session_token_var="SOME_CUSTOM_AWS_SESSION_TOKEN"
    ...             )
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
