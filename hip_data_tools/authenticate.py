"""
This module creates a boto object and finds the optimal mode of authentication to connect to aws
"""
import os


class AwsConnection():
    """
    Athena utility class to connect to a database and perform some basic operations

    Args:
        mode (string): the mode to use for acquiring access / credentials to aws can be one of :
                assume_role|standard_env_var|custom_env_var
        settings (dict): Extra settings dictonary based on the connection mode chosen
                    example -
                    >>> conn = AwsConnection(
                    ... mode="assume_role",
                    ... region_name="ap-southeast-2",
                    ... settings={"profile_name": "default"})

                    # OR if you want to connect using the standard aws environment variables
                    (aws_access_key_id, aws_secret_access_key):
                    >>> conn = AwsConnection(
                    ... mode="standard_env_var",
                    ... region_name="ap-southeast-2",
                    ... settings={})

                    # OR if you want custom set of env vars to connect
                    >>> conn = AwsConnection(mode="custom_env_var",
                    ... region_name="ap-southeast-2",
                    ... settings={
                    ...      "aws_access_key_id_env_var": "aws_access_key_id",
                    ...      "aws_secret_access_key_env_var": "aws_secret_access_key"
                    ...  })

    """
    import boto3 as boto

    def __init__(self, mode, region_name, settings):
        self.connection_modes = {
            "assume_role": self._from_assume_role,
            "standard_env_var": self._from_env_var,
            "custom_env_var": self._from_custom_env_var
        }
        self.settings = settings
        self.region_name = region_name
        self._session = self.connection_modes[mode]

    def client(self, client_type):
        """
        Get a client for specific aws service
        Args:
            client_type (string): choice of aws service like s3, athena, etc. based on boto3:
            session.client(...)

        Returns (client): boto3 client

        """
        return self._session(**self.settings).client(
            client_type,
            region_name=self.region_name)

    def resource(self, resource_type):
        """
        Get a resource for specific aws service
        Args:
            resource_type (string): choice of aws service like s3, athena, etc. based on boto3:
            session.client(...)

        Returns (resource): boto3 resource_type

        """
        return self._session(**self.settings).resource(
            resource_type,
            region_name=self.region_name)

    def _from_assume_role(self, profile_name):
        """
        Create boto Session object using the aws profile
        Args:
            profile_name (string): name of the profile to use for connection
        Returns: Session object
        """
        # Any clients created from this session will use credentials
        # from the [dev] section of ~/.aws/credentials.
        return self.boto.Session(profile_name=profile_name)

    def _from_env_var(self):
        """
        Create boto Session object using the aws standard env vars
        Returns: Session object
        """
        return self.boto.Session()

    def _from_custom_env_var(self,
                             aws_access_key_id_env_var,
                             aws_secret_access_key_env_var,
                             aws_session_token_env_var=None):
        """
        Connect using a set of custom env vars, this is useful when there are multiple aws
        connections, and we want to utilise env vars for secret storage
        Args:
            aws_access_key_id_env_var (string): name of the env var with aws_access_key_id
            aws_secret_access_key_env_var (string): name of the env var with aws_secret_access_key
            aws_session_token_env_var (string): name of the env var with aws_session_token
        Returns: Session object
        """
        if aws_session_token_env_var is not None:
            return self.boto.Session(
                aws_access_key_id=os.getenv(aws_access_key_id_env_var),
                aws_secret_access_key=os.getenv(aws_secret_access_key_env_var),
                aws_session_token=os.getenv(aws_session_token_env_var))
        return self.boto.Session(
            aws_access_key_id=os.getenv(aws_access_key_id_env_var),
            aws_secret_access_key=os.getenv(aws_secret_access_key_env_var))
