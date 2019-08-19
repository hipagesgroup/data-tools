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
                    >>> conn = AwsConnection(mode="assume_role", settings={"profile_name":
                    "default"})

                    # OR if you want to connect using the standard aws environment variables
                    (aws_access_key_id, aws_secret_access_key):
                    >>> conn = AwsConnection(mode="standard_env_var", settings={})

                    # OR if you want custom set of env vars to connect
                    >>> conn = AwsConnection(mode="custom_env_var", settings={
                         "aws_access_key_id_env_var": "aws_access_key_id",
                         "aws_secret_access_key_env_var": "aws_secret_access_key"
                     })

    """
    import boto3 as boto

    def __init__(self, mode, settings):
        self.connection_modes = {
            "assume_role": self._from_assume_role,
            "standard_env_var": self._from_env_var,
            "custom_env_var": self._from_custom_env_var
        }
        self.settings = settings
        self.connection_method = self.connection_modes[mode]

    def get_client(self, client_type):
        """
        Get a client for specific aws service
        Args:
            client_type (string): choice of aws service like s3, athena, etc. based on boto3:
            session.client(...)

        Returns (client): boto3 client

        """
        return self.connection_method(client_type, **self.settings)

    def _from_assume_role(self, client_type, profile_name):
        session = self.boto.Session(profile_name=profile_name)
        # Any clients created from this session will use credentials
        # from the [dev] section of ~/.aws/credentials.
        return session.client(client_type)

    def _from_env_var(self, client_type):
        return self.boto.client(client_type)

    def _from_custom_env_var(self,
                             client_type,
                             aws_access_key_id_env_var,
                             aws_secret_access_key_env_var,
                             aws_session_token_env_var=None):
        if aws_session_token_env_var is not None:
            return self.boto.client(
                client_type,
                aws_access_key_id=os.getenv(aws_access_key_id_env_var),
                aws_secret_access_key=os.getenv(aws_secret_access_key_env_var),
                aws_session_token=os.getenv(aws_session_token_env_var)
            )
        return self.boto.client(
            client_type,
            aws_access_key_id=os.getenv(aws_access_key_id_env_var),
            aws_secret_access_key=os.getenv(aws_secret_access_key_env_var)
        )
