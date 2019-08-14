"""
This module creates a boto object and finds the optimal mode of authentication to connect to aws
"""
import os


class AwsConnection():
    import boto3 as boto3
    boto = boto3

    def __init__(self, mode, settings):
        self.CONNECTION_MODES = {
            "assume_role": self._from_assume_role,
            "standard_env_var": self._from_env_var,
            "custom_env_var": self._from_custom_env_var
        }
        self.settings = settings
        self.connection_method = self.CONNECTION_MODES[mode]

    def get_client(self, client_type):
        return self.connection_method(client_type, **self.settings)

    def _from_assume_role(self, client_type, profile_name):
        session = self.boto.Session(profile_name=profile_name)
        # Any clients created from this session will use credentials
        # from the [dev] section of ~/.aws/credentials.
        return session.client(client_type)

    def _from_env_var(self, client_type):
        return self.boto.client(client_type)

    def _from_custom_env_var(self, client_type, aws_access_key_id_env_var, aws_secret_access_key_env_var,
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
