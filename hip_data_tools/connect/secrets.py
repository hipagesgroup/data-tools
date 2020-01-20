"""Mpodule for abstract connection class"""

import os
from abc import ABC, abstractmethod


class KeyValueSource(ABC):
    """
    Abstract class for sourcing secrets, it is a key value source for retrieving values for keys
    """

    @abstractmethod
    def get(self, key: str) -> str:
        """
        Abstract method to get the value for a given key
        Args:
            key (str): the key for which the value needs to be returned
        Returns: str
        """
        pass

    @abstractmethod
    def exists(self, key):
        """
        Abstract methosd to verify if a key exists in the given data store
        Args:
            key (str): the key to be verified for existance
        Returns: bool
        """
        pass


class EnvironmentKeyValueSource(KeyValueSource):
    def exists(self, key):
        """
        verify if a key exists
        Args:
            key (str): the key to be verified for existance
        Returns: bool
        """
        return os.getenv(key)

    def get(self, key):
        """
        get the value for a given key
        Args:
            key (str): the key for which the value needs to be returned
        Returns: str
        """
        return os.getenv(key)


ENVIRONMENT: EnvironmentKeyValueSource = EnvironmentKeyValueSource()


class FileKeyValueSource(KeyValueSource):
    def __init__(self, path):
        pass

    def exists(self, key):
        pass

    def get(self, key):
        pass


class SecretsManager(ABC):
    """
    A secret management abstract class that provides ways of extracting secrets
    The class allows a subsequent connection class to use env vars to extract secrets in a
    structured manner
    Args:
        required_keys (list[str]): a list of keys which will be checked for existence
        source (KeyValueSource): a kv source that has secrets
    """

    def __init__(self, required_keys: list[str], source: KeyValueSource):
        self.keys = required_keys
        self._source = source
        for key in self.keys:
            if not self._source.exists(key):
                raise Exception("Required Environment Variable {} does not exist!".format(key))

    def get_secret(self, key):
        """
        get the secret valye for a given key
        Args:
            key (str): the key for given secret
        Returns: str
        """
        return self._source.get(key)


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
