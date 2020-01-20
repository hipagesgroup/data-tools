"""Mpodule for abstract connection class"""

import os
from abc import ABC, abstractmethod


class KeyValueSource(ABC):
    """
    Abstract class for sourcing secrets, it is a key value source for retrieving values for keys
    """

    @abstractmethod
    def get(self, key):
        pass

    @abstractmethod
    def exists(self, key):
        pass


class EnvironmentKeyValueSource(KeyValueSource):
    def exists(self, key):
        return os.getenv(key)

    def get(self, key):
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
    """

    def __init__(self, required_keys: list, source: KeyValueSource):
        self.keys = required_keys
        self._source = source
        for key in self.keys:
            if not self._source.exists(key):
                raise Exception("Required Environment Variable {} does not exist!".format(key))

    def get_secret(self, key):
        return self._source.get(key)


class CassandraSecretsManager(SecretsManager):
    def __init__(self, source: KeyValueSource = ENVIRONMENT,
                 username_var: str = "CASSANDRA_USERNAME",
                 password_var: str = "CASSANDRA_PASSWORD"):
        super().__init__([username_var, password_var], source)

        self.username = self.get_secret(username_var)
        self.password = self.get_secret(password_var)


class AwsSecretsManager(SecretsManager):
    def __init__(self, source: KeyValueSource = ENVIRONMENT,
                 access_key_id_var="AWS_ACCESS_KEY_ID",
                 secret_access_key_var="AWS_SECRET_ACCESS_KEY", use_session_token=False,
                 aws_session_token_var="AWS_SESSION_TOKEN"):
        self._required_keys = [access_key_id_var, secret_access_key_var, ]
        if use_session_token:
            self._required_keys.append(aws_session_token_var)
        super().__init__(self._required_keys, source)

        self.aws_access_key_id = self.get_secret(access_key_id_var)
        self.aws_secret_access_key = self.get_secret(secret_access_key_var)
        self.aws_session_token = self.get_secret(aws_session_token_var)
