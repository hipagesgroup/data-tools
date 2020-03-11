"""
Module contains variables and methods used for common / shared operations throughput the package
"""

import logging
import os
import uuid
from abc import ABC, abstractmethod

LOG = logging.getLogger(__name__)
"""
logger object to handle logging in the entire package
"""


def get_release_version():
    """
    Gets the Release version based on the latest git tag from GIT_TAG env var, else returns 0.0
    Returns: string containing version for the release

    """
    git_version = os.getenv("GIT_TAG", "v0.0")
    pypi_version = git_version.lstrip("v").strip()
    return pypi_version


def get_long_description():
    """
    Get the contents of readme file as long_description
    Returns: bytes containing readme file

    """
    file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'README.md'))
    with open(file_path) as readme_file:
        return readme_file.read()


def _generate_random_file_name():
    random_tmp_file_nm = "/tmp/tmp_file{}".format(str(uuid.uuid4()))
    return random_tmp_file_nm


def get_from_env_or_default_with_warning(env_var, default_val):
    """
    Get environmental variables or, if they aren't present, default to a
    specific value
    Args:
        env_var (string): Name of the environmental variable to read
        default_val (any): Value to default to if relevant env var is not
                        present

    Returns (any): Value

    """

    value = os.environ.get(env_var)

    if value is None:
        warning_string = "Environmental variable {} not found, " \
                         "defaulting to {}".format(env_var,
                                                   str(default_val))
        LOG.warning(warning_string)
        value = default_val

    return value


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

    @abstractmethod
    def exists(self, key):
        """
        Abstract methosd to verify if a key exists in the given data store
        Args:
            key (str): the key to be verified for existance
        Returns: bool
        """


class EnvironmentKeyValueSource(KeyValueSource):
    """
    class for sourcing secrets from env variables
    """

    def exists(self, key):
        """
        verify if a key exists
        Args:
            key (str): the key to be verified for existance
        Returns: bool
        """
        if os.getenv(key) is None:
            return False
        return True

    def get(self, key):
        """
        get the value for a given key
        Args:
            key (str): the key for which the value needs to be returned
        Returns: str
        """
        return os.getenv(key)


class DictKeyValueSource(KeyValueSource):
    """
    class for sourcing secrets from a provided Dict object, usually used for testing
    """

    def __init__(self, data):
        self.data = data

    def exists(self, key):
        """
        verify if a key exists
        Args:
            key (str): the key to be verified for existance
        Returns: bool
        """
        if key in self.data:
            return True
        return False

    def get(self, key):
        """
        get the value for a given key
        Args:
            key (str): the key for which the value needs to be returned
        Returns: str
        """
        return self.data[key]


ENVIRONMENT: EnvironmentKeyValueSource = EnvironmentKeyValueSource()
"""
Standard Environment Variable Secret source to be reused across the project
"""


class SecretsManager(ABC):
    """
    A secret management abstract class that provides ways of extracting secrets
    The class allows a subsequent connection class to use env vars to extract secrets in a
    structured manner
    Args:
        required_keys (list[str]): a list of keys which will be checked for existence
        source (KeyValueSource): a kv source that has secrets
    """

    def __init__(self, required_keys: list, source: KeyValueSource):
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
