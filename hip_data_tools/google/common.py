"""
Module contains variables and methods used for common / shared operations throughput the google
services package
"""
from abc import ABC, abstractmethod
from typing import List

from attr import dataclass
from oauth2client.service_account import ServiceAccountCredentials

from hip_data_tools.common import SecretsManager, ENVIRONMENT, KeyValueSource


class GoogleApiSecretsManager(SecretsManager):
    """

    Args:
        source (KeyValueSource): a kv source that has secrets
        key_json_var (str): The variable name or the key for finding the key file json object
    """

    def __init__(self,
                 source: KeyValueSource = ENVIRONMENT,
                 key_json_var: str = "key_json"):
        self._required_keys = [key_json_var, ]
        super().__init__(self._required_keys, source)

        self.key_json = self.get_secret(key_json_var)


@dataclass
class GoogleApiConnectionSettings:
    """Encapsulates the Google API connection settings"""
    secrets_manager: GoogleApiSecretsManager


class GoogleApiConnectionManager(ABC):
    """
    Google API connection manager abstract class
    """

    def __init__(self, settings: GoogleApiConnectionSettings, scope: List[str]):
        self.settings = settings
        self.scope = scope

    def _credentials(self):
        """
        Get the credentials for google sheets
        Returns (ServiceAccountCredentials): credentials object to authorize google sheet service
        """
        return ServiceAccountCredentials.from_json_keyfile_dict(
            self.settings.secrets_manager.key_json,
            self.scope)

    @abstractmethod
    def get_client(self):
        """
        Get the connection for google sheets
        Returns: authenticated client object of the connection library
        """
