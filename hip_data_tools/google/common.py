"""
Module contains variables and methods used for common / shared operations throughput the google
services package
"""
from abc import ABC, abstractmethod
from typing import List

from attr import dataclass
from oauth2client.service_account import ServiceAccountCredentials


@dataclass
class GoogleApiConnectionSettings:
    """Encapsulates the Google API connection settings"""
    keys_object: str


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
            self.settings.keys_object,
            self.scope)

    @abstractmethod
    def get_client(self):
        """
        Get the connection for google sheets
        Returns: authenticated client object of the connection library
        """
        pass
