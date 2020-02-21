"""
Module contains variables and methods used for common / shared operations throughput the google
services package
"""
from abc import abstractmethod

from attr import dataclass


@dataclass
class GoogleApiConnectionSettings:
    """Encapsulates the Google API connection settings"""
    keys_object: str


class GoogleApiConnectionManager:
    """
    Google API connection manager abstract class
    """

    def __init__(self, settings: GoogleApiConnectionSettings):
        self.settings = settings
        self.scope = ['https://spreadsheets.google.com/feeds',
                      'https://www.googleapis.com/auth/drive']

    @abstractmethod
    def _credentials(self):
        """
        Get the credentials for a given google service
        Returns (ServiceAccountCredentials): credentials object to authorize google services
        """

    @abstractmethod
    def get_connection(self):
        """
        Get the credentials for google service
        Returns: authorised connection for google service
        """
