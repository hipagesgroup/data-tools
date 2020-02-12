import logging as log

from attr import dataclass
from oauth2client.service_account import ServiceAccountCredentials


@dataclass
class GoogleApiConnectionSettings:
    """Encapsulates the Google API connection settings"""
    key_file_path: str


class GoogleApiConnectionManager:

    def __init__(self, settings: GoogleApiConnectionSettings):
        self.settings = settings
        self.scope = ['https://spreadsheets.google.com/feeds',
                      'https://www.googleapis.com/auth/drive']

    def credentials(self, service):
        """
        Get the credentials for a given google service
        Args:
            service (string): choice of google service like sheet, doc and etc
        Returns (ServiceAccountCredentials): credentials object to authorize google services
        """
        if service == 'sheet':
            return ServiceAccountCredentials.from_json_keyfile_name(self.settings.key_file_path, self.scope)
        else:
            log.error("Requested service is not found")
            return None
