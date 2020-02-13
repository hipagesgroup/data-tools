from attr import dataclass


@dataclass
class GoogleApiConnectionSettings:
    """Encapsulates the Google API connection settings"""
    key_file_path: str


class GoogleApiConnectionManager:

    def __init__(self, settings: GoogleApiConnectionSettings):
        self.settings = settings
        self.scope = ['https://spreadsheets.google.com/feeds',
                      'https://www.googleapis.com/auth/drive']

    def _credentials(self):
        """
        Get the credentials for a given google service
        Returns (ServiceAccountCredentials): credentials object to authorize google services
        """
        return None
