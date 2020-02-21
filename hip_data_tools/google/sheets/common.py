"""
Module contains variables and methods used for common / shared operations throughput the google
sheet package
"""

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from hip_data_tools.google.common import GoogleApiConnectionManager


class GoogleSheetConnectionManager(GoogleApiConnectionManager):
    """Encapsulates the Google sheets API connection settings"""

    def _credentials(self):
        """
        Get the credentials for google sheets
        Returns (ServiceAccountCredentials): credentials object to authorize google sheet service
        """
        return ServiceAccountCredentials.from_json_keyfile_dict(self.settings.keys_object,
                                                                self.scope)

    def get_connection(self):
        """
        Get the credentials for google sheets
        Returns: authorised connection for google sheets
        """
        return gspread.authorize(self._credentials())
