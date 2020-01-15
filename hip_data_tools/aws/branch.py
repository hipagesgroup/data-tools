import gzip
import os
import uuid

# importing the requests library
import zlib
import requests


class BranchUtil:
    BRANCH_DATA_URL = 'https://api.branch.io/v3/export'

    def __init__(self, date):
        """
        initialise the the pre-requisites
        setting up headers
        Args:
            date (str): date to sunc
        """
        self.payload = {
            'branch_key': os.environ['BRANCH_KEY'],
            'branch_secret': os.environ['BRANCH_SECRET'],
            "export_date": date
        }

    def is_downloadables(self, url):
        """
        Determine where url has a downloadable file or html content
        Args:
            url (str): url to check
        Returns: bool
        """
        header = requests.head(url, allow_redirects=True).headers
        content_type = header.get('content-type')
        if 'text' in content_type.lower():
            return False
        if 'html' in content_type.lower():
            return False
        return True

    def write_to_file(self, localpath, content):
        """
        Method to write data to the file
        Args:
            localpath (str): local path to the file
            content (str): content of the url
        Returns: bool
        """
        print('Writing content  to filepath' + localpath)
        dirname = os.path.dirname(localpath)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        with open(localpath, 'w+') as file:
            file.write(str(content))

    def write_to_gzip_file(self, localpath, content):
        """
        Method to write data to a gzip file
        Args:
            localpath (str): local path to the file
            content (str): content of the file
        Returns: bool
        """
        print('Writing content to gzip filepath' + localpath)
        dirname = os.path.dirname(localpath)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        # start writing data to a gzip file
        with gzip.open(localpath, "wt") as file:
            file.write(str(content))

    def fetch_branch_data_url(self):
        """
        extract report url to download
        Returns: object response
        """
        print('Making API call to ' + self.BRANCH_DATA_URL)
        request = requests.post(url=self.BRANCH_DATA_URL, data=self.payload)
        # extracting headers
        self.requestHeaders = request.headers
        # check for http status codes
        request.raise_for_status()
        return request.json()

    def fetch_csv_data(self, url, localPath, filename):
        """
        extract gz file from the url
        Args:
            filename (str):
            url (str): url to make get call to
            localPath (str): local file path to save file content to
        Returns: dict
        """
        if filename is None:
            filename = str(uuid.uuid1()) + '.csv.gz'

        request = self.download_data(url)
        if self.is_downloadables(url):
            # adding 32 to windowBits will trigger header detection
            content = self.decompress(request.content)

            # start writing data to a gzip file
            self.write_to_gzip_file(localPath + '/' + filename, content)
        return {
            'status': True
        }

    def download_data(self, url):
        request = requests.get(url=url, allow_redirects=True)
        # extracting headers
        self.requestHeaders = request.headers
        # check for http status codes
        request.raise_for_status()
        return request

    def decompress(self, binary_content):
        content = zlib.decompress(binary_content, zlib.MAX_WBITS | 32).decode("utf-8")
        return content
