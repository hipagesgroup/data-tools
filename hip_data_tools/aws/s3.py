import uuid

from joblib import load


class S3Util:
    def __init__(self, conn, bucket):
        self.conn = conn
        self.bucket = bucket

    def _client(self):
        return self.conn.get_client(client_type='s3')

    def _resource(self):
        s3 = self.conn.get_resource('s3')
        return s3

    def download_file(self, local_file_path, s3_key):
        """
        Downloads a file from a path on s3 to a local path on disk
        :param local_file_path: Absolute path on s3
        :param s3_key: Absolute path to save file to local
        """
        s3 = self._client()

        s3.download_file(self.bucket, s3_key, local_file_path.strip("/"))

    def upload_file(self, local_file_path, s3_key):
        """
        Uploads a file from local to s3
        :param local_file_path: Absolute local path to the file to upload
        :param s3_key: Absolute path within the s3 buck to upload the file
        :return:
        """
        s3 = self._client()
        s3.upload_file(local_file_path, self.bucket, s3_key.strip("/"))

    def download_object_and_deserialse(self, s3_key, local_file_path=None):
        """
        Download a serialised object from S3 and deserialse
        :param s3_key: Absolute path on s3 to the file
        :return: The deserialsed object
        """
        if local_file_path is None:
            local_file_path = "/tmp/tmp_file" + str(uuid.uuid4())

        self.download_file(s3_key, local_file_path)
        return load(local_file_path)

    def create_bucket(self):
        self._resource().create_bucket(Bucket=self.bucket)
