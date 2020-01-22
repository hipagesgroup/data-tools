import os
from unittest import TestCase

from hip_data_tools.apache.cassandra import CassandraSecretsManager
from hip_data_tools.aws.aws import AwsSecretsManager


class TestCommon(TestCase):
    def test__cassandra_secrets_manager_should_instantiate_with_sensible_defaults(self):
        os.environ["CASSANDRA_USERNAME"] = "abc"
        os.environ["CASSANDRA_PASSWORD"] = "def"
        actual = CassandraSecretsManager()
        self.assertEqual(actual.username, "abc")
        self.assertEqual(actual.password, "def")

    def test__cassandra_secrets_manager_should_raise_errors_when_keys_are_not_found(self):
        def func():
            CassandraSecretsManager(username_var="SOMEUNKNOWNVAR")

        self.assertRaises(Exception, func)

    def test__aws_secrets_manager_should_instantiate_with_sensible_defaults(self):
        os.environ["AWS_ACCESS_KEY_ID"] = "abc"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "def"
        actual = AwsSecretsManager()
        self.assertEqual(actual.aws_access_key_id, "abc")
        self.assertEqual(actual.aws_secret_access_key, "def")

    def test__aws_secrets_manager_should_raise_errors_when_keys_are_not_found(self):
        def func():
            AwsSecretsManager(access_key_id_var="SOMEUNKNOWNVAR")

        self.assertRaises(Exception, func)
