import os
from unittest import TestCase

from hip_data_tools.apache.cassandra import CassandraSecretsManager
from hip_data_tools.aws.aws import AwsSecretsManager
from hip_data_tools.common import get_release_version, get_long_description


class TestCommon(TestCase):
    def test__get_release_version__parses_tag(self):
        os.environ["GIT_TAG"] = "v1.3"
        actual = get_release_version()
        expected = "1.3"
        self.assertEqual(actual, expected)

    def test__get_release_version__works_without_tag(self):
        del os.environ["GIT_TAG"]
        actual = get_release_version()
        expected = "0.0"
        self.assertEqual(actual, expected)

    def test__get_long_description__reads_readme(self):
        actual = get_long_description()[0:16]
        expected = "# hip-data-tools"
        self.assertEqual(actual, expected)


