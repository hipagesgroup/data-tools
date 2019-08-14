import os
from unittest import TestCase

from hip_data_tools.common import get_release_version


class TestAthenaUtil(TestCase):

    def test__get_release_version__works_without_a_version(self):
        del os.environ["GIT_TAG"]
        actual = get_release_version()
        expected = "0.0"
        self.assertEquals(actual, expected)

    def test__get_release_version__works_with_a_version(self):
        os.environ["GIT_TAG"] = "v1.2.3.4"
        actual = get_release_version()
        expected = "1.2.3.4"
        self.assertEquals(actual, expected)
