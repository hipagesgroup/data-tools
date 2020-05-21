"""
Setup and release this package for wider consumption using setup tools
"""
import os

import sys
from setuptools import setup, find_packages
from setuptools.command.test import test as test_command


def get_release_version():
    """
    Gets the Release version based on the latest git tag from GIT_TAG env var, else returns 0.0
    Returns: string containing version for the release

    """
    git_version = os.getenv("GIT_TAG", "v0.0")
    pypi_version = git_version.lstrip("v").strip()
    return pypi_version


def get_long_description():
    """
    Get the contents of readme file as long_description
    Returns: bytes containing readme file

    """
    file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'README.md'))
    with open(file_path) as readme_file:
        return readme_file.read()


class PyTest(test_command):
    """
    Setup tools class used to initiate pytests
    """
    user_options = [('pytest-args=', 'a', "Arguments to pass to py.test")]

    def initialize_options(self):
        """
        Added options to apply to the pytests
        Returns: None
        """
        test_command.initialize_options(self)
        self.pytest_args = ["-vv"]

    def finalize_options(self):
        """
        Options for finalisation
        Returns: None
        """
        test_command.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        """Test executions options"""
        # import here, cause outside the eggs aren't loaded
        import pytest
        errno = pytest.main(self.pytest_args)
        sys.exit(errno)


setup(
    long_description=get_long_description(),
    long_description_content_type='text/markdown',
    packages=find_packages(include=["hip_data_tools", "hip_data_tools.*"]),
    zip_safe=False,
    entry_points={'console_scripts': [
        'version-tracker=hip_data_tools.hipages.version_tracking:main']},
    install_requires=[
        "stringcase",
        "attrs",
        "boto3",
        "joblib",
        "pandas",
        "GitPython",
        "confluent-kafka",
        "pyarrow",
        "cassandra-driver",
        "tqdm",
        "retrying",
        "arrow",
        "s3fs",
        "dataclasses",
        "oauth2client",
        "gspread",
        "googleads",
    ],
    test_suite="tests",
    tests_require=[
        'pytest',
        'pytest-mock==1.10.1',
        "moto",
        "pyarrow",
        "python-snappy",
        "pytest-stub",
        "freezegun==0.1.11",
        "testcontainers",
    ],
    cmdclass={'test': PyTest},
    python_requires='~=3.6',
    version=get_release_version(),

)
