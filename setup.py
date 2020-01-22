"""
Setup and release this package for wider consumption using setup tools
"""
import sys

from setuptools import setup, find_packages
from setuptools.command.test import test as test_command

from hip_data_tools.common import get_release_version, get_long_description


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
        "boto3==1.9.216",
        "joblib==0.13.2",
        "pandas==0.25.1",
        "GitPython==3.0.0",
        'confluent-kafka==1.1.0',
        "pyarrow==0.14.1",
        "cassandra-driver==3.21.0",
    ],
    test_suite="tests",
    tests_require=[
        'attrs==19.1.0',
        'pytest==4.2.1',
        'pytest-mock==1.10.1',
        "moto==1.3.13",
        "pyarrow==0.14.1",
        "python-snappy==0.5.4",
        "pytest-stub==0.1.0",
        "freezegun==0.1.11",
    ],
    cmdclass={'test': PyTest},
    python_requires='~=3.6',
    version=get_release_version(),

)
