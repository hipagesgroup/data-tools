"""
Module contains variables and methods used for common / shared operations throughput the package
"""

import logging
import os

LOG = logging.getLogger(__name__)
"""
logger object to handle logging in the entire package
"""


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
    Get the contents of reame file as long_description
    Returns: bytes containing readme file

    """
    file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'README.md'))
    with open(file_path) as readme_file:
        return readme_file.read()
