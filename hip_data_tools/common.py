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
