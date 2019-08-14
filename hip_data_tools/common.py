"""
Common variables and methods
"""

import logging
import os

LOG = logging.getLogger(__name__)


def get_release_version():
    """
    Gets the Releaseversion based on the latest git tag from GIT_TAG env var, else returns 0.0
    Returns: string containing version for the release

    """
    git_version = os.getenv("GIT_TAG", "v0.0")
    pypi_version = git_version.lstrip("v").strip()
    return pypi_version