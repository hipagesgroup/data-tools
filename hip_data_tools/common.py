"""
Module contains variables and methods used for common / shared operations throughput the package
"""

import logging
import os
import uuid

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
    Get the contents of readme file as long_description
    Returns: bytes containing readme file

    """
    file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'README.md'))
    with open(file_path) as readme_file:
        return readme_file.read()


def _generate_random_file_name():
    random_tmp_file_nm = "/tmp/tmp_file{}".format(str(uuid.uuid4()))
    return random_tmp_file_nm


def get_from_env_or_default_with_warning(env_var, default_val):
    """
    Get environmental variables or, if they aren't present, default to a
    specific value
    Args:
        env_var (string): Name of the environmental variable to read
        default_val (any): Value to default to if relevant env var is not
                        present

    Returns (any): Value

    """

    value = os.environ.get(env_var)

    if value is None:
        warning_string = "Environmental variable {} not found, " \
                         "defaulting to {}".format(env_var,
                                                   str(default_val))
        LOG.warning(warning_string)
        value = default_val

    return value
