import logging
import os

log = logging.getLogger(__name__)


def get_release_version():
    git_version = os.getenv("GIT_TAG", "v0.0")
    pypi_version = git_version.lstrip("v").strip()
    return pypi_version
