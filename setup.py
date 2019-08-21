"""
Setup and release this package for wider consumption using setup tools
"""

from setuptools import setup, find_packages

from hip_data_tools.common import get_release_version, get_long_description

setup(
    long_description=get_long_description(),
    long_description_content_type='text/markdown',
    packages=find_packages(include=["hip_data_tools", "hip_data_tools.*"]),
    zip_safe=False,
    install_requires=[
        "boto3==1.9.206"
    ],
    test_suite="tests",
    tests_require=[
    ],
    python_requires='~=3.6',
    version=get_release_version(),
)
