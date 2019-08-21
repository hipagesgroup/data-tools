"""
Setup and release this package for wider consumption using setup tools
"""
import os

from setuptools import setup, find_packages


def get_long_description():
    """
    Get the contents of reame file as long_description
    Returns: bytes containing readme file

    """
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'README.md'),
              encoding='utf8') as readme_file:
        return readme_file.read()


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
    pbr=True
)
