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
        "boto3==1.9.206",
        "joblib==0.13.2",
        "PyYAML==5.1.2",
    ],
    test_suite="tests",
    tests_require=[
        "docker==4.0.2",
        "moto==1.3.13",
    ],
    python_requires='~=3.6',
    version=get_release_version(),
)
