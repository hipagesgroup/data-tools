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
        "boto3==1.9.216",
        "joblib==0.13.2",
        "pandas==0.25.1",
    ],
    test_suite="tests",
    tests_require=[
        "moto==1.3.13",
        "pyarrow==0.14.1",
        "python-snappy==0.5.4",
    ],
    python_requires='~=3.6',
    version=get_release_version(),
)
