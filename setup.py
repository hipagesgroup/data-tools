"""
Setup and release this package for wider consumption using setup tools
"""

from setuptools import setup, find_packages

setup(
    long_description=open("README.md").read(),
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
