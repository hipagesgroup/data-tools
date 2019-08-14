import os

from setuptools import setup, find_packages

git_version = os.getenv("GIT_TAG", "v0.0")
pypi_version = git_version.lstrip("v")

setup(
    name='hip_data_tools',
    author='hipages Data Science Team',
    author_email='datascience@hipagesgroup.com.au',
    version=pypi_version,
    description='Common utility functions for data engineering usecases',
    url='https://github.com/hipagesgroup/data-tools',
    packages=find_packages(include=["hip_data_tools", "hip_data_tools.*"]),
    zip_safe=False,
    install_requires=[
        "boto3==1.9.206"
    ],
    test_suite="tests",
    tests_require=[
    ],
    python_requires='~=3.6',
    license='osl-3.0',
)
