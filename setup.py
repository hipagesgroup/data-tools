from setuptools import setup, find_packages

setup(
    name='data_tools',
    author='hipages Data Science Team',
    author_email='datascience@hipagesgroup.com.au',
    version='1.0',
    description='Common utility functions for data engineering usecases',
    url='https://github.com/hipagesgroup/data-tools',
    packages=find_packages(include=["data_tools", "data_tools.*"]),
    zip_safe=False,
    install_requires=[
        "boto3==1.9.206"
    ],
    test_suite="tests",
    tests_require=[

    ],
    python_requires='~=3.6',
)
