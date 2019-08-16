# hip-data-tools
© Hipages Group Pty Ltd 2019

[![PyPI version](https://badge.fury.io/py/hip-data-tools.svg)](https://pypi.org/project/hip-data-tools/#history) 
[![CircleCI](https://circleci.com/gh/hipagesgroup/data-tools/tree/master.svg?style=svg)](https://circleci.com/gh/hipagesgroup/data-tools/tree/master)

Common Python tools and utilities for data engineering, ETL, Exploration, etc. 
The package is uploaded to PyPi for easy drop and use in various environmnets, such as (but not limited to):

1. Running production workloads
1. ML Training in Jupyter like notebooks
1. Local machine for dev and exploration

 
## Installation
Install from PyPi repo:
```bash
pip3 install hip-data-tools
```

Install from source
```bash
pip3 install .
```

## Connect to AWS 

You will need to instantiate an AWS Connection:
```python
from hip_data_tools.authenticate import AwsConnection

conn = AwsConnection(mode="assume_role", settings={"profile_name": "default"})

# OR if you want to connect using Env Vars:
conn = AwsConnection(mode="standard_env_var", settings={})

# OR if you want custom set of env vars to connect
conn = AwsConnection(mode="custom_env_var", settings={
     "aws_access_key_id_env_var": "aws_access_key_id",
     "aws_secret_access_key_env_var": "aws_secret_access_key"
 })

```

Using this connection to object you can use the aws utilities, for example aws Athena:
```python
from hip_data_tools.aws.athena import AthenaUtil

au = AthenaUtil(database="default", conn=conn, output_bucket="example", output_key="tmp/scratch/")
result = au.run_query("SELECT * FROM temp limit 10", return_result=True)
print(result)
```
