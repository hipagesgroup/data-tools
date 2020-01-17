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

## Connect to aws 

You will need to instantiate an AWS Connection:
```python
from hip_data_tools.connect.aws import AwsConnectionManager, AwsConnectionSettings

# to connect using an aws cli profile
conn = AwsConnectionManager(AwsConnectionSettings(region_name="ap-southeast-2", profile="default"))

# OR if you want to connect using the standard aws environment variables
# (aws_access_key_id, aws_secret_access_key):
conn = AwsConnectionManager(settings=AwsConnectionSettings(region_name="ap-southeast-2"))

# OR if you want custom set of env vars to connect
conn = AwsConnectionManager(
    settings=AwsConnectionSettings(
        region_name="ap-southeast-2",
        secrets_manager=AwsSecretsManager(
            access_key_id_var="SOME_CUSTOM_AWS_ACCESS_KEY_ID",
            secret_access_key_var="SOME_CUSTOM_AWS_SECRET_ACCESS_KEY",
            use_session_token=True,
            aws_session_token_var="SOME_CUSTOM_AWS_SESSION_TOKEN"
            )
        )
    )

```

Using this connection to object you can use the aws utilities, for example aws Athena:
```python
from hip_data_tools.aws.athena import AthenaUtil

au = AthenaUtil(database="default", conn=conn, output_bucket="example", output_key="tmp/scratch/")
result = au.run_query("SELECT * FROM temp limit 10", return_result=True)
print(result)
```

