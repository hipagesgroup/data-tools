# hip-data-tools
© Hipages Group Pty Ltd 2019

[![PyPI version](https://badge.fury.io/py/hip-data-tools.svg)](https://pypi.org/project/hip-data-tools/#history) 
[![CircleCI](https://circleci.com/gh/hipagesgroup/data-tools/tree/master.svg?style=svg)](https://circleci.com/gh/hipagesgroup/data-tools/tree/master)

Common Python tools and utilities for data engineering, ETL, Exploration, etc. 
The package is uploaded to PyPi for easy drop and use in various environmnets, such as (but not limited to):

1. Running production workloads
2. ML Training in Jupyter like notebooks
3. Local machine for dev and exploration

 
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
from hip_data_tools.aws.common import AwsConnectionManager, AwsConnectionSettings, AwsSecretsManager

# to connect using an aws cli profile
conn = AwsConnectionManager(AwsConnectionSettings(region="ap-southeast-2", secrets_manager=None, profile="default"))

# OR if you want to connect using the standard aws environment variables
conn = AwsConnectionManager(settings=AwsConnectionSettings(region="ap-southeast-2", secrets_manager=AwsSecretsManager(), profile=None))

# OR if you want custom set of env vars to connect
conn = AwsConnectionManager(
    settings=AwsConnectionSettings(
        region="ap-southeast-2",
        secrets_manager=AwsSecretsManager(
            access_key_id_var="SOME_CUSTOM_AWS_ACCESS_KEY_ID",
            secret_access_key_var="SOME_CUSTOM_AWS_SECRET_ACCESS_KEY",
            use_session_token=True,
            aws_session_token_var="SOME_CUSTOM_AWS_SESSION_TOKEN"
            ),
        profile=None,
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

## Connect to Cassandra

 ```python
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.cqlengine import columns
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.models import Model

load_balancing_policy = DCAwareRoundRobinPolicy(local_dc='AWS_VPC_AP_SOUTHEAST_2')

conn = CassandraConnectionManager(
    settings = CassandraConnectionSettings(
        cluster_ips=["1.1.1.1", "2.2.2.2"],
        port=9042,
        load_balancing_policy=load_balancing_policy,
    )
)

conn = CassandraConnectionManager(
    CassandraConnectionSettings(
        cluster_ips=["1.1.1.1", "2.2.2.2"],
        port=9042,
        load_balancing_policy=load_balancing_policy,
        secrets_manager=CassandraSecretsManager(
        username_var="MY_CUSTOM_USERNAME_ENV_VAR"),
    )
)

# For running Cassandra model operations
conn.setup_connection("dev_space")
class ExampleModel(Model):
    example_type    = columns.Integer(primary_key=True)
    created_at      = columns.DateTime()
    description     = columns.Text(required=False)
sync_table(ExampleModel)
```

## Connect to Google Sheets

You can use load_sheet_to_athena() function to load Google sheet data into an Athena table.

```python
GoogleSheetToAthena(GoogleSheetsToAthenaSettings(
        source_workbook_url='https://docs.google.com/spreadsheets/d/cKyrzCBLfsQM/edit?usp=sharing',
        source_sheet='spec_example',
        source_row_range=None,
        source_fields=None,
        source_field_names_row_number=5,
        source_field_types_row_number=4,
        source_data_start_row_number=6,
        source_connection_settings=get_google_connection_settings(gcp_conn_id=GCP_CONN_ID),
        manual_partition_key_value={"column": "start_date", "value": START_DATE},
        target_database=athena_util.database,
        target_table_name=TABLE_NAME,
        target_s3_bucket=s3_util.bucket,
        target_s3_dir=s3_dir,
        target_connection_settings=get_aws_connection_settings(aws_conn_id=AWS_CONN_ID),
        target_table_ddl_progress=False
    )).load_sheet_to_athena()
```

There is an integration test called "integration_test_should__load_sheet_to_athena__when_using_sheetUtil" to test this functionality. You can simply run it by removing the "integration_" prefix.