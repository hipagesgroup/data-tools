from hip_data_tools.aws.common import AwsConnectionSettings
from hip_data_tools.etl.athena_to_dataframe import AthenaToDataFrame,AthenaToDataFrameSettings

settings = AthenaToDataFrameSettings(
    source_database="common",
    source_table="muriel_queries",
    source_connection_settings=AwsConnectionSettings(
        region="ap-southeast-2",
        profile="default",
        secrets_manager=None,
    )
)

reader = AthenaToDataFrame(settings=settings)

df = reader.get_all_files_as_data_frame()

df.info()
