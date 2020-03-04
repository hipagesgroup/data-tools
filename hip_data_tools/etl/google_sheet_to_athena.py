"""
Module to deal with data transfer from Google sheets to Athena
"""
import logging as log
import re

from attr import dataclass

from hip_data_tools.aws.athena import AthenaUtil
from hip_data_tools.aws.common import AwsConnectionManager
from hip_data_tools.aws.common import AwsConnectionSettings
from hip_data_tools.aws.s3 import S3Util
from hip_data_tools.google.common import GoogleApiConnectionSettings
from hip_data_tools.google.sheets.common import GoogleSheetConnectionManager
from hip_data_tools.google.sheets.sheets import SheetUtil

DTYPE_GOOGLE_SHEET_TO_PARQUET_ATHENA = {
    "NUMBER": "DOUBLE",
    "STRING": "STRING",
    "BOOLEAN": "BOOLEAN",
    "DATE": "STRING"
}


@dataclass
class GoogleSheetsToAthenaSettings:
    """
    Google sheets to Athena ETL settings
    Args:
        workbook_name: the name of the workbook (eg: Tradie Acquisition Targets)
            If there are multiple workbooks with the same name, the workbook which has most recently
            shared with the google service account is used
        sheet_name: name of the google sheet (eg: sheet1)
        row_range: range of rows (eg: '2:5')
        table_name: name of the athena table (eg: 'sheet_table')
        --------make this a list of dictionary
        fields: list of sheet field names and types. Field names cannot contain hyphens('-'), spaces
            and special characters
            (eg: ['name:string','age:number','is_member:boolean'])
            If this is None, the field names and types from google sheet are used automatically
        field_names_row_number: row number of the field names (eg: 4). Will be ignored if fields
            have been specified (see above). Assumes the data starts at first column and there is no
            gaps. There should not be 2 fields with the same name.
        field_types_row_number: row number of the field types (eg: 5). Will be ignored if fields
            have been specified (see above)
        use_derived_types: if this is false type of the fields are considered as strings
            irrespective of the provided field types (eg: True)
        s3_bucket: s3 bucket to store the files (eg: au-test-bucket)
        s3_dir: s3 directory to store the files (eg: sheets/new)
        ---- call it manual_partition_key_value
        partition_key: list of partitions (eg: [{"column": "view", "type": "string"}]. Only one
            partition key can be used
        partition_value: value of the partition key (eg: '2020-02-14')
        ------- call it data_start_row_number --- it should be 6
        skip_top_rows_count: number of top rows that need to be skipped (eg: 1)
        ------ this should be GoogleSheetsConnectionSettings
        keys_object: google api keys dictionary object
            (eg: {'type': 'service_account', 'project_id': 'hip-gandalf-sheets',...... })
        ------ target_database
        database: name of the athena database (eg: dev)
        ------- target_connection_settings
        connection_settings: aws connection settings
    """
    source_workbook_name: str
    source_sheet_name: str
    source_row_range: str
    target_table_name: str
    source_fields: list
    source_field_names_row_number: int
    source_field_types_row_number: int
    use_derived_types: bool
    s3_bucket: str
    s3_dir: str
    manual_partition_key_value: list
    data_start_row_number: int
    source_connection_settings: GoogleSheetConnectionSettings
    target_database: str
    target_connection_settings: AwsConnectionSettings


def _simplified_dtype(data_type):
    """
    Return the athena base data type
    Args:
        data_type (string): data type
    :return: simplified data type
    """
    return ((re.sub(r'\(.*\)', '', data_type)).split(" ", 1)[0]).upper()


class GoogleSheetToAthena:
    """
    Class to transfer data from google sheet to athena
    Args:
        settings (GoogleSheetsToAthenaSettings): the settings around the etl to be executed
    """

    def __init__(self, settings: GoogleSheetsToAthenaSettings):
        self.settings = settings
        self.keys_to_transfer = None

    @staticmethod
    def __get_columns(columns, field_names=None, field_types=None):
        for field_name, field_type in zip(field_names, field_types):
            columns.append({"column": field_name,
                            "type": DTYPE_GOOGLE_SHEET_TO_PARQUET_ATHENA.get(
                                str(_simplified_dtype(field_type)),
                                "STRING")})

# TODO
    def _get_sheets_util(self):
        return SheetUtil(conn_manager=GoogleSheetConnectionManager(
            GoogleApiConnectionSettings(keys_object=self.settings.source_connection_settings)),
                         field_names_row_number=self.settings.source_field_names_row_number,
                         field_types_row_number=self.settings.source_field_types_row_number)

    def _get_athena_util(self):
        return AthenaUtil(database=self.settings.target_database,
                          conn=AwsConnectionManager(settings=self.settings.target_connection_settings),
                          output_bucket=self.settings.s3_bucket)

    def _get_s3_util(self):
        return S3Util(
            bucket=self.settings.s3_bucket,
            conn=AwsConnectionManager(settings=self.settings.target_connection_settings))

    def _get_table_settings(self, field_names, field_types):
        """
        Get the table settings dictionary
        Returns: table settings dictionary

        """
        table_settings = {
            "table": self.settings.target_table_name,
            "exists": True,
            "partitions": [],
            "columns": [],
            "storage_format_selector": "parquet",
            "s3_bucket": self.settings.s3_bucket,
            "s3_dir": self.settings.s3_dir,
            "encryption": False
        }
        columns = []
        if self.settings.use_derived_types:
            self.__get_columns(columns, field_names, field_types)
        else:
            for field_name in field_names:
                columns.append({"column": field_name, "type": "string"})
        table_settings["columns"] = columns
        table_settings["partitions"] = self.settings.manual_partition_key_value

        return table_settings

    def _get_the_insert_query(self, values_matrix, types):
        """
        Get the insert query for the athena table using the values matrix
        Args:
            values_matrix (array): values of the google sheet
            types (array): data type of each column
        Returns: insert query for the athena table

        """
        if not values_matrix:
            return "INSERT INTO {table_name} VALUES ()".format(table_name=self.settings.target_table_name)
        insert_query = "INSERT INTO {table_name} VALUES ".format(
            table_name=self.settings.target_table_name)
        values = ""
        if self.settings.partition_value:
            partition_value_statement = ", '{}'".format(self.settings.partition_value)
            types += 'STRING'
        else:
            partition_value_statement = ''
        for value in values_matrix:
            values += "({}{}), ".format(', '.join(self.__get_the_values_list(types, value)),
                                        partition_value_statement)
        values = values[:-2]
        insert_query += values
        return insert_query

    @staticmethod
    def __get_the_values_list(types, value):
        # Return a list of values (Quotes are added if the value type is string or date)
        # eg: ['2020-03-02', 435, 54, 'customer_1', 34]
        return ["'{}'".format(
            val) if data_type.upper() == 'STRING' or data_type.upper() == 'DATE' else
                "{}".format(
                    val) for
                val, data_type
                in zip(value, types)]

    def load_sheet_to_athena(self, overwrite_table=False):
        """
        Method to load google sheet to athena
        Args:
            overwrite_table (boolean): if this is true, it drops the existing athena table and
                clear the s3 location
        :return: None
        """
        sheet_util = self._get_sheets_util()
        athena_util = self._get_athena_util()
        s3_util = self._get_s3_util()

        if overwrite_table:
            athena_util.drop_table(self.settings.target_table_name)
            s3_util.delete_recursive(self.settings.s3_dir)

        field_names = []
        field_types = []
        if self.settings.source_fields is None:
            field_names = sheet_util.get_fields_names(self.settings.source_workbook_name,
                                                      self.settings.source_sheet_name)
            field_types = sheet_util.get_fields_types(self.settings.source_workbook_name,
                                                      self.settings.source_sheet_name)
            self.__validate_field_names_and_types_count(field_names, field_types)
            self.__validate_field_names(field_names)
        else:
            for field in self.settings.source_fields:
                field_name_type = field.split(':')
                field_name = field_name_type[0]
                field_names.append(field_name)
                field_types.append(field_name_type[1])
                self.__validate_field_names([field_name])

        values_matrix = sheet_util \
            .get_value_matrix(workbook_name=self.settings.source_workbook_name,
                              sheet_name=self.settings.source_sheet_name,
                              row_range=self.settings.source_row_range,
                              skip_top_rows_count=self.settings.data_start_row_number)
        log.info("The value matrix:\n %s", values_matrix)
        table_settings = self._get_table_settings(field_names, field_types)
        athena_util.create_table(table_settings)
        insert_query = self._get_the_insert_query(values_matrix=values_matrix, types=field_types)
        log.info("The insert query:\n %s", insert_query)
        athena_util.run_query(query_string=insert_query)

    @staticmethod
    def __validate_field_names_and_types_count(field_names, field_types):
        if len(field_names) != len(field_types):
            log.error("Number of field names and number of field types are not matching")
            raise Exception("Field names and types are not matching")

    @staticmethod
    def __validate_field_names(field_names):
        for field_name in field_names:
            # check for strings which only contains letters, numbers and underscores
            if not re.match("^[A-Za-z0-9_]+$", field_name):
                log.error("Unsupported field name: %s", field_name)
                raise Exception("Unsupported field name")
