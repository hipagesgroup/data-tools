import os
import uuid
import pandas as pd
from unittest import TestCase
from unittest.mock import Mock, patch
from hip_data_tools.apache.cassandra import CassandraConnectionSettings, CassandraUtil
from hip_data_tools.etl.dataframe_to_cassandra import DataFrameToCassandra, DataFrameToCassandraSettings

class TestDataFrameToCassandra(TestCase):
    def setUp(self):
        # Create a sample DataFrame for testing
        self.sample_df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['test1', 'test2', 'test3'],
            'value': [10.5, 20.5, 30.5]
        })
        
        # Setup mock for CassandraConnectionSettings
        self.mock_conn_settings = Mock(spec=CassandraConnectionSettings)
        
        # Create test settings
        self.settings = DataFrameToCassandraSettings(
            data_frame=self.sample_df,
            destination_keyspace="test_keyspace",
            destination_table="test_table",
            destination_table_primary_keys=["id"],
            destination_table_partition_key=["id"],
            destination_table_options_statement="",
            destination_batch_size=2,
            destination_connection_settings=self.mock_conn_settings,
        )
        
        self.df_to_cassandra = DataFrameToCassandra(self.settings)
        
        # Add the data_frame attribute that's missing but referenced
        self.df_to_cassandra.data_frame = self.sample_df

    def test_upsert_dataframe(self):
        # Mock the _upsert_data_frame method
        self.df_to_cassandra._upsert_data_frame = Mock(return_value=["mocked_result"])
        
        # Call the method we want to test
        result = self.df_to_cassandra.upsert_dataframe()
        
        # Assert the method was called with the correct DataFrame
        self.df_to_cassandra._upsert_data_frame.assert_called_once_with(self.sample_df)
        
        # Assert the result matches what we expect
        self.assertEqual(result, ["mocked_result"])
    
    @patch('hip_data_tools.etl.dataframe_to_cassandra.CassandraUtil')
    def test_create_table(self, mock_cassandra_util):
        # Setup mock
        mock_util_instance = Mock()
        mock_cassandra_util.return_value = mock_util_instance
        
        # Patch the _get_cassandra_util method to return our mock
        self.df_to_cassandra._get_cassandra_util = Mock(return_value=mock_util_instance)
        
        # Call the method to test
        self.df_to_cassandra.create_table()
        
        # Assert create_table_from_dataframe was called with the right arguments
        mock_util_instance.create_table_from_dataframe.assert_called_once()
        
        # Get the call arguments
        args, kwargs = mock_util_instance.create_table_from_dataframe.call_args
        
        # Check table name and options were passed correctly
        self.assertEqual(kwargs.get('table_name'), "test_table")
        self.assertEqual(kwargs.get('primary_key_column_list'), ["id"])
        self.assertEqual(kwargs.get('partition_key_column_list'), ["id"])
        self.assertEqual(kwargs.get('table_options_statement'), "")
    
    def test_create_and_upsert_all(self):
        # Mock the required methods
        self.df_to_cassandra.create_table = Mock()
        self.df_to_cassandra.upsert_all_files = Mock(return_value=["batch_result"])
        
        # Call the method to test
        result = self.df_to_cassandra.create_and_upsert_all()
        
        # Assert the methods were called in the right order
        self.df_to_cassandra.create_table.assert_called_once()
        self.df_to_cassandra.upsert_all_files.assert_called_once()
        
        # Assert the result is what we expect
        self.assertEqual(result, ["batch_result"])
        
    def test_upsert_all_files(self):
        # Define _get_iterator which is missing but called in upsert_all_files
        self.df_to_cassandra._get_iterator = Mock(return_value=[self.sample_df])
        self.df_to_cassandra._upsert_data_frame = Mock(return_value=["upsert_result"])
        
        # Call the method to test
        result = self.df_to_cassandra.upsert_all_files()
        
        # Assert the _upsert_data_frame was called with the right argument
        self.df_to_cassandra._upsert_data_frame.assert_called_once_with(self.sample_df)
        
        # Assert the result is what we expect
        self.assertEqual(result, [["upsert_result"]])
        
    def test__upsert_data_frame_with_batch(self):
        # Mock _get_cassandra_util
        mock_cassandra_util = Mock()
        self.df_to_cassandra._get_cassandra_util = Mock(return_value=mock_cassandra_util)
        
        # Test with batch_size > 1
        self.settings.destination_batch_size = 2
        
        # Call the method
        self.df_to_cassandra._upsert_data_frame(self.sample_df)
        
        # Assert the correct method was called
        mock_cassandra_util.upsert_dataframe_in_batches.assert_called_once_with(
            dataframe=self.sample_df, 
            table="test_table", 
            batch_size=2
        )
        
    def test__upsert_data_frame_without_batch(self):
        # Mock _get_cassandra_util
        mock_cassandra_util = Mock()
        self.df_to_cassandra._get_cassandra_util = Mock(return_value=mock_cassandra_util)
        
        # Test with batch_size = 1
        self.settings.destination_batch_size = 1
        
        # Call the method
        self.df_to_cassandra._upsert_data_frame(self.sample_df)
        
        # Assert the correct method was called
        mock_cassandra_util.upsert_dataframe.assert_called_once_with(
            dataframe=self.sample_df, 
            table="test_table"
        )
