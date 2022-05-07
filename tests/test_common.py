import datetime
import pandas as pd
from unittest import TestCase
from pandas._libs.tslibs.timestamps import Timestamp
from pandas._testing import assert_frame_equal
from hip_data_tools.common import flatten_nested_dict, \
    to_snake_case, nested_list_of_dict_to_dataframe, validate_and_fix_common_integer_fields


class TestCommon(TestCase):
    def test__should__flatten_dict__with_one_level_of_nesting(self):
        testinput = {
            "abc": 123,
            "def": "qwe",
            "foo": {
                "bar": "baz"
            }
        }
        testexpected = {
            "abc": 123,
            "def": "qwe",
            "foo_bar": "baz"
        }
        assert testexpected == flatten_nested_dict(testinput, "_")

    def test__should__flatten_dict__with_two_levels_of_nesting(self):
        testinput = {
            "abc": 123,
            "def": "qwe",
            "foo": {
                "bar": {
                    "baz": "boo"
                }
            }
        }
        testexpected = {
            "abc": 123,
            "def": "qwe",
            "foo_bar_baz": "boo"
        }
        assert testexpected == flatten_nested_dict(testinput)

    def test__should__flatten_dict__with_duplicate_keys(self):
        self.test__convert_to_snake_case_dict(["foo_bar", "baz"])

    def test__should__flatten_dict__with_camel_case(self):
        self.test__convert_to_snake_case_dict(["fooBar", "Baz"])

    @staticmethod
    def test__convert_to_snake_case_dict(arg):
        testinput = arg[1]
        testinput = {"abc": 123, "def": "qwe", "foo": {"bar": {"baz": "boo"}}, arg[0]: {testinput: "boo2"}}
        testexpected = {"abc": 123, "def": "qwe", "foo_bar_baz": "boo2"}
        assert testexpected == flatten_nested_dict(testinput)

    def test__should__convert_to_snake_case__with_camel_case(self):
        self.test__convert_to_snake_case_value(["ThisIsCamelCase", "this_is_camel_case"])

    def test__should__convert_to_snake_case__with_spaces(self):
        self.test__convert_to_snake_case_value(["ThisIs Camel Case", "this_is__camel__case"])

    def test__should__convert_to_snake_case__with_special_chars(self):
        self.test__convert_to_snake_case_value(["This%Is.Camel$%#@!^Case", "this__is__camel_______case"])

    def test__should__convert_to_snake_case__with_id(self):
        self.test__convert_to_snake_case_value(["ThisIsAnID", "this_is_an_i_d"])

    @staticmethod
    def test__convert_to_snake_case_value(arg):
        testinput = arg[0]
        assert arg[1] == to_snake_case(testinput)

    def test__should__convert_list_of_dict_to_proper_df__with__nested_items(self):
        testinput = [
            {
                "abc": 123,
                "def": "qwe",
                "fooBar": {
                    "Baz": "boo2"
                }
            },
            {
                "abc": 345,
                "def": "wer",
                "fooBar": {
                    "Baz": "bgt"
                }
            },
        ]
        testexpected = ['abc', 'def', 'foo_bar_baz']
        actual = nested_list_of_dict_to_dataframe(testinput)
        assert testexpected == list(actual.columns.values)

    def test__should__return_dataframe__when__a_list_of_dictionaries_with_complex_lists_is_given(self):
        testinput = [
            {
                'adGroupId': 94823864785,
                'labels': 'Hello_1',
                'tuple_field': ("field_1", "val_1"),
                'bool_field': True,
                'array_field': [["v1", "v2"]],
                'num_array_filed': [1, 3, 4, 5],
                'time_field': datetime.datetime(2020, 6, 18),
                'complex_field': []
            },
            {
                'adGroupId': 34523864785,
                'labels': 'Hello_2',
                'tuple_field': ("field_2", "val_2"),
                'bool_field': False,
                'array_field': [[["v1", "v2"], []]],
                'num_array_filed': [1, 3, 4, 5],
                'time_field': datetime.datetime(2020, 6, 18),
                'complex_field': [TestObject()]
            }
        ]
        testinput = nested_list_of_dict_to_dataframe(testinput)
        data = {'ad_group_id': {0: 94823864785, 1: 34523864785},
                'labels': {0: 'Hello_1', 1: 'Hello_2'},
                'tuple_field': {0: ('field_1', 'val_1'), 1: ('field_2', 'val_2')},
                'bool_field': {0: True, 1: False},
                'array_field': {0: ['["v1", "v2"]'], 1: ['[["v1", "v2"], []]']},
                'num_array_filed': {0: ['1', '3', '4', '5'], 1: ['1', '3', '4', '5']},
                'time_field': {
                    0: Timestamp('2020-06-18 00:00:00'),
                    1: Timestamp('2020-06-18 00:00:00')
                },
                'complex_field': {0: [], 1: ['{"x": "hello", "y": "world"}']}}
        assert_frame_equal(pd.DataFrame(data=data), testinput)

    def test__should__validate_and_fix_common_integer_fields(self):
        testinput = pd.DataFrame(data={
            "id": [None, "34523864785"],
            "country__territory": ["----", "3434"]
        })
        validate_and_fix_common_integer_fields(testinput)
        testexpected = pd.DataFrame(data={
            "id": [0, 34523864785],
            "country__territory": [0, 3434]
        })
        assert_frame_equal(testexpected, testinput)
class TestObject:
    def __init__(self):
        self.x = 'hello'
        self.y = 'world'
