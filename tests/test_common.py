import datetime
import json
from unittest import TestCase

import pandas as pd
from pandas._testing import assert_frame_equal

from hip_data_tools.common import flatten_nested_dict, \
    to_snake_case, nested_list_of_dict_to_dataframe, validate_and_fix_common_integer_fields


class TestCommon(TestCase):
    # def test__get_release_version__parses_tag(self):
    #     os.environ["GIT_TAG"] = "v1.3"
    #     actual = get_release_version()
    #     expected = "1.3"
    #     self.assertEqual(actual, expected)
    #
    # def test__get_release_version__works_without_tag(self):
    #     del os.environ["GIT_TAG"]
    #     actual = get_release_version()
    #     expected = "0.0"
    #     self.assertEqual(actual, expected)
    #
    # def test__get_long_description__reads_readme(self):
    #     actual = get_long_description()[0:16]
    #     expected = "# hip-data-tools"
    #     self.assertEqual(actual, expected)

    def test__should__flatten_dict__with_one_level_of_nesting(self):
        input = {
            "abc": 123,
            "def": "qwe",
            "foo": {
                "bar": "baz"
            }
        }
        expected = {
            "abc": 123,
            "def": "qwe",
            "foo_bar": "baz"
        }
        actual = flatten_nested_dict(input, "_")
        self.assertDictEqual(expected, actual)

    def test__should__flatten_dict__with_two_levels_of_nesting(self):
        input = {

            "abc": 123,
            "def": "qwe",
            "foo": {
                "bar": {
                    "baz": "boo"
                }
            }
        }
        expected = {
            "abc": 123,
            "def": "qwe",
            "foo_bar_baz": "boo"
        }
        actual = flatten_nested_dict(input)
        self.assertDictEqual(expected, actual)

    def test__should__flatten_dict__with_duplicate_keys(self):
        input = {

            "abc": 123,
            "def": "qwe",
            "foo": {
                "bar": {
                    "baz": "boo"
                }
            },
            "foo_bar": {
                "baz": "boo2"
            }
        }
        expected = {
            "abc": 123,
            "def": "qwe",
            "foo_bar_baz": "boo2"
        }
        actual = flatten_nested_dict(input)
        self.assertDictEqual(expected, actual)

    def test__should__flatten_dict__with_camel_case(self):
        input = {

            "abc": 123,
            "def": "qwe",
            "foo": {
                "bar": {
                    "baz": "boo"
                }
            },
            "fooBar": {
                "Baz": "boo2"
            }
        }
        expected = {
            "abc": 123,
            "def": "qwe",
            "foo_bar_baz": "boo2"
        }
        actual = flatten_nested_dict(input)
        self.assertDictEqual(expected, actual)

    def test__should__convert_to_snake_case__with_camel_case(self):
        input = "ThisIsCamelCase"
        expected = "this_is_camel_case"
        actual = to_snake_case(input)
        self.assertEqual(expected, actual)

    def test__should__convert_to_snake_case__with_spaces(self):
        input = "ThisIs Camel Case"
        expected = "this_is__camel__case"
        actual = to_snake_case(input)
        self.assertEqual(expected, actual)

    def test__should__convert_to_snake_case__with_special_chars(self):
        input = "This%Is.Camel$%#@!^Case"
        expected = "this__is__camel_______case"
        actual = to_snake_case(input)
        self.assertEqual(expected, actual)

    def test__should__convert_to_snake_case__with_id(self):
        input = "ThisIsAnID"
        expected = "this_is_an_i_d"
        actual = to_snake_case(input)
        self.assertEqual(expected, actual)

    def test__should__convert_list_of_dict_to_proper_df__with__nested_items(self):
        input = [
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
        expected = ['abc', 'def', 'foo_bar_baz']
        actual = nested_list_of_dict_to_dataframe(input)
        self.assertListEqual(expected, list(actual.columns.values))

    def test__should__return_dataframe__when__a_list_of_dictionaries_with_complex_lists_is_given(
            self):
        input_value = [
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
        actual = nested_list_of_dict_to_dataframe(input_value)
        expected = pd.DataFrame(
            data={"ad_group_id": [94823864785, 34523864785], "labels": ["Hello_1", "Hello_2"],
                  "tuple_field": [("field_1", "val_1"), ("field_2", "val_2")],
                  "bool_field": [True, False],
                  "array_field": [[["v1", "v2"]], [[["v1", "v2"], []]]],
                  "num_array_filed": [[1, 3, 4, 5], [1, 3, 4, 5]],
                  "time_field": [datetime.datetime(2020, 6, 18), datetime.datetime(2020, 6, 18)],
                  "complex_field": [[], [TestObject()]]})
        expected["array_field"] = expected["array_field"].apply(lambda x: json.dumps(x))
        expected["num_array_filed"] = expected["num_array_filed"].apply(lambda x: json.dumps(x))
        expected["complex_field"] = expected["complex_field"].apply(
            lambda obj: json.dumps(obj, default=lambda x: x.__dict__))

        assert_frame_equal(expected, actual)

    def test__should__validate_and_fix_common_integer_fields(self):
        actual = pd.DataFrame(
            data={"id": [None, "34523864785"],
                  "country__territory": ["----", "3434"]})
        validate_and_fix_common_integer_fields(actual)
        expected = pd.DataFrame(
            data={"id": [0, 34523864785],
                  "country__territory": [0, 3434]})

        assert_frame_equal(expected, actual)


class TestObject:
    def __init__(self):
        self.x = 'hello'
        self.y = 'world'
