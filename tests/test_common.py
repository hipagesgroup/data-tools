from unittest import TestCase

from hip_data_tools.common import flatten_nested_dict, \
    to_snake_case, nested_list_of_dict_to_dataframe


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

    def test__should__convert_object_type_to_string_type__when__dataframe_is_given(self):
        input = [
            {
                "abc": 123,
                "def": "qwe",
                "fooBar": [1, 4]
            },
            {
                "abc": 345,
                "def": "wer",
                "fooBar": [1, 2]
            },
        ]
        expected = ['int64', 'string', 'object']
        result_df = nested_list_of_dict_to_dataframe(input)
        actual = [str(result_df[col].dtype) for col in list(result_df)]
        self.assertEqual(expected, actual)
