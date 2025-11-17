import json
import uuid
from unittest.mock import mock_open, patch
import pytest
from freezegun import freeze_time
from joblib import hash
## from mock import patch, mock_open

import hip_data_tools.hipages.version_tracking as vt


class FakePackage:

    @staticmethod
    def some_method():
        return 'foo'


class ExampleClass:

    def __init__(self, some_attribute):
        self.some_attribute = some_attribute


def test__decorate_class_should_decorate_class_with_no_sideeffects():
    stubbed_class = FakePackage()
    decorated_class = vt.register_class_for_version_tracking(stubbed_class)

    assert hash(stubbed_class) == hash(decorated_class)


def test__decorate_class_should_decorate_class_with_no_sideeffects():
    decorated_class = \
        vt.register_method_for_version_tracking(FakePackage.some_method)

    assert hash(FakePackage.some_method) == hash(decorated_class)


def test__python_can_extract_fld_from_pkg_file(mocker):
    mocker.patch.object(vt, '_get_package_location')
    vt._get_package_location.return_value = '/foo/bar/__init__.py'
    found_location = vt._find_package_location(FakePackage)
    assert (found_location == '/foo/bar/')


def test__check_for_decorated_classes_in_file(mocker):
    some_decorated_string = "@decorator_string"
    type_definiton = "class"
    example_file = """import foo
    import pandas as pd
    
    {}
    {} ClassToBeTracked
        def __init__(self): 
            self.foo = 1 
    
    {} ClassToBeIgnored    
    """.format(some_decorated_string, type_definiton, type_definiton)
    #
    m = mock_open(read_data=example_file)
    with patch('builtins.open', m, create=True):
        classes_with_tag = \
            vt.check_for_decorated_declaration_in_file('some/mocked/file/path',
                                                       some_decorated_string,
                                                       type_definiton)

    assert (len(classes_with_tag) == 1)


def test__check_for_file_with_no_decorarted_classes(mocker):
    mappings = {
        'class': '@register_class_for_version_tracking',
        'def': '@register_method_for_version_tracking'
    }

    example_file = """import foo
    import pandas as pd
    
    {} 
    {} ClassToBeTracked
        def __init__(self): 
            self.foo = 1 
        
        {}
        {} foo(self, int1):
            return int1
    

    """.format("", 'class', "", 'def')

    with patch("builtins.open", mock_open(read_data=example_file)) as mock_file:
        list_of_files_to_analyse = ['some_file/location/this.file']
        classes_with_tag, files_with_tag = \
            vt.find_tracked_modules(
                list_of_files_to_analyse)

    assert (len(classes_with_tag) == 0)
    assert (len(files_with_tag) == 0)


def test__exception_raised_when_decorator_found_but_no_defintion(mocker):
    mappings = {
        'class': '@register_class_for_version_tracking',
        'def': '@register_method_for_version_tracking'
    }

    example_file = """import foo
    import pandas as pd
    
    {} 
    {} ClassToBeTracked
        def __init__(self): 
            self.foo = 1 
        
        {}
        {} foo(self, int1):
            return int1
    

    """.format(mappings['class'], 'def', mappings['def'], 'class')

    with patch("builtins.open", mock_open(read_data=example_file)) as mock_file:
        with pytest.raises(vt.DecoratorError):
            list_of_files_to_analyse = ['some_file/location/this.file']
            classes_with_tag, files_with_tag = \
                vt.find_tracked_modules(
                    list_of_files_to_analyse)


def test__check_for_single_decorated_classes_in_file(mocker):
    some_decorated_string = "@decorator_string"
    type_definiton = "class"
    example_file = """import foo
    import pandas as pd
    
    {}
    {} ClassToBeTracked
        def __init__(self): 
            self.foo = 1 
    
    {} ClassToBeIgnored    
    """.format(some_decorated_string, type_definiton, type_definiton)
    #
    m = mock_open(read_data=example_file)
    with patch('builtins.open', m, create=True):
        classes_with_tag = \
            vt.check_for_decorated_declaration_in_file('some/mocked/file/path',
                                                       some_decorated_string,
                                                       type_definiton)

    assert (len(classes_with_tag) == 1)
    assert classes_with_tag[0] == 'ClassToBeTracked'


def test__check_for_all_decorated_classes_in_file(mocker):
    some_decorated_string = "@decorator_string"
    type_definiton = "class"
    example_file = """import foo
    import pandas as pd
    
    {}
    {} ClassToBeTracked
        def __init__(self): 
            self.foo = 1 
    
    {} ClassToBeIgnored
    
    {}
    {} AnotherClassToBeTracked
        def __init__(self): 
            self.foo = 1     
    """.format(some_decorated_string,
               type_definiton,
               type_definiton,
               some_decorated_string,
               type_definiton)
    #
    m = mock_open(read_data=example_file)
    with patch('builtins.open', m, create=True):
        classes_with_tag = \
            vt.check_for_decorated_declaration_in_file('some/mocked/file/path',
                                                       some_decorated_string,
                                                       type_definiton)

    assert (len(classes_with_tag) == 2)
    assert classes_with_tag[0] == 'ClassToBeTracked'
    assert classes_with_tag[1] == 'AnotherClassToBeTracked'


def test__get_latest_git_hash_of_files_in_repo(stub):
    commit_sha = 'someHexCommitString'

    class Commit:

        def __init__(self):

            self.counter = 0
            self.hexsha = commit_sha

        def __next__(self):

            if self.counter == 0:
                self.counter += 1
                return self
            else:
                raise StopIteration

        def __iter__(self):

            yield self

    class RepoStub:

        def iter_commits(self, paths, max_count):
            return Commit()

    stub.apply({
        'git.Repo': RepoStub
    })

    files_to_get = ['/some/files/file.py', '/some/other/file.py']

    git_hashes = vt.get_latest_git_hash_of_files_in_repo(RepoStub(),
                                                         files_to_get)

    assert ([commit_sha, commit_sha] == git_hashes)


def test__versiontracker_should_load_version_file():
    version_file = """{"some_class": "version_1"}"""

    with patch("builtins.open", mock_open(read_data=version_file)) as mock_file:
        version_tracker = \
            vt.VersionTracker()

        version_tracker.add_versions_from_json_file('some_location')

        assert version_tracker._version_dict == \
               {'some_class': 'version_1'}


def test__versiontracker_should_raise_error_when_file_not_found():
    random_file_location = str(uuid.uuid4())

    with pytest.raises(FileNotFoundError) as fnf:
        version_tracker = \
            vt.VersionTracker()

        version_tracker.add_versions_from_json_file(random_file_location)

    assert fnf.typename == "FileNotFoundError"


def test__versiontracker_should_raise_decode_error_when_problem_found():
    version_file = """not a json file"""

    with patch("builtins.open", mock_open(read_data=version_file)) as mock_file:
        with pytest.raises(json.decoder.JSONDecodeError) as decode_error:
            version_tracker = vt.VersionTracker()

            version_tracker.add_versions_from_json_file('some_file/location')

    assert decode_error.typename == "JSONDecodeError"


def test_versiontracker_should_add_a_consistent_object_version_for_tracking():
    example_class_1 = ExampleClass('attr1')
    example_class_2 = ExampleClass('attr2')

    expected_hash_value_1 = '7755191057dcd7367e90110af2f38378'
    expected_hash_value_2 = '27a50c050e78a07ff1ccd27e4dcab7b7'

    version_tracker = vt.VersionTracker()

    version_tracker.add_object_version('example_class_1', example_class_1)
    version_tracker.add_object_version('example_class_2', example_class_2)

    expected_dict = {'example_class_1': expected_hash_value_1,
                     'example_class_2': expected_hash_value_2}

    assert expected_dict == version_tracker._version_dict


def test__versiontracker_should_add_string_to_version_tracking():
    version_tracker = vt.VersionTracker()

    version_tracker.add_string_to_version_tracking("A_path_to_something",
                                                   'some/path/some/where')

    version_tracker.add_string_to_version_tracking("another_path_to_something",
                                                   'another/path/some/where')

    expected_dict = {"A_path_to_something": 'some/path/some/where',
                     "another_path_to_something": 'another/path/some/where'}

    assert version_tracker._version_dict == expected_dict


@freeze_time("2017-03-28 11:10:10")
def test_versiontracker_provide_all_the_versions(mocker):
    version_file = """{"some_class": "version_1"}"""

    with patch("builtins.open", mock_open(read_data=version_file)) as mock_file:
        with mocker.patch("socket.gethostname", return_value="a_host"):
            example_class_1 = ExampleClass('attr1')
            example_class_2 = ExampleClass('attr2')

            vtracker = vt.VersionTracker()

            vtracker.add_versions_from_json_file("some/location")
            vtracker.add_string_to_version_tracking("A_path_to_something",
                                                    'some/path/some/where')

            vtracker.add_string_to_version_tracking("another_path_to_something",
                                                    'another/path/some/where')

            vtracker.add_object_version('example_class_1', example_class_1)
            vtracker.add_object_version('example_class_2', example_class_2)

            version_dict = vtracker.get_version_dict()

            expected_dict = {'A_path_to_something': 'some/path/some/where',
                             'aggregated_version':
                                 '080d27b8c6e6f8975ea8d227742c22bd',
                             'another_path_to_something':
                                 'another/path/some/where',
                             'example_class_1':
                                 '7755191057dcd7367e90110af2f38378',
                             'example_class_2':
                                 '27a50c050e78a07ff1ccd27e4dcab7b7',
                             'hostname': 'a_host',
                             'some_class': 'version_1',
                             'versioning_timestamp': '2017-03-28T11:10:10'}

            assert expected_dict == version_dict
