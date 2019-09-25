from unittest.mock import mock_open, patch

import pytest
from joblib import hash

import hip_data_tools.hipages.version_tracking as vt


class FakePackage:

    @staticmethod
    def some_method():
        return 'foo'


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


def test_finding_decorators_in_a_file():
    lines_in_file = ['some_line', '', vt.CLASS_DECORATOR_STRING,
                     "class "
                     "DecoratedClass"]


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


def test__check_for_all_decorated_methods_and_classes_in_file(mocker):
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
