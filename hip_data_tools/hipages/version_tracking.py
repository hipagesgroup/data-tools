"""
This module helps us version track classes by capturing the
latest git hash of the file they are written in, and dumping these versions to
a json file

The application looks through a package for classes with the registration
decorator. The last git commit has for files which contain this decorator are
then used to track the current version of the class

Example:
  The application takes two arguments the name of the package you're interested
  in, and the output location for the json file

  $ python version-tracker -p transformers -o ./version.json

It also provides transparent functions which can be used as decorators to
classes, functions and methods to tag them as requiring tracking by the
version control app

Typical usage:

from version_tracking import register_class_for_version_tracking
from version_tracking import register_method_for_version_tracking

@register_class_for_version_tracking
class Foo:
    def __init__(self):
        self.foo = 'some value'


    @register_method_for_version_tracking
    def track_this_method(self):
        return self.foo

When the version tracking app is executed the latest githash of the files
containing registered classes, functions, and methods will be logged to disk
in a json file
"""

import argparse
import json
import os
import socket
import traceback
from datetime import datetime
from pathlib import Path

import git
import joblib

from hip_data_tools.common import LOG as log

CLASS_DECORATOR_STRING = '@register_class_for_version_tracking'
"string : Used to trigger identification of the class by the version tracker"

METHOD_DECORATOR_STRING = '@register_method_for_version_tracking'
"string : Used to trigger identification of the method by the version tracker"

DEFINITION_MAPPING = {
    'class': CLASS_DECORATOR_STRING,
    'def': METHOD_DECORATOR_STRING
}
"dictionary: Used to map between the type and their respective decorator"


def register_class_for_version_tracking(cls):
    """
    Transparent decorator class used for registering a class with version
    monitoring.

    Args:
        cls: class to decorate

    Returns: decorated class

    """
    return cls


def register_method_for_version_tracking(meth):
    """
    Transparent decorator method used for registering a class with version
    monitoring.

    Args:
        meth: class to decorate

    Returns: decorated class

    """
    return meth


def _find_package_location(pkg):
    """
    find the location on disk of the package of interest
    Args:
        pkg: package of interest

    Returns: string, folder location of the package

    """
    loc = _get_package_location(pkg)
    return loc.split("__init__.py")[0]


def _get_package_location(pkg):
    """
    Finds the location of the __init__.py file of the package
    Args:
        pkg: package of interest

    Returns: string, the location of the __init__.py file of the package

    """
    return pkg.__file__


def _find_all_python_modules_in_folder(fld):
    """
    Finds all python modules within a given folder and its subfolders
    Args:
        fld: folder of interest

    Returns: list of paths to the python files

    """
    return list(Path(fld).rglob("*.py"))


def _instantiate_repo(repo_path):
    """
    Instaniates a git repo job at the the given path
    Args:
        repo_path: absolute or relative path the github repo

    Returns: github repo object

    """
    return git.Repo(repo_path)


def find_tracked_modules(file_list):
    """
    Iterates through a list of python files, and returns the names of classes,
    methods and functions and their file locations, which require a version
    to be found
    Args:
        file_list: list of paths to python files

    Returns:
            definitions_with_tags: list of class names which require version
            tracking
            files_with_tag: list of files in which the version tracked files
                are found

    """
    files_with_tag = []
    definitions_with_tags = []

    for path in file_list:
        defs_with_tags_in_file = \
            find_any_relevant_decorations_in_file(path)

        definitions_with_tags = definitions_with_tags + defs_with_tags_in_file

        if defs_with_tags_in_file:
            files_with_tag.extend(
                [path for _ in defs_with_tags_in_file]
            )

    return definitions_with_tags, files_with_tag


def find_any_relevant_decorations_in_file(path):
    """
    Find any relevant decorations in the provided in tehe
    Args:
        path (string): Path to the file being scanned

    Returns (list(string)): List of decorated definitions found in the file

    """

    definitions_with_tags = []

    for declaration, decorating_string in DEFINITION_MAPPING.items():
        definitions_with_tags.extend(
            check_for_decorated_declaration_in_file(
                path,
                decorating_string,
                declaration))

    return definitions_with_tags


def check_for_decorated_declaration_in_file(path,
                                            decorating_string,
                                            declaration):
    """
    Finds any classes in a file with the relevant decorator string
    Args:
        path: absolute or relative path to the python file
        decorating_string (string) : string of decorator used to identify the
             methods, classes and functions which need tracked
        declaration (string): Declaration to look for
    Returns: list of classes within the file which require version tracking

    """

    lines_in_file = _load_file_lines_into_list(path)

    lines_with_decorator = _find_lines_with_decorator(decorating_string,
                                                      lines_in_file)

    tagged_declarations = _find_decorated_declarations(declaration,
                                                       lines_in_file,
                                                       lines_with_decorator)

    return tagged_declarations


def _find_decorated_declarations(declaration,
                                 lines_in_file,
                                 lines_with_decorator):
    tagged_declarations = []
    line_limit = len(lines_in_file) - 1
    for decorator_indicies in lines_with_decorator:
        tagged_declarations.extend(
            _check_lines_after_declaration(declaration,
                                           decorator_indicies,
                                           lines_in_file,
                                           line_limit))

    return tagged_declarations


def _check_lines_after_declaration(declaration,
                                   decorator_indicies,
                                   lines_in_file,
                                   line_limit):
    tagged_declarations = []
    for cur_line_index in range(decorator_indicies, line_limit + 1):
        cur_line = lines_in_file[cur_line_index]
        found_value = _check_line_for_declaration(cur_line,
                                                  declaration,
                                                  cur_line_index >= line_limit,
                                                  decorator_indicies)

        if found_value is not None:
            tagged_declarations.append(found_value)
            break

    return tagged_declarations


def _check_line_for_declaration(cur_line,
                                declaration,
                                end_of_file,
                                decorator_indicies):
    declaration_in_line = cur_line.startswith(declaration + " ")
    # Ignore the line if its empty string indicating whitespace,
    # or if its another decorator
    # and stop if its the end of the file
    if (not cur_line or not cur_line.startswith("@")) \
        and end_of_file and not declaration_in_line:
        raise DecoratorError(declaration, cur_line, decorator_indicies)

    out = None
    if declaration_in_line:
        out = _extract_declaration_name(declaration, cur_line)

    return out


def _find_lines_with_decorator(decorating_string, lines_in_file):
    # find index of lines with decorator
    lines_with_decorator = []
    for idx, line in enumerate(lines_in_file):
        if line.startswith(decorating_string):
            lines_with_decorator.append(idx)
    return lines_with_decorator


def _load_file_lines_into_list(path):
    with open(path, 'r') as file:
        lines_in_file = [line.strip() for line in file.readlines()]
    return lines_in_file


def _extract_declaration_name(declaration, line):
    declaration_name = line.strip().split(declaration)[1] \
        .split("(")[0] \
        .replace(":", "") \
        .strip()
    return declaration_name


def get_latest_git_hash_of_files_in_repo(repo, files_to_get):
    """
    Iterates through a files and gets their latest commit hash using the repo
    object to interface with git
    Args:
        repo: git Repo object
        files_to_get: list of absolute or relative paths of files which
            require the git hashes

    Returns: list of git hashes for the file paths supplied

    """
    git_hashes = []

    for path in files_to_get:
        commit = next(repo.iter_commits(paths=path, max_count=1))
        git_hashes.append(commit.hexsha)

    return git_hashes


def find_relevant_file_versions(package_location, repo_location):
    """
    Finds all the classes, and versions, for classes with the relevant
    decorator given the package and the repo location
    Args:
        package_location (str): path to package
        repo_location (str): path to relevant git repo

    Returns: dictionary of classes and the latest git hash of the
        file in which they live

    """

    module_locations = _find_all_python_modules_in_folder(package_location)
    classes_with_tag, files_with_tag = \
        find_tracked_modules(module_locations)
    repo = _instantiate_repo(repo_location)
    git_hashes = get_latest_git_hash_of_files_in_repo(repo, files_with_tag)

    version_dict = \
        {class_name: git_hash
         for class_name, git_hash in zip(classes_with_tag, git_hashes)}
    return version_dict


def write_versions_to_json(version_dict, output_location):
    """
    Creates a json of the versions dictionary and writes it to file
    Args:
        version_dict: version dictionary of classes and the latest git
        commit hash of the file in which they live
        output_location: location to write output json file to

    """
    with open(output_location, 'w') as file:
        json.dump(version_dict, file)


def find_and_export_relevant_versions(path,
                                      repo_location,
                                      output_location):
    """
    Looks through a package and finds classes which have been decorated
    as requiring version tagging, and then writes these classes and their
    versions to a json file
    Args:
        path: local path to package
        repo_location: location of github repo used to track versions
        output_location: location of version control json report

    """

    versions_dict = find_relevant_file_versions(path, repo_location)

    write_versions_to_json(versions_dict, output_location)


class DecoratorError(Exception):
    """
    Error raised when there is a problem with the decoration of a class or
    method

    Args:
        declaration (str): Declartion the application is looking for
        line (str): Line where the declaration is made
        line_number (int): Line number in found of declaration

    """

    def __init__(self, declaration, line, line_number):
        # Call the base class constructor with the parameters it needs
        message_body = "Decorator Found, but no declaration discovered. " \
                       "Declaration {}, at line {}, line number {}" \
            .format(declaration, line, str(line_number))

        super().__init__(message_body)


class VersionTracker:
    """
    A class that allows us to track versions through hashes and strings by
    generating a dictionary. Versions can be loaded from file or can be
    generated on the fly from object hashes

    """

    def __init__(self):

        self._version_dict = {}

    def add_dictionary_to_versions(self, in_dict):
        """
        Add a Python dictionary to the versioning dictionary
        Args:
            in_dict (dict): A dictionary to append to the version dictionary

        Returns: None

        """

        self._version_dict.update(in_dict)

    def add_versions_from_json_file(self, version_file_location):
        """
        Load a json file from local storage and append these versions to the
        version tracking dictionary
        Args:
            version_file_location (str): Path to the json file

        Returns: None

        """
        try:
            with open(version_file_location, "r") as content:
                file_content = content.read()
                version_dict_from_file = json.loads(file_content)

                self.add_dictionary_to_versions(version_dict_from_file)

        except FileNotFoundError as fnf:
            log.error("No versioning file found at : %s",
                      version_file_location)
            raise fnf

        except json.decoder.JSONDecodeError as decode_error:

            log.error("JSON file failed to decode, check version dict is "
                      "correctly formatted")

            raise decode_error

        except Exception as exception:
            log.error("unknown error")
            log.error(traceback.format_exc())
            raise exception

    def _add_hostname(self):

        self._version_dict.update({'hostname': socket.gethostname()})

    def add_object_version(self, dict_key, obj):
        """Adds a hash of an object to the version tracking
        Args:
            dict_key (str): Name to be using in the version tracking
                dictioanry
            obj (Object): Any hashable object

        Returns: None
        """

        self.add_dictionary_to_versions({dict_key: joblib.hash(obj)})

    def add_string_to_version_tracking(self, dict_key, in_string):
        """
        Adds a string value to the version tracking.
        Args:
            dict_key (str): Name to use as the key in the version tracking
                dictionary
            in_string (str): Value to be stored in the version tracking
                dictionary

        Returns: None

        """

        self.add_dictionary_to_versions({dict_key: in_string})

    def _set_agglomerated_version_hash(self):
        """
        This hash takes all of the versioning hashes and combines them to
        produce a single hash of everything excluding the timestamp and
        hostname. This should, therefore, provide a single hash which
        encapsulates the software versions.

        Returns: None

        """
        relevant_versions = {x: self._version_dict[x]
                             for x in self._version_dict if x not in
                             ['hostname', 'versioning_timestamp']}

        version_hash = joblib.hash(json.dumps(relevant_versions,
                                              sort_keys=True)
                                   )

        self.add_dictionary_to_versions({'aggregated_version': version_hash})

    def _add_versioning_timestamp(self):
        self.add_dictionary_to_versions({'versioning_timestamp':
                                             datetime.utcnow().isoformat()})

    def get_version_dict(self):
        """
        Brings together all of the required versioning information and
        returns the relevant dictionary
        Returns (dict): Dictionary which tracks all of the tracked versions
        """

        self._set_agglomerated_version_hash()
        self._add_hostname()
        self._add_versioning_timestamp()

        return self._version_dict


def main():
    """
    Main function which takes command line arguments
        and executes the version finder
    """
    parser = argparse.ArgumentParser()

    parser.add_argument("-p",
                        "--package",
                        dest='pkg_nm',
                        required=True,
                        metavar='str',
                        help="package to be inspected")

    parser.add_argument("-r",
                        "--repo",
                        dest='repo',
                        required=False,
                        default=".",
                        metavar='str',
                        help="location of git repo")

    parser.add_argument("-o",
                        "--output-location",
                        dest='out_loc',
                        required=False,
                        default="version_tracking.json",
                        metavar='str',
                        help="location to put versioning file")

    parser.add_argument("-d",
                        "--pacakge-directory",
                        dest='pkg_dir',
                        required=False,
                        default=os.getcwd(),
                        metavar='str',
                        help="location to put versioning file")

    args = parser.parse_args()

    find_and_export_relevant_versions(args.pkg_nm, args.repo, args.out_loc)


if __name__ == "__main__":
    main()
