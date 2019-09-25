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

  $ python version_tracking bruce ./version.json

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
"""

import argparse
import importlib
import json
import os
import re
import sys
from pathlib import Path

import git

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
    find the location on disc of the package of interest
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


def find_all_modules_which_require_version_tracking(file_list):
    """
    Iterates through a list of python files, and returns the names of classes,
    methods and functions and their file locations, which require a version
    to be found
    Args:
        file_list: list of paths to python files

    Returns:
            defitions_with_tags : list of class names which require version
            tracking
            files_with_tag: list of files in which the version tracked files
            are found

    """
    files_with_tag = []
    defitions_with_tags = []

    for path in file_list:
        defs_with_tags_in_file = \
            find_any_relevant_decorations_in_file(defitions_with_tags)

        defitions_with_tags = defitions_with_tags + defs_with_tags_in_file

        if defs_with_tags_in_file:
            files_with_tag.extend(
                [path for _ in defs_with_tags_in_file]
            )

    return defitions_with_tags, files_with_tag


def find_any_relevant_decorations_in_file(path):
    """
    Find any relevant decorations in the provided in tehe
    Args:
        path (string): Path to the file being scanned

    Returns (list(string)): List of decorated definitions found in the file

    """
    classes_in_file_with_decorator = []
    definitions_with_tags = []
    for declaration, decorating_string in DEFINITION_MAPPING.items():
        classes_in_file_with_decorator.extend(
            check_for_decorated_declaration_in_file(
                path,
                decorating_string,
                declaration))
    definitions_with_tags.extend(classes_in_file_with_decorator)

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

    with open(path, 'r') as file:
        lines = file.readlines()
        tagged_delcarations = _find_declarations_in_lines(declaration,
                                                          decorating_string,
                                                          lines)

    return tagged_delcarations


def _find_declarations_in_lines(declaration, decorating_string, lines):
    next_line = False
    tagged_delcarations = []
    for line in lines:
        if next_line and line.strip().startswith(declaration):
            tagged_delcarations.append(_extract_declaration_name(
                declaration, line))
            next_line = False
        if re.search(decorating_string, line):
            next_line = True

    return tagged_delcarations

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


def find_relevant_file_versions(pkg, repo_location):
    """
    Finds all the classes, and versions, for classes with the relevant
    decorator given the package and the repo location
    Args:
        pkg: package to analyse
        repo_location: path to relevant git repo

    Returns: dictionary of classes and the latest git hash of the
        file in which they live

    """
    package_location = _find_package_location(pkg)
    module_locations = _find_all_python_modules_in_folder(package_location)
    classes_with_tag, files_with_tag = \
        find_all_modules_which_require_version_tracking(module_locations)
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


def find_and_export_relevant_versions(pkg,
                                      repo_location,
                                      output_location):
    """
    Looks through a package and finds classes which have been decorated
    as requiring version tagging, and then writes these classes and their
    versions to a json file
    Args:
        pkg: package to analyse
        repo_location: location of github repo used to track versions
        output_location: location of version control json report

    """

    versions_dict = find_relevant_file_versions(pkg, repo_location)

    write_versions_to_json(versions_dict, output_location)


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
    sys.path.append(args.pkg_dir)
    pkg_of_interest = importlib.import_module(args.pkg_nm)

    find_and_export_relevant_versions(pkg_of_interest, args.repo, args.out_loc)


if __name__ == "__main__":
    main()
