#!/usr/bin/env python

# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import argparse
import os
from subprocess import check_call
import glob

DEFAULT_DESTINATION_FOLDER = "./dist"
CURRENT_DIR = os.path.curdir


# build the wheels for all packages
def create_storage_package():
    check_call(['python3', 'tool_build_packages.py', 'all'])


# install dependencies required for testing into the virtual environment
def install_dependency_packages(executable_location):
    check_call([executable_location, 'install', '-r', 'requirements.txt'])
    check_call([executable_location, 'install', 'pytest'])


# install the storage packages into the virtual environment
def install_storage_package(executable_location, environment):
    if environment == 'test':
        check_call([executable_location, 'install', 'azure-storage-nspkg'])
        check_call([executable_location, 'install', 'azure-storage-common', '-i',
                    'https://testpypi.python.org/pypi', '--no-deps'])
        check_call([executable_location, 'install', 'azure-storage-blob', '-i',
                    'https://testpypi.python.org/pypi', '--no-deps'])
        check_call([executable_location, 'install', 'azure-storage-file', '-i',
                    'https://testpypi.python.org/pypi', '--no-deps'])
        check_call([executable_location, 'install', 'azure-storage-queue', '-i',
                    'https://testpypi.python.org/pypi', '--no-deps'])
    elif environment == 'prod':
        check_call([executable_location, 'install', 'azure-storage-blob', '--no-cache-dir'])
        check_call([executable_location, 'install', 'azure-storage-file', '--no-cache-dir'])
        check_call([executable_location, 'install', 'azure-storage-queue', '--no-cache-dir'])
    else:
        # install the namespace package first
        nspkg_wheel = glob.glob("dist/*nspkg*.whl")
        check_call([executable_location, 'install', os.path.abspath(nspkg_wheel[0])])

        # install the common package
        common_wheel = glob.glob("dist/*common*.whl")
        check_call([executable_location, 'install', os.path.abspath(common_wheel[0])])

        # install all the other packages
        # this simply skips the common and namespace package since they are already installed
        storage_wheels = glob.glob("dist/*.whl")
        for wheel in storage_wheels:
            check_call([executable_location, 'install', os.path.abspath(wheel)])


# clean up the test directory containing the virtual environment
def delete_directory_if_exists(dir_name):
    if os.path.exists(CURRENT_DIR + '/' + dir_name):
        check_call(['rm', '-r', dir_name])


# create virtual environment for python 2
def create_py2_venv(environment):
    dir_name = 'py2test-' + environment
    pip_location = dir_name + '/bin/pip'

    delete_directory_if_exists(dir_name)
    os.system('virtualenv ' + dir_name)  # this creates the virtual environment
    install_dependency_packages(pip_location)
    install_storage_package(pip_location, environment)
    return dir_name + '/bin/python'


# create virtual environment for python 3
def create_py3_venv(environment):
    dir_name = 'py3test-' + environment
    pip_location = dir_name + '/bin/pip'

    delete_directory_if_exists(dir_name)
    os.system('python3 -m venv ' + dir_name) # this creates the virtual environment
    install_dependency_packages(pip_location)
    install_storage_package(pip_location, environment)
    return dir_name + '/bin/python'


# kicks off the entire test suite
def run_unit_tests(executable_location):
    check_call([executable_location, '-m', 'pytest'])


# this script requires Bash, which exists on all major platforms (including Win10)
# assumption: python 2 is invoked with 'python', python 3 is invoked with 'python3'
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Validate Azure Storage packages.')
    parser.add_argument('--python-version', '-p', help='The desired python version', default='3')
    parser.add_argument('--create-package', '-c', help='Whether new packages need to be generated', default='y')
    parser.add_argument('--run-tests', '-t', help='Whether the unit tests should run', default='y')
    parser.add_argument('--environment', '-e', help='Choose from [local, test, prod]', default='local')

    # step 1: parse the command line arguments
    args = parser.parse_args()
    print("Starting package validation: python_version={0}, create_package={1}, run_tests={2}, environment={3}"
          .format(args.python_version, args.create_package, args.run_tests, args.environment))

    # step 2: generate wheels if necessary
    if args.create_package in ('yes', 'true', 'y', 't'):
        create_storage_package()

    # step 3: create the virtual environment for the specified python version
    if args.python_version == '2':
        virtual_py = create_py2_venv(args.environment)
    else:
        virtual_py = create_py3_venv(args.environment)

    # step 4: run unit test suite if necessary
    if args.run_tests in ('yes', 'true', 'y', 't'):
        run_unit_tests(virtual_py)
