#!/usr/bin/env python

# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import argparse
import os
from subprocess import check_call

DEFAULT_DESTINATION_FOLDER = "../dist"
package_list = ['azure-storage-blob', 'azure-storage-file', 'azure-storage-queue',
                'azure-storage-common', 'azure-storage-nspkg']


def create_package(name, dest_folder=DEFAULT_DESTINATION_FOLDER):
    absdirpath = os.path.abspath(name)
    check_call(['python', 'setup.py', 'bdist_wheel', '-d', dest_folder], cwd=absdirpath)
    check_call(['python', 'setup.py', "sdist", '-d', dest_folder], cwd=absdirpath)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Build Azure package.')
    parser.add_argument('name', help='The package name')
    parser.add_argument('--dest', '-d', default=DEFAULT_DESTINATION_FOLDER,
                        help='Destination folder. Relative to the package dir. [default: %(default)s]')

    args = parser.parse_args()
    if args.name == 'all':
        for package in package_list:
            create_package(package, args.dest)
    else:
        create_package(args.name, args.dest)
