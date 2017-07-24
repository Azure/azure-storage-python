# -------------------------------------------------------------------------
# Copyright (c) Microsoft.  All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# --------------------------------------------------------------------------
import uuid

from azure.common import (
    AzureConflictHttpError,
    AzureMissingResourceHttpError,
)


class DirectorySamples():
    def __init__(self, account):
        self.account = account

    def run_all_samples(self):
        self.service = self.account.create_file_service()

        self.list_directories_and_files()
        self.create_directory()
        self.delete_directory()
        self.directory_metadata()
        self.directory_properties()
        self.directory_exists()

    def _get_resource_reference(self, prefix):
        return '{}{}'.format(prefix, str(uuid.uuid4()).replace('-', ''))

    def _get_directory_reference(self, prefix='dir'):
        return self._get_resource_reference(prefix)

    def _create_share(self, prefix='share'):
        share_name = self._get_resource_reference(prefix)
        self.service.create_share(share_name)
        return share_name

    def list_directories_and_files(self):
        share_name = self._create_share()

        self.service.create_directory(share_name, 'dir1')
        self.service.create_directory(share_name, 'dir2')

        self.service.create_file(share_name, 'dir1', 'file1', 512)
        self.service.create_file(share_name, 'dir1', 'file2', 512)

        self.service.create_file(share_name, None, 'rootfile', 512)

        # Basic
        # List from root
        root_file_dir = list(self.service.list_directories_and_files(share_name))
        for res in root_file_dir:
            print(res.name)  # dir1, dir2, rootfile

        # List from directory
        dir1 = list(self.service.list_directories_and_files(share_name, 'dir1'))
        for res in dir1:
            print(res.name)  # file1, file2

        # Num results
        root_file_dir = list(self.service.list_directories_and_files(share_name, num_results=2))
        for res in root_file_dir:
            print(res.name)  # dir1, dir2

    def create_directory(self):
        share_name = self._create_share()

        # Basic
        dir_name = self._get_directory_reference()
        created = self.service.create_directory(share_name, dir_name)  # True

        # Metadata
        metadata = {'val1': 'foo', 'val2': 'blah'}
        dir_name = self._get_directory_reference()
        created = self.service.create_directory(share_name, dir_name, metadata=metadata)  # True

        # Fail on exist
        dir_name = self._get_directory_reference()
        created = self.service.create_directory(share_name, dir_name)  # True
        created = self.service.create_directory(share_name, dir_name)  # False
        try:
            self.service.create_directory(share_name, dir_name, fail_on_exist=True)
        except AzureConflictHttpError:
            pass

        self.service.delete_share(share_name)

    def delete_directory(self):
        share_name = self._create_share()

        # Basic
        dir_name = self._get_directory_reference()
        deleted = self.service.delete_directory(share_name, dir_name)  # True

        # Fail not exist
        dir_name = self._get_directory_reference()
        deleted = self.service.delete_directory(share_name, dir_name)  # False
        try:
            self.service.delete_directory(share_name, dir_name, fail_not_exist=True)
        except AzureMissingResourceHttpError:
            pass

        self.service.delete_share(share_name)

    def directory_metadata(self):
        share_name = self._create_share()
        dir_name = self._get_directory_reference()
        self.service.create_directory(share_name, dir_name)
        metadata = {'val1': 'foo', 'val2': 'blah'}

        # Basic
        self.service.set_directory_metadata(share_name, dir_name, metadata=metadata)
        metadata = self.service.get_directory_metadata(share_name,
                                                       dir_name)  # metadata={'val1': 'foo', 'val2': 'blah'}

        # Replaces values, does not merge
        metadata = {'new': 'val'}
        self.service.set_directory_metadata(share_name, dir_name, metadata=metadata)
        metadata = self.service.get_directory_metadata(share_name, dir_name)  # metadata={'new': 'val'}

        # Capital letters
        metadata = {'NEW': 'VAL'}
        self.service.set_directory_metadata(share_name, dir_name, metadata=metadata)
        metadata = self.service.get_directory_metadata(share_name, dir_name)  # metadata={'new': 'VAL'}

        # Clearing
        self.service.set_directory_metadata(share_name, dir_name)
        metadata = self.service.get_directory_metadata(share_name, dir_name)  # metadata={}

        self.service.delete_share(share_name)

    def directory_properties(self):
        share_name = self._create_share()
        dir_name = self._get_directory_reference()
        self.service.create_directory(share_name, dir_name)
        metadata = {'val1': 'foo', 'val2': 'blah'}

        # Note that directory has no settable properties

        # Basic
        directory = self.service.get_directory_properties(share_name, dir_name)
        etag = directory.properties.etag  # etag string
        lmt = directory.properties.last_modified  # datetime object

        # Metadata
        self.service.set_directory_metadata(share_name, dir_name, metadata=metadata)
        directory = self.service.get_directory_properties(share_name, dir_name)
        metadata = directory.metadata  # metadata={'val1': 'foo', 'val2': 'blah'}

        self.service.delete_share(share_name)

    def directory_exists(self):
        share_name = self._create_share()
        directory_name = self._get_directory_reference()

        # Basic
        exists = self.service.exists(share_name, directory_name)  # False
        self.service.create_directory(share_name, directory_name)
        exists = self.service.exists(share_name, directory_name)  # True

        self.service.delete_share(share_name)
