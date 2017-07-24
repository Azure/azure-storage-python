# coding: utf-8

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
import unittest

from azure.common import (
    AzureConflictHttpError,
    AzureMissingResourceHttpError,
)

from azure.storage.file import (
    FileService,
)
from tests.testcase import (
    StorageTestCase,
    record,
)


# ------------------------------------------------------------------------------


class StorageDirectoryTest(StorageTestCase):
    def setUp(self):
        super(StorageDirectoryTest, self).setUp()

        self.fs = self._create_storage_service(FileService, self.settings)
        self.share_name = self.get_resource_name('utshare')

        if not self.is_playback():
            self.fs.create_share(self.share_name)

    def tearDown(self):
        if not self.is_playback():
            try:
                self.fs.delete_share(self.share_name)
            except:
                pass

        return super(StorageDirectoryTest, self).tearDown()

    # --Helpers-----------------------------------------------------------------

    # --Test cases for directories ----------------------------------------------
    @record
    def test_create_directories(self):
        # Arrange

        # Act
        created = self.fs.create_directory(self.share_name, 'dir1')

        # Assert
        self.assertTrue(created)

    @record
    def test_create_directories_with_metadata(self):
        # Arrange
        metadata = {'hello': 'world', 'number': '42'}

        # Act
        self.fs.create_directory(self.share_name, 'dir1', metadata=metadata)

        # Assert
        md = self.fs.get_directory_metadata(self.share_name, 'dir1')
        self.assertDictEqual(md, metadata)

    @record
    def test_create_directories_fail_on_exist(self):
        # Arrange

        # Act
        created = self.fs.create_directory(self.share_name, 'dir1')
        with self.assertRaises(AzureConflictHttpError):
            self.fs.create_directory(self.share_name, 'dir1', fail_on_exist=True)

        # Assert
        self.assertTrue(created)

    @record
    def test_create_directory_with_already_existing_directory(self):
        # Arrange

        # Act
        created1 = self.fs.create_directory(self.share_name, 'dir1')
        created2 = self.fs.create_directory(self.share_name, 'dir1')

        # Assert
        self.assertTrue(created1)
        self.assertFalse(created2)

    @record
    def test_get_directory_properties(self):
        # Arrange
        self.fs.create_directory(self.share_name, 'dir1')

        # Act
        props = self.fs.get_directory_properties(self.share_name, 'dir1')

        # Assert
        self.assertIsNotNone(props)
        self.assertIsNotNone(props.properties.etag)
        self.assertIsNotNone(props.properties.last_modified)

    @record
    def test_get_directory_properties_with_non_existing_directory(self):
        # Arrange

        # Act
        with self.assertRaises(AzureMissingResourceHttpError):
            self.fs.get_directory_properties(self.share_name, 'dir1')

            # Assert

    @record
    def test_directory_exists(self):
        # Arrange
        self.fs.create_directory(self.share_name, 'dir1')

        # Act
        exists = self.fs.exists(self.share_name, 'dir1')

        # Assert
        self.assertTrue(exists)

    @record
    def test_directory_not_exists(self):
        # Arrange

        # Act
        exists = self.fs.exists(self.share_name, 'missing')

        # Assert
        self.assertFalse(exists)

    @record
    def test_get_set_directory_metadata(self):
        # Arrange
        metadata = {'hello': 'world', 'number': '43'}
        self.fs.create_directory(self.share_name, 'dir1')

        # Act
        self.fs.set_directory_metadata(self.share_name, 'dir1', metadata)
        md = self.fs.get_directory_metadata(self.share_name, 'dir1')

        # Assert
        self.assertDictEqual(md, metadata)

    @record
    def test_delete_directory_with_existing_share(self):
        # Arrange
        self.fs.create_directory(self.share_name, 'dir1')

        # Act
        deleted = self.fs.delete_directory(self.share_name, 'dir1')

        # Assert
        self.assertTrue(deleted)
        with self.assertRaises(AzureMissingResourceHttpError):
            self.fs.get_directory_properties(self.share_name, 'dir1')

    @record
    def test_delete_directory_with_existing_directory_fail_not_exist(self):
        # Arrange
        self.fs.create_directory(self.share_name, 'dir1')

        # Act
        deleted = self.fs.delete_directory(self.share_name, 'dir1')

        # Assert
        self.assertTrue(deleted)
        with self.assertRaises(AzureMissingResourceHttpError):
            self.fs.get_directory_properties(self.share_name, 'dir1')

    @record
    def test_delete_directory_with_non_existing_directory(self):
        # Arrange

        # Act
        deleted = self.fs.delete_directory(self.share_name, 'dir1', False)

        # Assert
        self.assertFalse(deleted)

    @record
    def test_delete_directory_with_non_existing_directory_fail_not_exist(self):
        # Arrange

        # Act
        with self.assertRaises(AzureMissingResourceHttpError):
            self.fs.delete_directory(self.share_name, 'dir1', True)

            # Assert

    @record
    def test_get_directory_properties_server_encryption(self):
        # Arrange
        self.fs.create_directory(self.share_name, 'dir1')

        # Act
        props = self.fs.get_directory_properties(self.share_name, 'dir1')

        # Assert
        self.assertIsNotNone(props)
        self.assertIsNotNone(props.properties.etag)
        self.assertIsNotNone(props.properties.last_modified)

        if self.is_file_encryption_enabled():
            self.assertTrue(props.properties.server_encrypted)
        else:
            self.assertFalse(props.properties.server_encrypted)


# ------------------------------------------------------------------------------
if __name__ == '__main__':
    unittest.main()
