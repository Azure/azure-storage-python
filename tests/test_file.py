# coding: utf-8

#-------------------------------------------------------------------------
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
#--------------------------------------------------------------------------
import base64
import os
import random
import requests
import sys
import unittest
from datetime import datetime, timedelta
from azure.common import (
    AzureHttpError,
    AzureConflictHttpError,
    AzureMissingResourceHttpError,
)
from azure.storage import (
    AccessPolicy,
    ResourceTypes,
    AccountPermissions,
)
from azure.storage.file import (
    FileService,
    File,
    FileService,
    Range,
    ContentSettings,
    FilePermissions,
    SharePermissions,
)
from tests.common_recordingtestcase import (
    TestMode,
    record,
)
from tests.testcase import StorageTestCase


#------------------------------------------------------------------------------


class StorageFileTest(StorageTestCase):

    def setUp(self):
        super(StorageFileTest, self).setUp()

        self.fs = self._create_storage_service(FileService, self.settings)

        if self.settings.REMOTE_STORAGE_ACCOUNT_NAME and self.settings.REMOTE_STORAGE_ACCOUNT_KEY:
            self.fs2 = self._create_storage_service(
                FileService,
                self.settings,
                self.settings.REMOTE_STORAGE_ACCOUNT_NAME,
                self.settings.REMOTE_STORAGE_ACCOUNT_KEY,
            )
        else:
            print("REMOTE_STORAGE_ACCOUNT_NAME and REMOTE_STORAGE_ACCOUNT_KEY not set in test settings file.")

        # test chunking functionality by reducing the threshold
        # for chunking and the size of each chunk, otherwise
        # the tests would take too long to execute
        self.fs._FILE_MAX_DATA_SIZE = 64 * 1024
        self.fs._FILE_MAX_CHUNK_DATA_SIZE = 4 * 1024

        self.share_name = self.get_resource_name('utshare')
        self.additional_share_names = []
        self.remote_share_name = None

    def tearDown(self):
        if not self.is_playback():
            try:
                self.fs.delete_share(self.share_name)
            except:
                pass

            for name in self.additional_share_names:
                try:
                    self.fs.delete_share(name)
                except:
                    pass

            if self.remote_share_name:
                try:
                    self.fs2.delete_share(self.remote_share_name)
                except:
                    pass

        for tmp_file in ['file_input.temp.dat', 'file_output.temp.dat']:
            if os.path.isfile(tmp_file):
                try:
                    os.remove(tmp_file)
                except:
                    pass

        return super(StorageFileTest, self).tearDown()

    #--Helpers-----------------------------------------------------------------
    def _create_share_and_file(self, share_name, file_name,
                                        content_length):
        self.fs.create_share(self.share_name)
        resp = self.fs.create_file(self.share_name, None, file_name, content_length)
        self.assertIsNone(resp)

    def _create_share_and_file_with_text(self, share_name, file_name,
                                        text):
        self.fs.create_share(self.share_name)
        resp = self.fs.create_file_from_text(self.share_name, None, file_name, text)
        self.assertIsNone(resp)

    def _create_remote_share_and_file(self, source_file_name, data, sas=True):
        self.remote_share_name = self.get_resource_name('remotectnr')
        self.fs2.create_share(self.remote_share_name)
        self.fs2.create_file_from_bytes(self.remote_share_name, None, source_file_name, data)

        sas_token = None
        if sas:
            sas_token = self.fs2.generate_file_shared_access_signature(self.remote_share_name, 
                                                                 None, 
                                                                 source_file_name, 
                                                                 permission=FilePermissions.READ, 
                                                                 expiry=datetime.utcnow() + timedelta(hours=1))
        source_file_url = self.fs2.make_file_url(self.remote_share_name, None, source_file_name, sas_token=sas_token)
        return source_file_url

    def _wait_for_async_copy(self, share_name, file_name):
        count = 0
        file = self.fs.get_file_properties(share_name, None, file_name)
        while file.properties.copy.status != 'success':
            count = count + 1
            if count > 5:
                self.assertTrue(
                    False, 'Timed out waiting for async copy to complete.')
            self.sleep(5)
            file = self.fs.get_file_properties(share_name, None, file_name)
        self.assertEqual(file.properties.copy.status, 'success')

    def assertFileEqual(self, share_name, file_name, expected_data):
        actual_data = self.fs.get_file_to_bytes(share_name, None, file_name)
        self.assertEqual(actual_data.content, expected_data)

    def assertFileLengthEqual(self, share_name, file_name, expected_length):
        file = self.fs.get_file_properties(share_name, None, file_name)
        self.assertEqual(int(file.properties.content_length), expected_length)

    def _get_oversized_binary_data(self):
        '''Returns random binary data exceeding the size threshold for
        chunking file upload.'''
        size = self.fs._FILE_MAX_DATA_SIZE + 12345
        return self._get_random_bytes(size)

    def _get_expected_progress(self, file_size, unknown_size=True):
        result = []
        index = 0
        if unknown_size:
            result.append((0, None))
        else:
            while (index < file_size):
                result.append((index, file_size))
                index += self.fs._FILE_MAX_CHUNK_DATA_SIZE
        result.append((file_size, file_size))
        return result

    def _get_random_bytes(self, size):
        # Must not be really random, otherwise playback of recordings
        # won't work. Data must be randomized, but the same for each run.
        # Use the checksum of the qualified test name as the random seed.
        rand = random.Random(self.checksum)
        result = bytearray(size)
        for i in range(size):
            result[i] = rand.randint(0, 255)
        return bytes(result)

    def _get_oversized_file_binary_data(self):
        '''Returns random binary data exceeding the size threshold for
        chunking file upload.'''
        size = self.fs._FILE_MAX_DATA_SIZE + 16384
        return self._get_random_bytes(size)

    def _get_oversized_text_data(self):
        '''Returns random unicode text data exceeding the size threshold for
        chunking file upload.'''
        # Must not be really random, otherwise playback of recordings
        # won't work. Data must be randomized, but the same for each run.
        # Use the checksum of the qualified test name as the random seed.
        rand = random.Random(self.checksum)
        size = self.fs._FILE_MAX_DATA_SIZE + 12345
        text = u''
        words = [u'hello', u'world', u'python', u'啊齄丂狛狜']
        while (len(text) < size):
            index = rand.randint(0, len(words) - 1)
            text = text + u' ' + words[index]

        return text

    class NonSeekableFile(object):
        def __init__(self, wrapped_file):
            self.wrapped_file = wrapped_file

        def write(self, data):
            self.wrapped_file.write(data)

        def read(self, count):
            return self.wrapped_file.read(count)
        
    #--Test cases for shares -----------------------------------------
    @record
    def test_create_share(self):
        # Arrange

        # Act
        created = self.fs.create_share(self.share_name)

        # Assert
        self.assertTrue(created)

    @record
    def test_create_share_fail_on_exist(self):
        # Arrange

        # Act
        created = self.fs.create_share(self.share_name)
        with self.assertRaises(AzureConflictHttpError):
            self.fs.create_share(self.share_name, fail_on_exist=True)

        # Assert
        self.assertTrue(created)

    @record
    def test_create_share_with_already_existing_share(self):
        # Arrange

        # Act
        created1 = self.fs.create_share(self.share_name)
        created2 = self.fs.create_share(self.share_name)

        # Assert
        self.assertTrue(created1)
        self.assertFalse(created2)

    @record
    def test_create_share_with_metadata(self):
        # Arrange
        metadata = {'hello': 'world', 'number': '42'}

        # Act
        created = self.fs.create_share(self.share_name, metadata)

        # Assert
        self.assertTrue(created)
        md = self.fs.get_share_metadata(self.share_name)
        self.assertDictEqual(md, metadata)

    @record
    def test_create_share_with_quota(self):
        # Arrange

        # Act
        self.fs.create_share(self.share_name, quota=1)

        # Assert
        share = self.fs.get_share_properties(self.share_name)
        self.assertIsNotNone(share)
        self.assertEqual(share.properties.quota, 1)

    @record
    def test_share_exists(self):
        # Arrange
        self.fs.create_share(self.share_name)

        # Act
        exists = self.fs.exists(self.share_name)

        # Assert
        self.assertTrue(exists)

    @record
    def test_share_not_exists(self):
        # Arrange

        # Act
        exists = self.fs.exists(self.get_resource_name('missing'))

        # Assert
        self.assertFalse(exists)


    @record
    def test_list_shares_no_options(self):
        # Arrange
        self.fs.create_share(self.share_name)

        # Act
        shares = list(self.fs.list_shares())

        # Assert
        self.assertIsNotNone(shares)
        self.assertGreaterEqual(len(shares), 1)
        self.assertIsNotNone(shares[0])
        self.assertIsNotNone(shares[0].name)
        self.assertIsNotNone(shares[0].properties.quota)
        self.assertIsNotNone(shares[0].properties.last_modified)
        self.assertIsNotNone(shares[0].properties.etag)
        self.assertNamedItemInContainer(shares, self.share_name)

    @record
    def test_list_shares_with_prefix(self):
        # Arrange
        self.fs.create_share(self.share_name)

        # Act
        shares = list(self.fs.list_shares(self.share_name))

        # Assert
        self.assertIsNotNone(shares)
        self.assertEqual(len(shares), 1)
        self.assertIsNotNone(shares[0])
        self.assertEqual(shares[0].name, self.share_name)
        self.assertIsNone(shares[0].metadata)

    @record
    def test_list_shares_with_include_metadata(self):
        # Arrange
        metadata = {'hello': 'world', 'number': '42'}
        self.fs.create_share(self.share_name)
        resp = self.fs.set_share_metadata(self.share_name, metadata)

        # Act
        shares = list(self.fs.list_shares(self.share_name, None, None, 'metadata'))

        # Assert
        self.assertIsNotNone(shares)
        self.assertGreaterEqual(len(shares), 1)
        self.assertIsNotNone(shares[0])
        self.assertNamedItemInContainer(shares, self.share_name)
        self.assertDictEqual(shares[0].metadata, metadata)

    @record
    def test_list_shares_with_maxresults_and_marker(self):
        # Arrange
        self.additional_share_names = [self.share_name + 'a',
                                           self.share_name + 'b',
                                           self.share_name + 'c',
                                           self.share_name + 'd']
        for name in self.additional_share_names:
            self.fs.create_share(name)

        # Act
        generator1 = self.fs.list_shares(self.share_name, None, 2)
        generator2 = self.fs.list_shares(self.share_name, generator1.next_marker, 2)

        shares1 = generator1.items
        shares2 = generator2.items

        # Assert
        self.assertIsNotNone(shares1)
        self.assertEqual(len(shares1), 2)
        self.assertNamedItemInContainer(shares1, self.share_name + 'a')
        self.assertNamedItemInContainer(shares1, self.share_name + 'b')
        self.assertIsNotNone(shares2)
        self.assertEqual(len(shares2), 2)
        self.assertNamedItemInContainer(shares2, self.share_name + 'c')
        self.assertNamedItemInContainer(shares2, self.share_name + 'd')

    @record
    def test_set_share_metadata(self):
        # Arrange
        metadata = {'hello': 'world', 'number': '42'}
        self.fs.create_share(self.share_name)

        # Act
        resp = self.fs.set_share_metadata(self.share_name, metadata)

        # Assert
        self.assertIsNone(resp)
        md = self.fs.get_share_metadata(self.share_name)
        self.assertDictEqual(md, metadata)

    @record
    def test_set_share_metadata_with_non_existing_share(self):
        # Arrange

        # Act
        with self.assertRaises(AzureMissingResourceHttpError):
            self.fs.set_share_metadata(
                self.share_name, {'hello': 'world', 'number': '43'})

        # Assert

    @record
    def test_get_share_metadata(self):
        # Arrange
        metadata = {'hello': 'world', 'number': '42'}
        self.fs.create_share(self.share_name)
        self.fs.set_share_metadata(self.share_name, metadata)

        # Act
        md = self.fs.get_share_metadata(self.share_name)

        # Assert
        self.assertDictEqual(md, metadata)

    @record
    def test_get_share_metadata_with_non_existing_share(self):
        # Arrange

        # Act
        with self.assertRaises(AzureMissingResourceHttpError):
            self.fs.get_share_metadata(self.share_name)

        # Assert

    @record
    def test_get_share_properties(self):
        # Arrange
        metadata = {'hello': 'world', 'number': '42'}
        self.fs.create_share(self.share_name)
        self.fs.set_share_metadata(self.share_name, metadata)

        # Act
        props = self.fs.get_share_properties(self.share_name)

        # Assert
        self.assertIsNotNone(props)
        self.assertDictEqual(props.metadata, metadata)
        self.assertIsNotNone(props.properties.etag)

    @record
    def test_get_share_properties_with_non_existing_share(self):
        # Arrange

        # Act
        with self.assertRaises(AzureMissingResourceHttpError):
            self.fs.get_share_properties(self.share_name)

        # Assert

    @record
    def test_set_share_properties(self):
        # Arrange
        self.fs.create_share(self.share_name)
        self.fs.set_share_properties(self.share_name, 1)

        # Act
        props = self.fs.get_share_properties(self.share_name)

        # Assert
        self.assertIsNotNone(props)
        self.assertEqual(props.properties.quota, 1)

    @record
    def test_delete_share_with_existing_share(self):
        # Arrange
        self.fs.create_share(self.share_name)

        # Act
        deleted = self.fs.delete_share(self.share_name)

        # Assert
        self.assertTrue(deleted)
        self.assertFalse(self.fs.exists(self.share_name))

    @record
    def test_delete_share_with_existing_share_fail_not_exist(self):
        # Arrange
        self.fs.create_share(self.share_name)

        # Act
        deleted = self.fs.delete_share(self.share_name)

        # Assert
        self.assertTrue(deleted)
        self.assertFalse(self.fs.exists(self.share_name))

    @record
    def test_delete_share_with_non_existing_share(self):
        # Arrange

        # Act
        deleted = self.fs.delete_share(self.share_name, False)

        # Assert
        self.assertFalse(deleted)

    @record
    def test_delete_share_with_non_existing_share_fail_not_exist(self):
        # Arrange

        # Act
        with self.assertRaises(AzureMissingResourceHttpError):
            self.fs.delete_share(self.share_name, True)

        # Assert

    @record
    def test_get_share_stats(self):
        # Arrange
        data = b'hello world'
        self._create_share_and_file_with_text(
            self.share_name, 'file1', data)

        # Act
        stats = self.fs.get_share_stats(self.share_name)

        # Assert
        self.assertIsNotNone(stats)
        self.assertEqual(stats.share_usage, 1)

    #--Test cases for directories ----------------------------------------------
    @record
    def test_create_directories(self):
        # Arrange

        # Act
        self.fs.create_share(self.share_name)
        created = self.fs.create_directory(self.share_name, 'dir1')

        # Assert
        self.assertTrue(created)

    @record
    def test_create_directories_fail_on_exist(self):
        # Arrange

        # Act
        created = self.fs.create_share(self.share_name)
        created = self.fs.create_directory(self.share_name, 'dir1')
        with self.assertRaises(AzureConflictHttpError):
            self.fs.create_directory(self.share_name, 'dir1', True)

        # Assert
        self.assertTrue(created)

    @record
    def test_create_directory_with_already_existing_directory(self):
        # Arrange

        # Act
        created = self.fs.create_share(self.share_name)
        created1 = self.fs.create_directory(self.share_name, 'dir1')
        created2 = self.fs.create_directory(self.share_name, 'dir1')

        # Assert
        self.assertTrue(created1)
        self.assertFalse(created2)

    @record
    def test_get_directory_properties(self):
        # Arrange
        self.fs.create_share(self.share_name)
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
        self.fs.create_share(self.share_name)
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
        self.fs.create_share(self.share_name)
        self.fs.create_directory(self.share_name, 'dir1')

        # Act
        self.fs.set_directory_metadata(self.share_name, 'dir1', metadata)
        md = self.fs.get_directory_metadata(self.share_name, 'dir1')

        # Assert
        self.assertDictEqual(md, metadata)

    @record
    def test_delete_directory_with_existing_share(self):
        # Arrange
        self.fs.create_share(self.share_name)
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
        self.fs.create_share(self.share_name)
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
    def test_list_directories_and_files(self):
        # Arrange
        self.fs.create_share(self.share_name)
        self.fs.create_directory(self.share_name, 'dir1')
        self.fs.create_directory(self.share_name, 'dir2')
        self.fs.create_file(self.share_name, None, 'file1', 1024)
        self.fs.create_file(self.share_name, 'dir1', 'file2', 1025)

        # Act
        resp = list(self.fs.list_directories_and_files(self.share_name))

        # Assert
        self.assertIsNotNone(resp)
        self.assertEqual(len(resp), 3)
        self.assertIsNotNone(resp[0])
        self.assertNamedItemInContainer(resp, 'dir1')
        self.assertNamedItemInContainer(resp, 'dir2')
        self.assertNamedItemInContainer(resp, 'file1')

    @record
    def test_list_directories_and_files_with_maxresults(self):
        # Arrange
        self.fs.create_share(self.share_name)
        self.fs.create_directory(self.share_name, 'dir1')
        self.fs.create_file(self.share_name, None, 'filea1', 1024)
        self.fs.create_file(self.share_name, None, 'filea2', 1024)
        self.fs.create_file(self.share_name, None, 'filea3', 1024)
        self.fs.create_file(self.share_name, None, 'fileb1', 1024)

        # Act
        result = list(self.fs.list_directories_and_files(self.share_name, None, None, 2))

        # Assert
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 2)
        self.assertNamedItemInContainer(result, 'dir1')
        self.assertNamedItemInContainer(result, 'filea1')

    @record
    def test_list_directories_and_files_with_maxresults_and_marker(self):
        # Arrange
        self.fs.create_share(self.share_name)
        self.fs.create_directory(self.share_name, 'dir1')
        self.fs.create_file(self.share_name, 'dir1', 'filea1', 1024)
        self.fs.create_file(self.share_name, 'dir1', 'filea2', 1024)
        self.fs.create_file(self.share_name, 'dir1', 'filea3', 1024)
        self.fs.create_file(self.share_name, 'dir1', 'fileb1', 1024)

        # Act
        generator1 = self.fs.list_directories_and_files(self.share_name, 'dir1', None, 2)
        generator2 = self.fs.list_directories_and_files(self.share_name, 'dir1', generator1.next_marker, 2)

        result1 = generator1.items
        result2 = generator2.items

        # Assert
        self.assertEqual(len(result1), 2)
        self.assertEqual(len(result2), 2)
        self.assertNamedItemInContainer(result1, 'filea1')
        self.assertNamedItemInContainer(result1, 'filea2')
        self.assertNamedItemInContainer(result2, 'filea3')
        self.assertNamedItemInContainer(result2, 'fileb1')
        self.assertEqual(result2.next_marker, None)

    #--Test cases for files ----------------------------------------------
    @record
    def test_make_file_url(self):
        # Arrange

        # Act
        res = self.fs.make_file_url('vhds', 'vhd_dir', 'my.vhd')

        # Assert
        self.assertEqual(res, 'https://' + self.settings.STORAGE_ACCOUNT_NAME
                         + '.file.core.windows.net/vhds/vhd_dir/my.vhd')

    @record
    def test_make_file_url_no_directory(self):
        # Arrange

        # Act
        res = self.fs.make_file_url('vhds', None, 'my.vhd')

        # Assert
        self.assertEqual(res, 'https://' + self.settings.STORAGE_ACCOUNT_NAME
                         + '.file.core.windows.net/vhds/my.vhd')

    @record
    def test_make_file_url_with_protocol(self):
        # Arrange

        # Act
        res = self.fs.make_file_url('vhds', 'vhd_dir', 'my.vhd', protocol='http')

        # Assert
        self.assertEqual(res, 'http://' + self.settings.STORAGE_ACCOUNT_NAME
                         + '.file.core.windows.net/vhds/vhd_dir/my.vhd')

    @record
    def test_make_file_url_with_sas(self):
        # Arrange

        # Act
        res = self.fs.make_file_url(
            'vhds', 'vhd_dir', 'my.vhd', sas_token='sas')

        # Assert
        self.assertEqual(res, 'https://' + self.settings.STORAGE_ACCOUNT_NAME + 
                         '.file.core.windows.net/vhds/vhd_dir/my.vhd?sas')

    @record
    def test_create_file(self):
        # Arrange
        self.fs.create_share(self.share_name)

        # Act
        resp = self.fs.create_file(self.share_name, None, 'file1', 1024)

        # Assert
        self.assertIsNone(resp)

    @record
    def test_create_file_with_metadata(self):
        # Arrange
        metadata={'hello': 'world', 'number': '42'}
        self.fs.create_share(self.share_name)

        # Act
        resp = self.fs.create_file(self.share_name, None, 'file1', 1024, metadata=metadata)

        # Assert
        self.assertIsNone(resp)
        md = self.fs.get_file_metadata(self.share_name, None, 'file1')
        self.assertDictEqual(md, metadata)

    @record
    def test_file_exists(self):
        # Arrange
        self._create_share_and_file_with_text(
            self.share_name, 'file1', b'hello world')

        # Act
        exists = self.fs.exists(self.share_name, None, 'file1')

        # Assert
        self.assertTrue(exists)

    @record
    def test_file_not_exists(self):
        # Arrange

        # Act
        exists = self.fs.exists(self.get_resource_name('missing'), 'missingdir', 'file1')

        # Assert
        self.assertFalse(exists)

    @record
    def test_get_file_with_existing_file(self):
        # Arrange
        self._create_share_and_file_with_text(
            self.share_name, 'file1', b'hello world')

        # Act
        file = self.fs.get_file_to_bytes(self.share_name, None, 'file1')

        # Assert
        self.assertIsInstance(file, File)
        self.assertEqual(file.content, b'hello world')

    @record
    def test_get_file_with_range(self):
        # Arrange
        self._create_share_and_file_with_text(
            self.share_name, 'file1', b'hello world')

        # Act
        file = self.fs.get_file_to_bytes(
            self.share_name, None, 'file1', start_range=0, end_range=5)

        # Assert
        self.assertIsInstance(file, File)
        self.assertEqual(file.content, b'hello ')

    @record
    def test_get_file_with_range_and_get_content_md5(self):
        # Arrange
        self._create_share_and_file_with_text(
            self.share_name, 'file1', b'hello world')

        # Act
        file = self.fs.get_file_to_bytes(self.share_name, None, 'file1',
                                 start_range=0, end_range=5,
                                 range_get_content_md5=True)

        # Assert
        self.assertIsInstance(file, File)
        self.assertEqual(file.content, b'hello ')
        self.assertEqual(
            file.properties.content_settings.content_md5,
            '+BSJN3e8wilf/wXwDlCNpg==')

    @record
    def test_get_file_with_non_existing_share(self):
        # Arrange

        # Act
        with self.assertRaises(AzureMissingResourceHttpError):
            self.fs.get_file_to_bytes(self.share_name, None, 'file1')

        # Assert

    @record
    def test_get_file_with_non_existing_file(self):
        # Arrange
        self.fs.create_share(self.share_name)

        # Act
        with self.assertRaises(AzureMissingResourceHttpError):
            self.fs.get_file_to_bytes(self.share_name, None, 'file1')

        # Assert
        
    @record
    def test_resize_file(self):
        # Arrange
        self._create_share_and_file_with_text(
            self.share_name, 'file1', b'hello world')

        # Act
        resp = self.fs.resize_file(self.share_name, None, 
            'file1', 5)

        # Assert
        self.assertIsNone(resp)
        file = self.fs.get_file_properties(self.share_name, None, 'file1')
        self.assertEqual(file.properties.content_length, 5)

    @record
    def test_set_file_properties(self):
        # Arrange
        self._create_share_and_file_with_text(
            self.share_name, 'file1', b'hello world')

        # Act
        resp = self.fs.set_file_properties(
            self.share_name,
            None, 
            'file1',
            content_settings=ContentSettings(
                content_language='spanish',
                content_disposition='inline')
        )

        # Assert
        self.assertIsNone(resp)
        file = self.fs.get_file_properties(self.share_name, None, 'file1')
        self.assertEqual(file.properties.content_settings.content_language, 'spanish')
        self.assertEqual(file.properties.content_settings.content_disposition, 'inline')

    @record
    def test_set_file_properties_with_non_existing_share(self):
        # Arrange

        # Act
        with self.assertRaises(AzureMissingResourceHttpError):
            self.fs.set_file_properties(
                self.share_name, None, 'file1',
                content_settings=ContentSettings(content_language='spanish'))

        # Assert

    @record
    def test_set_file_properties_with_non_existing_file(self):
        # Arrange
        self.fs.create_share(self.share_name)
        self.fs.create_directory(self.share_name, 'dir1')

        # Act
        with self.assertRaises(AzureMissingResourceHttpError):
            self.fs.set_file_properties(
                self.share_name, 'dir1', 'file1',
                content_settings=ContentSettings(content_language='spanish'))

        # Assert

    @record
    def test_get_file_properties_with_existing_file(self):
        # Arrange
        self._create_share_and_file_with_text(
            self.share_name, 'file1', b'hello world')

        # Act
        file = self.fs.get_file_properties(
            self.share_name, None, 'file1')

        # Assert
        self.assertIsNotNone(file)
        self.assertEqual(file.properties.content_length, 11)

    @record
    def test_get_file_properties_with_non_existing_share(self):
        # Arrange

        # Act
        with self.assertRaises(AzureMissingResourceHttpError):
            self.fs.get_file_properties(self.share_name, None, 'file1')

        # Assert

    @record
    def test_get_file_properties_with_non_existing_file(self):
        # Arrange
        self.fs.create_share(self.share_name)

        # Act
        with self.assertRaises(AzureMissingResourceHttpError):
            self.fs.get_file_properties(self.share_name, None, 'file1')

        # Assert

    @record
    def test_get_file_metadata_with_existing_file(self):
        # Arrange
        self._create_share_and_file_with_text(
            self.share_name, 'file1', b'hello world')

        # Act
        md = self.fs.get_file_metadata(self.share_name, None, 'file1')

        # Assert
        self.assertIsNotNone(md)

    @record
    def test_set_file_metadata_with_upper_case(self):
        # Arrange
        metadata = {'hello': 'world', 'number': '42', 'UP': 'UPval'}
        self._create_share_and_file_with_text(
            self.share_name, 'file1', b'hello world')

        # Act
        resp = self.fs.set_file_metadata(
            self.share_name,
            None, 
            'file1',
            metadata)

        # Assert
        self.assertIsNone(resp)
        md = self.fs.get_file_metadata(self.share_name, None, 'file1')
        self.assertEqual(3, len(md))
        self.assertEqual(md['hello'], 'world')
        self.assertEqual(md['number'], '42')
        self.assertEqual(md['up'], 'UPval')

    @record
    def test_delete_file_with_existing_file(self):
        # Arrange
        self._create_share_and_file_with_text(
            self.share_name, 'file1', b'hello world')

        # Act
        resp = self.fs.delete_file(self.share_name, None, 'file1')

        # Assert
        self.assertIsNone(resp)

    @record
    def test_delete_file_with_non_existing_file(self):
        # Arrange
        self.fs.create_share(self.share_name)

        # Act
        with self.assertRaises(AzureMissingResourceHttpError):
            self.fs.delete_file (self.share_name, None, 'file1')

        # Assert

    @record
    def test_update_file(self):
        # Arrange
        self._create_share_and_file(
            self.share_name, 'file1', 1024)

        # Act
        data = b'abcdefghijklmnop' * 32
        resp = self.fs.update_range(
            self.share_name, None, 'file1', 
            data, 0, 511)

        # Assert
        self.assertIsNone(resp)

    @record
    def test_clear_file(self):
        # Arrange
        self._create_share_and_file(
            self.share_name, 'file1', 1024)

        # Act
        resp = self.fs.clear_range(
            self.share_name, None, 'file1', 0, 511)

        # Assert
        self.assertIsNone(resp)

    @record
    def test_update_file_unicode(self):
        # Arrange
        self._create_share_and_file(self.share_name, 'file1', 512)

        # Act
        data = u'abcdefghijklmnop' * 32
        with self.assertRaises(TypeError):
            self.fs.update_range(self.share_name, None, 'file1',
                             data, 0, 511)

        # Assert

    @record
    def test_list_ranges_none(self):
        # Arrange
        self._create_share_and_file(
            self.share_name, 'file1', 1024)

        # Act
        ranges = self.fs.list_ranges(self.share_name, None, 'file1')

        # Assert
        self.assertIsNotNone(ranges)
        self.assertEqual(len(ranges), 0)

    @record
    def test_list_ranges_2(self):
        # Arrange
        self._create_share_and_file(
            self.share_name, 'file1', 2048)
        data = b'abcdefghijklmnop' * 32
        resp1 = self.fs.update_range(
            self.share_name, None, 'file1', data, 0, 511)
        resp2 = self.fs.update_range(
            self.share_name, None, 'file1', data, 1024, 1535)

        # Act
        ranges = self.fs.list_ranges(self.share_name, None, 'file1')

        # Assert
        self.assertIsNotNone(ranges)
        self.assertEqual(len(ranges), 2)
        self.assertEqual(ranges[0].start, 0)
        self.assertEqual(ranges[0].end, 511)
        self.assertEqual(ranges[1].start, 1024)
        self.assertEqual(ranges[1].end, 1535)

    @record
    def test_list_ranges_iter(self):
        # Arrange
        self._create_share_and_file(
            self.share_name, 'file1', 2048)
        data = b'abcdefghijklmnop' * 32
        resp1 = self.fs.update_range(
            self.share_name, None, 'file1', data,
            0, 511)
        resp2 = self.fs.update_range(
            self.share_name, None, 'file1', data,
            1024, 1535)

        # Act
        ranges = self.fs.list_ranges(self.share_name, None, 'file1')
        for byte_range in ranges:
            pass

        # Assert
        self.assertEqual(len(ranges), 2)
        self.assertIsInstance(ranges[0], Range)
        self.assertIsInstance(ranges[1], Range)

    @record
    def test_copy_file_with_existing_file(self):
        # Arrange
        file_name = 'file1'
        data = b'abcdefghijklmnopqrstuvwxyz'
        self._create_share_and_file_with_text(
            self.share_name, file_name, data)

        # Act
        source_file_url = self.fs.make_file_url(self.share_name, None, file_name)
        resp = self.fs.copy_file(self.share_name, None, 'file1copy', source_file_url)

        # Assert
        self.assertIsNotNone(resp)
        self.assertEqual(resp.status, 'success')
        self.assertIsNotNone(resp.id)
        copy = self.fs.get_file_to_bytes(self.share_name, None, 'file1copy')
        self.assertEqual(copy.content, data)

    @record
    def test_copy_file_async_private_file(self):
        # Arrange
        self.fs.create_share(self.share_name)
        data = b'12345678' * 1024 * 1024
        source_file_name = 'sourcefile'
        source_file_url = self._create_remote_share_and_file(source_file_name, data, False)

        # Act
        target_file_name = 'targetfile'
        with self.assertRaises(AzureMissingResourceHttpError):
            self.fs.copy_file(self.share_name, None,
                              target_file_name, source_file_url)

        # Assert

    @record
    def test_copy_file_async_private_file_with_sas(self):
        # Arrange
        self.fs.create_share(self.share_name)
        data = b'12345678' * 1024 * 1024
        source_file_name = 'sourcefile'
        source_file_url = self._create_remote_share_and_file(source_file_name, data)

        # Act
        target_file_name = 'targetfile'
        self.fs2.create_share(self.share_name)
        copy_resp = self.fs.copy_file(
            self.share_name, None, target_file_name, source_file_url)

        # Assert
        self.assertEqual(copy_resp.status, 'pending')
        self._wait_for_async_copy(self.share_name, target_file_name)
        self.assertFileEqual(self.share_name, target_file_name, data)

    @record
    def test_abort_copy_file(self):
        # Arrange
        self.fs.create_share(self.share_name)
        data = b'12345678' * 1024 * 1024
        source_file_name = 'sourcefile'
        source_file_url = self._create_remote_share_and_file(source_file_name, data)

        # Act
        target_file_name = 'targetfile'
        copy_resp = self.fs.copy_file(
            self.share_name, None, target_file_name, source_file_url)
        self.assertEqual(copy_resp.status, 'pending')
        self.fs.abort_copy_file(self.share_name, None, 'targetfile', copy_resp.id)

        # Assert
        target_file = self.fs.get_file_to_bytes(self.share_name, None, target_file_name)
        self.assertEqual(target_file.content, b'')
        self.assertEqual(target_file.properties.copy.status, 'aborted')

    @record
    def test_abort_copy_file_with_synchronous_copy_fails(self):
        # Arrange
        source_file_name = 'sourcefile'
        self._create_share_and_file_with_text(
            self.share_name, source_file_name, b'hello world')
        source_file_url = self.fs.make_file_url(self.share_name, None, source_file_name)

        # Act
        target_file_name = 'targetfile'
        copy_resp = self.fs.copy_file(
            self.share_name, None, target_file_name, source_file_url)
        with self.assertRaises(AzureHttpError):
            self.fs.abort_copy_file(
                self.share_name,
                None,
                target_file_name,
                copy_resp.id)

        # Assert
        self.assertEqual(copy_resp.status, 'success')

    @record
    def test_with_filter(self):
        # Single filter
        if sys.version_info < (3,):
            strtype = (str, unicode)
            strornonetype = (str, unicode, type(None))
        else:
            strtype = str
            strornonetype = (str, type(None))

        called = []

        def my_filter(request, next):
            called.append(True)
            for header in request.headers:
                self.assertIsInstance(header, tuple)
                for item in header:
                    self.assertIsInstance(item, strornonetype)
            self.assertIsInstance(request.host, strtype)
            self.assertIsInstance(request.method, strtype)
            self.assertIsInstance(request.path, strtype)
            self.assertIsInstance(request.query, list)
            self.assertIsInstance(request.body, strtype)
            response = next(request)

            self.assertIsInstance(response.body, (bytes, type(None)))
            self.assertIsInstance(response.headers, list)
            for header in response.headers:
                self.assertIsInstance(header, tuple)
                for item in header:
                    self.assertIsInstance(item, strtype)
            self.assertIsInstance(response.status, int)
            return response

        bc = self.fs.with_filter(my_filter)
        bc.create_share(self.share_name + '0', fail_on_exist=False)

        self.assertTrue(called)

        del called[:]

        bc.delete_share(self.share_name + '0')

        self.assertTrue(called)
        del called[:]

        # Chained filters
        def filter_a(request, next):
            called.append('a')
            return next(request)

        def filter_b(request, next):
            called.append('b')
            return next(request)

        bc = self.fs.with_filter(filter_a).with_filter(filter_b)
        bc.create_share(self.share_name + '1', fail_on_exist=False)

        self.assertEqual(called, ['b', 'a'])

        bc.delete_share(self.share_name + '1')

        self.assertEqual(called, ['b', 'a', 'b', 'a'])

    @record
    def test_unicode_create_share_unicode_name(self):
        # Arrange
        self.share_name = self.share_name + u'啊齄丂狛狜'

        # Act
        with self.assertRaises(AzureHttpError):
            # not supported - share name must be alphanumeric, lowercase
            self.fs.create_share(self.share_name)

        # Assert

    @record
    def test_unicode_get_file_unicode_name(self):
        # Arrange
        self._create_share_and_file_with_text(
            self.share_name, '啊齄丂狛狜', b'hello world')

        # Act
        file = self.fs.get_file_to_bytes(self.share_name, None, '啊齄丂狛狜')

        # Assert
        self.assertIsInstance(file, File)
        self.assertEqual(file.content, b'hello world')

    @record
    def test_put_file_block_file_unicode_data(self):
        # Arrange
        self.fs.create_share(self.share_name)

        # Act
        data = u'hello world啊齄丂狛狜'.encode('utf-8')
        resp = self.fs.create_file(
            self.share_name, None, 'file1', 1024)

        # Assert
        self.assertIsNone(resp)

    @record
    def test_unicode_get_file_unicode_data(self):
        # Arrange
        file_data = u'hello world啊齄丂狛狜'.encode('utf-8')
        self._create_share_and_file_with_text(
            self.share_name, 'file1', file_data)

        # Act
        file = self.fs.get_file_to_bytes(self.share_name, None, 'file1')

        # Assert
        self.assertIsInstance(file, File)
        self.assertEqual(file.content, file_data)

    @record
    def test_unicode_get_file_binary_data(self):
        # Arrange
        base64_data = 'AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+P0BBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWltcXV5fYGFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6e3x9fn+AgYKDhIWGh4iJiouMjY6PkJGSk5SVlpeYmZqbnJ2en6ChoqOkpaanqKmqq6ytrq+wsbKztLW2t7i5uru8vb6/wMHCw8TFxsfIycrLzM3Oz9DR0tPU1dbX2Nna29zd3t/g4eLj5OXm5+jp6uvs7e7v8PHy8/T19vf4+fr7/P3+/wABAgMEBQYHCAkKCwwNDg8QERITFBUWFxgZGhscHR4fICEiIyQlJicoKSorLC0uLzAxMjM0NTY3ODk6Ozw9Pj9AQUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVpbXF1eX2BhYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ent8fX5/gIGCg4SFhoeIiYqLjI2Oj5CRkpOUlZaXmJmam5ydnp+goaKjpKWmp6ipqqusra6vsLGys7S1tre4ubq7vL2+v8DBwsPExcbHyMnKy8zNzs/Q0dLT1NXW19jZ2tvc3d7f4OHi4+Tl5ufo6err7O3u7/Dx8vP09fb3+Pn6+/z9/v8AAQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyAhIiMkJSYnKCkqKywtLi8wMTIzNDU2Nzg5Ojs8PT4/QEFCQ0RFRkdISUpLTE1OT1BRUlNUVVZXWFlaW1xdXl9gYWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXp7fH1+f4CBgoOEhYaHiImKi4yNjo+QkZKTlJWWl5iZmpucnZ6foKGio6SlpqeoqaqrrK2ur7CxsrO0tba3uLm6u7y9vr/AwcLDxMXGx8jJysvMzc7P0NHS09TV1tfY2drb3N3e3+Dh4uPk5ebn6Onq6+zt7u/w8fLz9PX29/j5+vv8/f7/AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+P0BBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWltcXV5fYGFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6e3x9fn+AgYKDhIWGh4iJiouMjY6PkJGSk5SVlpeYmZqbnJ2en6ChoqOkpaanqKmqq6ytrq+wsbKztLW2t7i5uru8vb6/wMHCw8TFxsfIycrLzM3Oz9DR0tPU1dbX2Nna29zd3t/g4eLj5OXm5+jp6uvs7e7v8PHy8/T19vf4+fr7/P3+/w=='
        binary_data = base64.b64decode(base64_data)

        self._create_share_and_file_with_text(
            self.share_name, 'file1', binary_data)

        # Act
        file = self.fs.get_file_to_bytes(self.share_name, None, 'file1')

        # Assert
        self.assertIsInstance(file, File)
        self.assertEqual(file.content, binary_data)

    @record
    def test_get_file_to_bytes(self):
        # Arrange
        file_name = 'file1'
        data = b'abcdefghijklmnopqrstuvwxyz'
        self._create_share_and_file_with_text(
            self.share_name, file_name, data)

        # Act
        resp = self.fs.get_file_to_bytes(self.share_name, None, file_name)

        # Assert
        self.assertEqual(data, resp.content)

    @record
    def test_get_file_to_bytes_chunked_download(self):
        # Arrange
        file_name = 'file1'
        data = self._get_oversized_binary_data()
        self._create_share_and_file_with_text(
            self.share_name, file_name, data)

        # Act
        resp = self.fs.get_file_to_bytes(self.share_name, None, file_name)

        # Assert
        self.assertEqual(data, resp.content)

    def test_get_file_to_bytes_chunked_download_parallel(self):
        # parallel tests introduce random order of requests, can only run live
        if TestMode.need_recordingfile(self.test_mode):
            return

        # Arrange
        file_name = 'file1'
        data = self._get_oversized_binary_data()
        self._create_share_and_file_with_text(
            self.share_name, file_name, data)

        # Act
        resp = self.fs.get_file_to_bytes(self.share_name, None, file_name,
                                         max_connections=10)

        # Assert
        self.assertEqual(data, resp.content)

    def test_ranged_get_file_to_bytes_chunked_download_parallel(self):
        # parallel tests introduce random order of requests, can only run live
        if TestMode.need_recordingfile(self.test_mode):
            return

        # Arrange
        file_name = 'file1'
        data = self._get_oversized_binary_data()
        self._create_share_and_file_with_text(
            self.share_name, file_name, data)

        # Act
        resp = self.fs.get_file_to_bytes(self.share_name, None, file_name,
                                         start_range=0, max_connections=10)

        # Assert
        self.assertEqual(data, resp.content)

    def test_ranged_get_file_to_bytes(self):
        # parallel tests introduce random order of requests, can only run live
        if TestMode.need_recordingfile(self.test_mode):
            return

        # Arrange
        file_name = 'file1'
        data = b'foo'
        self._create_share_and_file_with_text(
            self.share_name, file_name, data)

        # Act
        resp = self.fs.get_file_to_bytes(self.share_name, None, file_name,
                                         start_range=1, end_range=3, max_connections=10)

        # Assert
        self.assertEqual(b"oo", resp.content)

    def test_ranged_get_file_to_bytes_md5_without_end_range_fail(self):
        # parallel tests introduce random order of requests, can only run live
        if TestMode.need_recordingfile(self.test_mode):
            return

        # Arrange
        file_name = 'file1'
        data = b'foo'
        self._create_share_and_file_with_text(
            self.share_name, file_name, data)

        # Act
        with self.assertRaises(ValueError):
            self.fs.get_file_to_bytes(self.share_name, None, file_name, start_range=1,
                                      range_get_content_md5=True, max_connections=10)

        # Assert

    @record
    def test_get_file_to_bytes_with_progress(self):
        # Arrange
        file_name = 'file1'
        data = b'abcdefghijklmnopqrstuvwxyz'
        self._create_share_and_file_with_text(
            self.share_name, file_name, data)

        # Act
        progress = []

        def callback(current, total):
            progress.append((current, total))

        resp = self.fs.get_file_to_bytes(
            self.share_name, None, file_name, progress_callback=callback)

        # Assert
        self.assertEqual(data, resp.content)
        self.assertEqual(progress, self._get_expected_progress(len(data)))

    @record
    def test_get_file_to_bytes_with_progress_chunked_download(self):
        # Arrange
        file_name = 'file1'
        data = self._get_oversized_binary_data()
        self._create_share_and_file_with_text(
            self.share_name, file_name, data)

        # Act
        progress = []

        def callback(current, total):
            progress.append((current, total))

        resp = self.fs.get_file_to_bytes(
            self.share_name, None, file_name, progress_callback=callback,
            max_connections=2)

        # Assert
        self.assertEqual(data, resp.content)
        self.assertEqual(progress, self._get_expected_progress(len(data), False))

    @record
    def test_get_file_to_stream(self):
        # Arrange
        file_name = 'file1'
        data = b'abcdefghijklmnopqrstuvwxyz'
        file_path = 'file_output.temp.dat'
        self._create_share_and_file_with_text(
            self.share_name, file_name, data)

        # Act
        with open(file_path, 'wb') as stream:
            resp = self.fs.get_file_to_stream(
                self.share_name, None, file_name, stream)

        # Assert
        self.assertIsInstance(resp, File)
        with open(file_path, 'rb') as stream:
            actual = stream.read()
            self.assertEqual(data, actual)

    @record
    def test_get_file_to_stream_chunked_download(self):
        # Arrange
        file_name = 'file1'
        data = self._get_oversized_binary_data()
        file_path = 'file_output.temp.dat'
        self._create_share_and_file_with_text(
            self.share_name, file_name, data)

        # Act
        with open(file_path, 'wb') as stream:
            resp = self.fs.get_file_to_stream(
                self.share_name, None, file_name, stream)

        # Assert
        self.assertIsInstance(resp, File)
        with open(file_path, 'rb') as stream:
            actual = stream.read()
            self.assertEqual(data, actual)

    def test_get_file_to_stream_chunked_download_parallel(self):
        # parallel tests introduce random order of requests, can only run live
        if TestMode.need_recordingfile(self.test_mode):
            return

        # Arrange
        file_name = 'file1'
        data = self._get_oversized_binary_data()
        file_path = 'file_output.temp.dat'
        self._create_share_and_file_with_text(
            self.share_name, file_name, data)

        # Act
        with open(file_path, 'wb') as stream:
            resp = self.fs.get_file_to_stream(
                self.share_name, None, file_name, stream,
                max_connections=10)

        # Assert
        self.assertIsInstance(resp, File)
        with open(file_path, 'rb') as stream:
            actual = stream.read()
            self.assertEqual(data, actual)

    @record
    def test_get_file_to_stream_non_seekable_chunked_download(self):
        # Arrange
        file_name = 'file1'
        data = self._get_oversized_binary_data()
        file_path = 'file_output.temp.dat'
        self._create_share_and_file_with_text(
            self.share_name, file_name, data)

        # Act
        with open(file_path, 'wb') as stream:
            non_seekable_stream = StorageFileTest.NonSeekableFile(stream)
            resp = self.fs.get_file_to_stream(
                self.share_name, None, file_name, non_seekable_stream,
                max_connections=1)

        # Assert
        self.assertIsInstance(resp, File)
        with open(file_path, 'rb') as stream:
            actual = stream.read()
            self.assertEqual(data, actual)

    def test_get_file_to_stream_non_seekable_chunked_download_parallel(self):
        # parallel tests introduce random order of requests, can only run live
        if TestMode.need_recordingfile(self.test_mode):
            return

        # Arrange
        file_name = 'file1'
        data = self._get_oversized_binary_data()
        file_path = 'file_output.temp.dat'
        self._create_share_and_file_with_text(
            self.share_name, file_name, data)

        # Act
        with open(file_path, 'wb') as stream:
            non_seekable_stream = StorageFileTest.NonSeekableFile(stream)

            # Parallel downloads require that the file be seekable
            with self.assertRaises(AttributeError):
                resp = self.fs.get_file_to_stream(
                    self.share_name, None, file_name, non_seekable_stream,
                    max_connections=10)

        # Assert

    @record
    def test_get_file_to_stream_with_progress(self):
        # Arrange
        file_name = 'file1'
        data = b'abcdefghijklmnopqrstuvwxyz'
        file_path = 'file_output.temp.dat'
        self._create_share_and_file_with_text(
            self.share_name, file_name, data)

        # Act
        progress = []

        def callback(current, total):
            progress.append((current, total))

        with open(file_path, 'wb') as stream:
            resp = self.fs.get_file_to_stream(
                self.share_name, None, file_name, stream,
                progress_callback=callback)

        # Assert
        self.assertIsInstance(resp, File)
        with open(file_path, 'rb') as stream:
            actual = stream.read()
            self.assertEqual(data, actual)
        self.assertEqual(progress, self._get_expected_progress(len(data)))

    def test_get_file_to_stream_with_progress_chunked_download_parallel(self):
        # parallel tests introduce random order of requests, can only run live
        if TestMode.need_recordingfile(self.test_mode):
            return

        # Arrange
        file_name = 'file1'
        data = self._get_oversized_binary_data()
        file_path = 'file_output.temp.dat'
        self._create_share_and_file_with_text(
            self.share_name, file_name, data)

        # Act
        progress = []

        def callback(current, total):
            progress.append((current, total))

        with open(file_path, 'wb') as stream:
            resp = self.fs.get_file_to_stream(
                self.share_name, None, file_name, stream,
                progress_callback=callback,
                max_connections=5)

        # Assert
        self.assertIsInstance(resp, File)
        with open(file_path, 'rb') as stream:
            actual = stream.read()
            self.assertEqual(data, actual)
        self.assertEqual(progress, sorted(progress))
        self.assertGreater(len(progress), 0)

    @record
    def test_get_file_to_path(self):
        # Arrange
        file_name = 'file1'
        data = b'abcdefghijklmnopqrstuvwxyz'
        file_path = 'file_output.temp.dat'
        self._create_share_and_file_with_text(
            self.share_name, file_name, data)

        # Act
        resp = self.fs.get_file_to_path(
            self.share_name, None, file_name, file_path)

        # Assert
        self.assertIsInstance(resp, File)
        with open(file_path, 'rb') as stream:
            actual = stream.read()
            self.assertEqual(data, actual)

    @record
    def test_get_file_to_path_chunked_downlad(self):
        # Arrange
        file_name = 'file1'
        data = self._get_oversized_binary_data()
        file_path = 'file_output.temp.dat'
        self._create_share_and_file_with_text(
            self.share_name, file_name, data)

        # Act
        resp = self.fs.get_file_to_path(
            self.share_name, None, file_name, file_path)

        # Assert
        self.assertIsInstance(resp, File)
        with open(file_path, 'rb') as stream:
            actual = stream.read()
            self.assertEqual(data, actual)

    def test_get_file_to_path_chunked_downlad_parallel(self):
        # parallel tests introduce random order of requests, can only run live
        if TestMode.need_recordingfile(self.test_mode):
            return

        # Arrange
        file_name = 'file1'
        data = self._get_oversized_binary_data()
        file_path = 'file_output.temp.dat'
        self._create_share_and_file_with_text(
            self.share_name, file_name, data)

        # Act
        resp = self.fs.get_file_to_path(
            self.share_name, None, file_name, file_path,
            max_connections=10)

        # Assert
        self.assertIsInstance(resp, File)
        with open(file_path, 'rb') as stream:
            actual = stream.read()
            self.assertEqual(data, actual)

    @record
    def test_get_file_to_path_with_progress(self):
        # Arrange
        file_name = 'file1'
        data = b'abcdefghijklmnopqrstuvwxyz'
        file_path = 'file_output.temp.dat'
        self._create_share_and_file_with_text(
            self.share_name, file_name, data)

        # Act
        progress = []

        def callback(current, total):
            progress.append((current, total))

        resp = self.fs.get_file_to_path(
            self.share_name, None, file_name, file_path,
            progress_callback=callback)

        # Assert
        self.assertIsInstance(resp, File)
        with open(file_path, 'rb') as stream:
            actual = stream.read()
            self.assertEqual(data, actual)
        self.assertEqual(progress, self._get_expected_progress(len(data)))

    @record
    def test_get_file_to_path_with_progress_chunked_download(self):
        # Arrange
        file_name = 'file1'
        data = self._get_oversized_binary_data()
        file_path = 'file_output.temp.dat'
        self._create_share_and_file_with_text(
            self.share_name, file_name, data)

        # Act
        progress = []

        def callback(current, total):
            progress.append((current, total))

        resp = self.fs.get_file_to_path(
            self.share_name, None, file_name, file_path,
            progress_callback=callback, max_connections=2)

        # Assert
        self.assertIsInstance(resp, File)
        with open(file_path, 'rb') as stream:
            actual = stream.read()
            self.assertEqual(data, actual)
        self.assertEqual(progress, self._get_expected_progress(len(data), False))

    @record
    def test_get_file_to_path_with_mode(self):
        # Arrange
        file_name = 'file1'
        data = b'abcdefghijklmnopqrstuvwxyz'
        file_path = 'file_output.temp.dat'
        self._create_share_and_file_with_text(
            self.share_name, file_name, data)
        with open(file_path, 'wb') as stream:
            stream.write(b'abcdef')

        # Act
        resp = self.fs.get_file_to_path(
            self.share_name, None, file_name, file_path, 'a+b')

        # Assert
        self.assertIsInstance(resp, File)
        with open(file_path, 'rb') as stream:
            actual = stream.read()
            self.assertEqual(b'abcdef' + data, actual)

    @record
    def test_get_file_to_path_with_mode_chunked_download(self):
        # Arrange
        file_name = 'file1'
        data = self._get_oversized_binary_data()
        file_path = 'file_output.temp.dat'
        self._create_share_and_file_with_text(
            self.share_name, file_name, data)
        with open(file_path, 'wb') as stream:
            stream.write(b'abcdef')

        # Act
        resp = self.fs.get_file_to_path(
            self.share_name, None, file_name, file_path, 'a+b')

        # Assert
        self.assertIsInstance(resp, File)
        with open(file_path, 'rb') as stream:
            actual = stream.read()
            self.assertEqual(b'abcdef' + data, actual)

    @record
    def test_get_file_to_text(self):
        # Arrange
        file_name = 'file1'
        text = u'hello 啊齄丂狛狜 world'
        data = text.encode('utf-8')
        self._create_share_and_file_with_text(
            self.share_name, file_name, data)

        # Act
        resp = self.fs.get_file_to_text(self.share_name, None, file_name)

        # Assert
        self.assertEqual(text, resp.content)

    @record
    def test_get_file_to_text_with_encoding(self):
        # Arrange
        file_name = 'file1'
        text = u'hello 啊齄丂狛狜 world'
        data = text.encode('utf-16')
        self._create_share_and_file_with_text(
            self.share_name, file_name, data)

        # Act
        resp = self.fs.get_file_to_text(
            self.share_name, None, file_name, 'utf-16')

        # Assert
        self.assertEqual(text, resp.content)

    @record
    def test_get_file_to_text_chunked_download(self):
        # Arrange
        file_name = 'file1'
        text = self._get_oversized_text_data()
        data = text.encode('utf-8')
        self._create_share_and_file_with_text(
            self.share_name, file_name, data)

        # Act
        resp = self.fs.get_file_to_text(self.share_name, None, file_name)

        # Assert
        self.assertEqual(text, resp.content)

    def test_get_file_to_text_chunked_download_parallel(self):
        # parallel tests introduce random order of requests, can only run live
        if TestMode.need_recordingfile(self.test_mode):
            return

        # Arrange
        file_name = 'file1'
        text = self._get_oversized_text_data()
        data = text.encode('utf-8')
        self._create_share_and_file_with_text(
            self.share_name, file_name, data)

        # Act
        resp = self.fs.get_file_to_text(self.share_name, None, file_name,
                                        max_connections=10)

        # Assert
        self.assertEqual(text, resp.content)

    @record
    def test_get_file_to_text_with_progress(self):
        # Arrange
        file_name = 'file1'
        text = u'hello 啊齄丂狛狜 world'
        data = text.encode('utf-8')
        self._create_share_and_file_with_text(
            self.share_name, file_name, data)

        # Act
        progress = []

        def callback(current, total):
            progress.append((current, total))

        resp = self.fs.get_file_to_text(
            self.share_name, None, file_name, progress_callback=callback)

        # Assert
        self.assertEqual(text, resp.content)
        self.assertEqual(progress, self._get_expected_progress(len(data)))

    @record
    def test_get_file_to_text_with_encoding_and_progress(self):
        # Arrange
        file_name = 'file1'
        text = u'hello 啊齄丂狛狜 world'
        data = text.encode('utf-16')
        self._create_share_and_file_with_text(
            self.share_name, file_name, data)

        # Act
        progress = []

        def callback(current, total):
            progress.append((current, total))

        resp = self.fs.get_file_to_text(
            self.share_name, None, file_name, 'utf-16',
            progress_callback=callback)

        # Assert
        self.assertEqual(text, resp.content)
        self.assertEqual(progress, self._get_expected_progress(len(data)))

    @record
    def test_create_file_from_bytes(self):
        # Arrange
        self.fs.create_share(self.share_name)

        # Act
        data = self._get_random_bytes(2048)
        resp = self.fs.create_file_from_bytes(
            self.share_name, None, 'file1', data)

        # Assert
        self.assertIsNone(resp)
        self.assertEqual(data, self.fs.get_file_to_bytes(self.share_name, None, 'file1').content)

    @record
    def test_create_file_from_bytes_with_progress(self):
        # Arrange
        self.fs.create_share(self.share_name)

        # Act
        progress = []

        def callback(current, total):
            progress.append((current, total))

        data = self._get_random_bytes(2048)
        resp = self.fs.create_file_from_bytes(
            self.share_name, None, 'file1', data, progress_callback=callback)

        # Assert
        self.assertIsNone(resp)
        self.assertEqual(data, self.fs.get_file_to_bytes(self.share_name, None, 'file1').content)
        self.assertEqual(progress, self._get_expected_progress(len(data), False))

    @record
    def test_create_file_from_bytes_with_index(self):
        # Arrange
        self.fs.create_share(self.share_name)
        index = 1024

        # Act
        data = self._get_random_bytes(2048)
        resp = self.fs.create_file_from_bytes(
            self.share_name, None, 'file1', data, index)

        # Assert
        self.assertIsNone(resp)
        self.assertEqual(data[index:],
                         self.fs.get_file_to_bytes(self.share_name, None, 'file1').content)

    @record
    def test_create_file_from_bytes_with_index_and_count(self):
        # Arrange
        self.fs.create_share(self.share_name)
        index = 512
        count = 1024

        # Act
        data = self._get_random_bytes(2048)
        resp = self.fs.create_file_from_bytes(
            self.share_name, None, 'file1', data, index, count)

        # Assert
        self.assertIsNone(resp)
        self.assertEqual(data[index:index + count],
                         self.fs.get_file_to_bytes(self.share_name, None, 'file1').content)

    @record
    def test_create_file_from_bytes_chunked_upload(self):
        # Arrange
        self.fs.create_share(self.share_name)
        file_name = 'file1'
        data = self._get_oversized_file_binary_data()

        # Act
        resp = self.fs.create_file_from_bytes(
            self.share_name, None, file_name, data)

        # Assert
        self.assertIsNone(resp)
        self.assertFileLengthEqual(self.share_name, file_name, len(data))
        self.assertFileEqual(self.share_name, file_name, data)

    def test_create_file_from_bytes_chunked_upload_parallel(self):
        # parallel tests introduce random order of requests, can only run live
        if TestMode.need_recordingfile(self.test_mode):
            return

        # Arrange
        self.fs.create_share(self.share_name)
        file_name = 'file1'
        data = self._get_oversized_file_binary_data()

        # Act
        resp = self.fs.create_file_from_bytes(
            self.share_name, None, file_name, data,
            max_connections=10)

        # Assert
        self.assertIsNone(resp)
        self.assertFileLengthEqual(self.share_name, file_name, len(data))
        self.assertFileEqual(self.share_name, file_name, data)

    @record
    def test_create_file_from_bytes_chunked_upload_with_index_and_count(self):
        # Arrange
        self.fs.create_share(self.share_name)
        file_name = 'file1'
        data = self._get_oversized_file_binary_data()
        index = 512
        count = len(data) - 1024

        # Act
        resp = self.fs.create_file_from_bytes(
            self.share_name, None, file_name, data, index, count)

        # Assert
        self.assertIsNone(resp)
        self.assertFileLengthEqual(self.share_name, file_name, count)
        self.assertFileEqual(self.share_name,
                             file_name, data[index:index + count])

    def test_create_file_from_bytes_chunked_upload_with_index_and_count_parallel(self):
        # parallel tests introduce random order of requests, can only run live
        if TestMode.need_recordingfile(self.test_mode):
            return

        # Arrange
        self.fs.create_share(self.share_name)
        file_name = 'file1'
        data = self._get_oversized_file_binary_data()
        index = 512
        count = len(data) - 1024

        # Act
        resp = self.fs.create_file_from_bytes(
            self.share_name, None, file_name, data, index, count,
            max_connections=10)

        # Assert
        self.assertIsNone(resp)
        self.assertFileLengthEqual(self.share_name, file_name, count)
        self.assertFileEqual(self.share_name,
                             file_name, data[index:index + count])

    @record
    def test_create_file_from_path_chunked_upload(self):
        # Arrange
        self.fs.create_share(self.share_name)
        file_name = 'file1'
        data = self._get_oversized_file_binary_data()
        file_path = 'file_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)

        # Act
        resp = self.fs.create_file_from_path(
            self.share_name, None, file_name, file_path)

        # Assert
        self.assertIsNone(resp)
        self.assertFileLengthEqual(self.share_name, file_name, len(data))
        self.assertFileEqual(self.share_name, file_name, data)

    def test_create_file_from_path_chunked_upload_parallel(self):
        # parallel tests introduce random order of requests, can only run live
        if TestMode.need_recordingfile(self.test_mode):
            return

        # Arrange
        self.fs.create_share(self.share_name)
        file_name = 'file1'
        data = self._get_oversized_file_binary_data()
        file_path = 'file_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)

        # Act
        resp = self.fs.create_file_from_path(
            self.share_name, None, file_name, file_path,
            max_connections=10)

        # Assert
        self.assertIsNone(resp)
        self.assertFileLengthEqual(self.share_name, file_name, len(data))
        self.assertFileEqual(self.share_name, file_name, data)

    @record
    def test_create_file_from_path_with_progress_chunked_upload(self):
        # Arrange
        self.fs.create_share(self.share_name)
        file_name = 'file1'
        data = self._get_oversized_file_binary_data()
        file_path = 'file_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)

        # Act
        progress = []

        def callback(current, total):
            progress.append((current, total))

        resp = self.fs.create_file_from_path(
            self.share_name, None, file_name, file_path,
            progress_callback=callback)

        # Assert
        self.assertIsNone(resp)
        self.assertFileLengthEqual(self.share_name, file_name, len(data))
        self.assertFileEqual(self.share_name, file_name, data)
        self.assertEqual(progress, self._get_expected_progress(len(data), False))

    @record
    def test_create_file_from_stream_chunked_upload(self):
        # Arrange
        self.fs.create_share(self.share_name)
        file_name = 'file1'
        data = self._get_oversized_file_binary_data()
        file_path = 'file_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)

        # Act
        file_size = len(data)
        with open(file_path, 'rb') as stream:
            resp = self.fs.create_file_from_stream(
                self.share_name, None, file_name, stream, file_size)

        # Assert
        self.assertIsNone(resp)
        self.assertFileLengthEqual(self.share_name, file_name, file_size)
        self.assertFileEqual(self.share_name, file_name, data[:file_size])

    def test_create_file_from_stream_chunked_upload_parallel(self):
        # parallel tests introduce random order of requests, can only run live
        if TestMode.need_recordingfile(self.test_mode):
            return

        # Arrange
        self.fs.create_share(self.share_name)
        file_name = 'file1'
        data = self._get_oversized_file_binary_data()
        file_path = 'file_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)

        # Act
        file_size = len(data)
        with open(file_path, 'rb') as stream:
            resp = self.fs.create_file_from_stream(
                self.share_name, None, file_name, stream, file_size,
                max_connections=10)

        # Assert
        self.assertIsNone(resp)
        self.assertFileLengthEqual(self.share_name, file_name, file_size)
        self.assertFileEqual(self.share_name, file_name, data[:file_size])

    @record
    def test_create_file_from_stream_non_seekable_chunked_upload(self):
        # Arrange
        self.fs.create_share(self.share_name)
        file_name = 'file1'
        data = self._get_oversized_file_binary_data()
        file_path = 'file_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)

        # Act
        file_size = len(data)
        with open(file_path, 'rb') as stream:
            non_seekable_file = StorageFileTest.NonSeekableFile(stream)
            resp = self.fs.create_file_from_stream(
                self.share_name, None, file_name, non_seekable_file, file_size,
                max_connections=1)

        # Assert
        self.assertIsNone(resp)
        self.assertFileLengthEqual(self.share_name, file_name, file_size)
        self.assertFileEqual(self.share_name, file_name, data[:file_size])

    def test_create_file_from_stream_non_seekable_chunked_upload_parallel(self):
        # parallel tests introduce random order of requests, can only run live
        if TestMode.need_recordingfile(self.test_mode):
            return

        # Arrange
        self.fs.create_share(self.share_name)
        file_name = 'file1'
        data = self._get_oversized_file_binary_data()
        file_path = 'file_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)

        # Act
        file_size = len(data)
        with open(file_path, 'rb') as stream:
            non_seekable_file = StorageFileTest.NonSeekableFile(stream)

            # Parallel uploads require that the file be seekable
            with self.assertRaises(AttributeError):
                resp = self.fs.create_file_from_stream(
                    self.share_name, None, file_name, non_seekable_file, 
                    file_size, max_connections=10)

        # Assert

    @record
    def test_create_file_from_stream_with_progress_chunked_upload(self):
        # Arrange
        self.fs.create_share(self.share_name)
        file_name = 'file1'
        data = self._get_oversized_file_binary_data()
        file_path = 'file_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)

        # Act
        progress = []

        def callback(current, total):
            progress.append((current, total))

        file_size = len(data)
        with open(file_path, 'rb') as stream:
            resp = self.fs.create_file_from_stream(
                self.share_name, None, file_name, stream, file_size,
                progress_callback=callback)

        # Assert
        self.assertIsNone(resp)
        self.assertFileLengthEqual(self.share_name, file_name, file_size)
        self.assertFileEqual(self.share_name, file_name, data[:file_size])
        self.assertEqual(progress, self._get_expected_progress(len(data), False))

    def test_create_file_from_stream_with_progress_chunked_upload_parallel(self):
        # parallel tests introduce random order of requests, can only run live
        if TestMode.need_recordingfile(self.test_mode):
            return

        # Arrange
        self.fs.create_share(self.share_name)
        file_name = 'file1'
        data = self._get_oversized_file_binary_data()
        file_path = 'file_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)

        # Act
        progress = []

        def callback(current, total):
            progress.append((current, total))

        file_size = len(data)
        with open(file_path, 'rb') as stream:
            resp = self.fs.create_file_from_stream(
                self.share_name, None, file_name, stream, file_size,
                progress_callback=callback,
                max_connections=5)

        # Assert
        self.assertIsNone(resp)
        self.assertFileLengthEqual(self.share_name, file_name, file_size)
        self.assertFileEqual(self.share_name, file_name, data[:file_size])
        self.assertEqual(progress, sorted(progress))
        self.assertGreater(len(progress), 0)

    @record
    def test_create_file_from_stream_chunked_upload_truncated(self):
        # Arrange
        self.fs.create_share(self.share_name)
        file_name = 'file1'
        data = self._get_oversized_file_binary_data()
        file_path = 'file_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)

        # Act
        file_size = len(data) - 512
        with open(file_path, 'rb') as stream:
            resp = self.fs.create_file_from_stream(
                self.share_name, None, file_name, stream, file_size)

        # Assert
        self.assertIsNone(resp)
        self.assertFileLengthEqual(self.share_name, file_name, file_size)
        self.assertFileEqual(self.share_name, file_name, data[:file_size])

    def test_create_file_from_stream_chunked_upload_truncated_parallel(self):
        # parallel tests introduce random order of requests, can only run live
        if TestMode.need_recordingfile(self.test_mode):
            return

        # Arrange
        self.fs.create_share(self.share_name)
        file_name = 'file1'
        data = self._get_oversized_file_binary_data()
        file_path = 'file_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)

        # Act
        file_size = len(data) - 512
        with open(file_path, 'rb') as stream:
            resp = self.fs.create_file_from_stream(
                self.share_name, None, file_name, stream, file_size,
                max_connections=10)

        # Assert
        self.assertIsNone(resp)
        self.assertFileLengthEqual(self.share_name, file_name, file_size)
        self.assertFileEqual(self.share_name, file_name, data[:file_size])

    @record
    def test_create_file_from_stream_with_progress_chunked_upload_truncated(self):
        # Arrange
        self.fs.create_share(self.share_name)
        file_name = 'file1'
        data = self._get_oversized_file_binary_data()
        file_path = 'file_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)

        # Act
        progress = []

        def callback(current, total):
            progress.append((current, total))

        file_size = len(data) - 512
        with open(file_path, 'rb') as stream:
            resp = self.fs.create_file_from_stream(
                self.share_name, None, file_name, stream, file_size,
                progress_callback=callback)

        # Assert
        self.assertIsNone(resp)
        self.assertFileLengthEqual(self.share_name, file_name, file_size)
        self.assertFileEqual(self.share_name, file_name, data[:file_size])
        self.assertEqual(progress, self._get_expected_progress(file_size, False))


    #--Test cases for sas & acl ------------------------------------------------
    @record
    def test_sas_access_file(self):
        # SAS URL is calculated from storage key, so this test runs live only
        if TestMode.need_recordingfile(self.test_mode):
            return

        # Arrange
        data = b'shared access signature with read permission on file'
        file_name = 'file1.txt'
        # Arrange
        self._create_share_and_file_with_text(
            self.share_name, file_name, data)
        
        token = self.fs.generate_file_shared_access_signature(
            self.share_name,
            None,
            file_name,
            permission=FilePermissions.READ,
            expiry=datetime.utcnow() + timedelta(hours=1),
        )

        # Act
        service = FileService(
            self.settings.STORAGE_ACCOUNT_NAME,
            sas_token=token,
            request_session=requests.Session(),
        )
        self._set_service_options(service, self.settings)
        result = service.get_file_to_bytes(self.share_name, None, file_name)

        # Assert
        self.assertEqual(data, result.content)

    @record
    def test_sas_signed_identifier(self):
        # SAS URL is calculated from storage key, so this test runs live only
        if TestMode.need_recordingfile(self.test_mode):
            return

        # Arrange
        data = b'shared access signature with signed identifier'
        file_name = 'file1.txt'
        self._create_share_and_file_with_text(
            self.share_name, file_name, data)

        access_policy = AccessPolicy()
        access_policy.start = '2011-10-11'
        access_policy.expiry = '2018-10-12'
        access_policy.permission = FilePermissions.READ
        identifiers = {'testid': access_policy}

        resp = self.fs.set_share_acl(self.share_name, identifiers)

        token = self.fs.generate_file_shared_access_signature(
            self.share_name,
            None,
            file_name,
            id='testid'
            )

        # Act
        service = FileService(
            self.settings.STORAGE_ACCOUNT_NAME,
            sas_token=token,
            request_session=requests.Session(),
        )
        self._set_service_options(service, self.settings)
        result = service.get_file_to_bytes(self.share_name, None, file_name)

        # Assert
        self.assertEqual(data, result.content)


    @record
    def test_account_sas(self):
        # SAS URL is calculated from storage key, so this test runs live only
        if TestMode.need_recordingfile(self.test_mode):
            return

        # Arrange
        data = b'shared access signature with read permission on file'
        file_name = 'file1.txt'
        self._create_share_and_file_with_text(
            self.share_name, file_name, data)

        token = self.fs.generate_account_shared_access_signature(
            ResourceTypes.OBJECT,
            AccountPermissions.READ,
            datetime.utcnow() + timedelta(hours=1),
        )

        # Act
        url = self.fs.make_file_url(
            self.share_name,
            None,
            file_name,
            sas_token=token,
        )
        response = requests.get(url)

        # Assert
        self.assertTrue(response.ok)
        self.assertEqual(data, response.content)

    @record
    def test_shared_read_access_file(self):
        # SAS URL is calculated from storage key, so this test runs live only
        if TestMode.need_recordingfile(self.test_mode):
            return

        # Arrange
        data = b'shared access signature with read permission on file'
        file_name = 'file1.txt'
        self._create_share_and_file_with_text(
            self.share_name, file_name, data)

        token = self.fs.generate_file_shared_access_signature(
            self.share_name,
            None,
            file_name,
            permission=FilePermissions.READ,
            expiry=datetime.utcnow() + timedelta(hours=1),
        )

        # Act
        url = self.fs.make_file_url(
            self.share_name,
            None,
            file_name,
            sas_token=token,
        )
        response = requests.get(url)

        # Assert
        self.assertTrue(response.ok)
        self.assertEqual(data, response.content)

    @record
    def test_shared_read_access_file_with_content_query_params(self):
        # SAS URL is calculated from storage key, so this test runs live only
        if TestMode.need_recordingfile(self.test_mode):
            return

        # Arrange
        data = b'shared access signature with read permission on file'
        file_name = 'file1.txt'
        self._create_share_and_file_with_text(
            self.share_name, file_name, data)

        token = self.fs.generate_file_shared_access_signature(
            self.share_name,
            None,
            file_name,
            permission=FilePermissions.READ,
            expiry=datetime.utcnow() + timedelta(hours=1),
            cache_control='no-cache',
            content_disposition='inline',
            content_encoding='utf-8',
            content_language='fr',
            content_type='text',
        )
        url = self.fs.make_file_url(
            self.share_name,
            None,
            file_name,
            sas_token=token,
        )

        # Act
        response = requests.get(url)

        # Assert
        self.assertEqual(data, response.content)
        self.assertEqual(response.headers['cache-control'], 'no-cache')
        self.assertEqual(response.headers['content-disposition'], 'inline')
        self.assertEqual(response.headers['content-encoding'], 'utf-8')
        self.assertEqual(response.headers['content-language'], 'fr')
        self.assertEqual(response.headers['content-type'], 'text')

    @record
    def test_shared_write_access_file(self):
        # SAS URL is calculated from storage key, so this test runs live only
        if TestMode.need_recordingfile(self.test_mode):
            return

        # Arrange
        data = b'shared access signature with write permission on file'
        updated_data = b'updated file data'
        file_name = 'file1.txt'
        self._create_share_and_file_with_text(
            self.share_name, file_name, data)

        token = self.fs.generate_file_shared_access_signature(
            self.share_name,
            None,
            file_name,
            permission=FilePermissions.WRITE,
            expiry=datetime.utcnow() + timedelta(hours=1),
        )
        url = self.fs.make_file_url(
            self.share_name,
            None,
            file_name,
            sas_token=token,
        )

        # Act
        headers={'x-ms-range': 'bytes=0-16', 'x-ms-write': 'update'} 
        response = requests.put(url + '&comp=range', headers=headers, data=updated_data)

        # Assert
        self.assertTrue(response.ok)
        file = self.fs.get_file_to_bytes(self.share_name, None, 'file1.txt')
        self.assertEqual(b'updated file datanature with write permission on file', file.content)

    @record
    def test_shared_delete_access_file(self):
        # SAS URL is calculated from storage key, so this test runs live only
        if TestMode.need_recordingfile(self.test_mode):
            return

        # Arrange
        data = b'shared access signature with delete permission on file'
        file_name = 'file1.txt'
        self._create_share_and_file_with_text(
            self.share_name, file_name, data)

        token = self.fs.generate_file_shared_access_signature(
            self.share_name,
            None,
            file_name,
            permission=FilePermissions.DELETE,
            expiry=datetime.utcnow() + timedelta(hours=1),
        )
        url = self.fs.make_file_url(
            self.share_name,
            None,
            file_name,
            sas_token=token,
        )

        # Act
        response = requests.delete(url)

        # Assert
        self.assertTrue(response.ok)
        with self.assertRaises(AzureMissingResourceHttpError):
            file = self.fs.get_file_to_bytes(self.share_name, None, file_name)

    @record
    def test_shared_access_share(self):
        # SAS URL is calculated from storage key, so this test runs live only
        if TestMode.need_recordingfile(self.test_mode):
            return

        # Arrange
        data = b'shared access signature with read permission on share'
        file_name = 'file1.txt'
        self._create_share_and_file_with_text(
            self.share_name, file_name, data)

        token = self.fs.generate_share_shared_access_signature(
            self.share_name,
            expiry=datetime.utcnow() + timedelta(hours=1),
            permission=SharePermissions.READ,
        )
        url = self.fs.make_file_url(
            self.share_name,
            None,
            file_name,
            sas_token=token,
        )

        # Act
        response = requests.get(url)

        # Assert
        self.assertTrue(response.ok)
        self.assertEqual(data, response.content)

    @record
    def test_set_share_acl_with_empty_signed_identifiers(self):
        # Arrange
        self.fs.create_share(self.share_name)

        # Act
        resp = self.fs.set_share_acl(self.share_name, dict())

        # Assert
        self.assertIsNone(resp)
        acl = self.fs.get_share_acl(self.share_name)
        self.assertIsNotNone(acl)
        self.assertEqual(len(acl), 0)

    @record
    def test_set_share_acl_with_signed_identifiers(self):
        # Arrange
        self.fs.create_share(self.share_name)

        # Act
        identifiers = dict()
        identifiers['testid'] = AccessPolicy(
            permission=SharePermissions.READ,
            expiry=datetime.utcnow() + timedelta(hours=1),  
            start=datetime.utcnow() - timedelta(minutes=1),  
            )

        resp = self.fs.set_share_acl(self.share_name, identifiers)

        # Assert
        self.assertIsNone(resp)
        acl = self.fs.get_share_acl(self.share_name)
        self.assertIsNotNone(acl)
        self.assertEqual(len(acl), 1)
        self.assertTrue('testid' in acl)


#------------------------------------------------------------------------------
if __name__ == '__main__':
    unittest.main()
