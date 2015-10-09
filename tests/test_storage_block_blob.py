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
import datetime
import os
import random
import requests
import sys
import unittest

from azure.common import (
    AzureHttpError,
    AzureConflictHttpError,
    AzureMissingResourceHttpError,
)
from azure.storage import (
    DEV_ACCOUNT_NAME,
    DEV_ACCOUNT_KEY,
    AccessPolicy,
    Logging,
    HourMetrics,
    MinuteMetrics,
    SharedAccessPolicy,
    SignedIdentifier,
    SignedIdentifiers,
    StorageServiceProperties,
)
from azure.storage.blob import (
    BLOB_SERVICE_HOST_BASE,
    BlobBlockList,
    BlobResult,
    BlockBlobService,
    BlobSharedAccessPermissions,
    ContainerSharedAccessPermissions,
    PageList,
    PageRange,
)
from azure.storage.storageclient import (
    AZURE_STORAGE_ACCESS_KEY,
    AZURE_STORAGE_ACCOUNT,
    EMULATED,
)
from tests.common_recordingtestcase import (
    TestMode,
    record,
)
from tests.storage_testcase import StorageTestCase


#------------------------------------------------------------------------------


class StorageBlockBlobTest(StorageTestCase):

    def setUp(self):
        super(StorageBlockBlobTest, self).setUp()

        self.bs = self._create_storage_service(BlockBlobService, self.settings)

        if self.settings.REMOTE_STORAGE_ACCOUNT_NAME and self.settings.REMOTE_STORAGE_ACCOUNT_KEY:
            self.bs2 = self._create_storage_service(
                BlockBlobService,
                self.settings,
                self.settings.REMOTE_STORAGE_ACCOUNT_NAME,
                self.settings.REMOTE_STORAGE_ACCOUNT_KEY,
            )
        else:
            print("REMOTE_STORAGE_ACCOUNT_NAME and REMOTE_STORAGE_ACCOUNT_KEY not set in test settings file.")

        # test chunking functionality by reducing the threshold
        # for chunking and the size of each chunk, otherwise
        # the tests would take too long to execute
        self.bs._BLOB_MAX_DATA_SIZE = 64 * 1024
        self.bs._BLOB_MAX_CHUNK_DATA_SIZE = 4 * 1024

        self.container_name = self.get_resource_name('utcontainer')
        self.container_lease_id = None
        self.additional_container_names = []
        self.remote_container_name = None

    def tearDown(self):
        if not self.is_playback():
            if self.container_lease_id:
                try:
                    self.bs.break_container_lease(
                        self.container_name, self.container_lease_id)
                except:
                    pass
            try:
                self.bs.delete_container(self.container_name)
            except:
                pass

            for name in self.additional_container_names:
                try:
                    self.bs.delete_container(name)
                except:
                    pass

            if self.remote_container_name:
                try:
                    self.bs2.delete_container(self.remote_container_name)
                except:
                    pass

        for tmp_file in ['blob_input.temp.dat', 'blob_output.temp.dat']:
            if os.path.isfile(tmp_file):
                try:
                    os.remove(tmp_file)
                except:
                    pass

        return super(StorageBlockBlobTest, self).tearDown()

    #--Helpers-----------------------------------------------------------------
    def _create_container(self, container_name):
        self.bs.create_container(container_name, None, None, True)

    def _create_container_and_blob(self, container_name, blob_name, blob_data):
        self._create_container(container_name)
        resp = self.bs.put_blob(container_name, blob_name, blob_data)
        self.assertIsNone(resp)

    def _create_container_and_blob_with_random_data(
        self, container_name, blob_name, block_count, block_size):

        self._create_container_and_blob(container_name, blob_name, '')
        block_list = []
        for i in range(0, block_count):
            block_id = '{0:04d}'.format(i)
            block_data = self._get_random_bytes(block_size)
            self.bs.put_block(container_name, blob_name, block_data, block_id)
            block_list.append(block_id)
        self.bs.put_block_list(container_name, blob_name, block_list)

    def _blob_exists(self, container_name, blob_name):
        resp = self.bs.list_blobs(container_name)
        for blob in resp:
            if blob.name == blob_name:
                return True
        return False

    def _create_remote_container_and_blob(self, source_blob_name, data,
                                          x_ms_blob_public_access):
        self.remote_container_name = self.get_resource_name('remotectnr')
        self.bs2.create_container(
            self.remote_container_name,
            x_ms_blob_public_access=x_ms_blob_public_access)
        self.bs2.put_blob_from_bytes(
            self.remote_container_name, source_blob_name, data)
        source_blob_url = self.bs2.make_blob_url(
            self.remote_container_name, source_blob_name)
        return source_blob_url

    def _wait_for_async_copy(self, container_name, blob_name):
        count = 0
        props = self.bs.get_blob_properties(container_name, blob_name)
        while props['x-ms-copy-status'] != 'success':
            count = count + 1
            if count > 5:
                self.assertTrue(
                    False, 'Timed out waiting for async copy to complete.')
            self.sleep(5)
            props = self.bs.get_blob_properties(container_name, blob_name)
        self.assertEqual(props['x-ms-copy-status'], 'success')

    def assertBlobEqual(self, container_name, blob_name, expected_data):
        actual_data = self.bs.get_blob(container_name, blob_name)
        self.assertEqual(actual_data, expected_data)

    def assertBlobLengthEqual(self, container_name, blob_name, expected_length):
        props = self.bs.get_blob_properties(container_name, blob_name)
        self.assertEqual(int(props['content-length']), expected_length)

    def _get_oversized_binary_data(self):
        '''Returns random binary data exceeding the size threshold for
        chunking blob upload.'''
        size = self.bs._BLOB_MAX_DATA_SIZE + 12345
        return self._get_random_bytes(size)

    def _get_expected_progress(self, blob_size, unknown_size=False):
        result = []
        index = 0
        total = None if unknown_size else blob_size
        while (index < blob_size):
            result.append((index, total))
            index += self.bs._BLOB_MAX_CHUNK_DATA_SIZE
        result.append((blob_size, total))
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

    def _get_oversized_text_data(self):
        '''Returns random unicode text data exceeding the size threshold for
        chunking blob upload.'''
        # Must not be really random, otherwise playback of recordings
        # won't work. Data must be randomized, but the same for each run.
        # Use the checksum of the qualified test name as the random seed.
        rand = random.Random(self.checksum)
        size = self.bs._BLOB_MAX_DATA_SIZE + 12345
        text = u''
        words = [u'hello', u'world', u'python', u'?????']
        while (len(text) < size):
            index = rand.randint(0, len(words) - 1)
            text = text + u' ' + words[index]

        return text

    def _get_shared_access_policy(self, permission):
        date_format = "%Y-%m-%dT%H:%M:%SZ"
        start = datetime.datetime.utcnow() - datetime.timedelta(minutes=1)
        expiry = start + datetime.timedelta(hours=1)
        return SharedAccessPolicy(
            AccessPolicy(
                start.strftime(date_format),
                expiry.strftime(date_format),
                permission
            )
        )

    class NonSeekableFile(object):
        def __init__(self, wrapped_file):
            self.wrapped_file = wrapped_file

        def write(self, data):
            self.wrapped_file.write(data)

        def read(self, count):
            return self.wrapped_file.read(count)

    #--Test cases for block blobs --------------------------------------------

    @record
    def test_put_blob(self):
        # Arrange
        self._create_container(self.container_name)

        # Act
        data = b'hello world'
        resp = self.bs.put_blob(self.container_name, 'blob1', data)

        # Assert
        self.assertIsNone(resp)

    @record
    def test_put_blob_unicode(self):
        # Arrange
        self._create_container(self.container_name)

        # Act
        data = u'hello world'
        with self.assertRaises(TypeError):
            resp = self.bs.put_blob(self.container_name, 'blob1', data)

        # Assert

    @record
    def test_put_blob_with_lease_id(self):
        # Arrange
        self._create_container_and_blob(
            self.container_name, 'blob1', b'hello world')
        lease = self.bs.acquire_blob_lease(self.container_name, 'blob1')
        lease_id = lease['x-ms-lease-id']

        # Act
        data = b'hello world again'
        resp = self.bs.put_blob(
            self.container_name, 'blob1', data, x_ms_lease_id=lease_id)

        # Assert
        self.assertIsNone(resp)
        blob = self.bs.get_blob(
            self.container_name, 'blob1', x_ms_lease_id=lease_id)
        self.assertEqual(blob, b'hello world again')

    @record
    def test_put_blob_with_metadata(self):
        # Arrange
        self._create_container(self.container_name)

        # Act
        data = b'hello world'
        resp = self.bs.put_blob(
            self.container_name, 'blob1', data,
            x_ms_meta_name_values={'hello': 'world', 'number': '42'})

        # Assert
        self.assertIsNone(resp)
        md = self.bs.get_blob_metadata(self.container_name, 'blob1')
        self.assertEqual(md['x-ms-meta-hello'], 'world')
        self.assertEqual(md['x-ms-meta-number'], '42')

    @record
    def test_put_block(self):
        # Arrange
        self._create_container_and_blob(
            self.container_name, 'blob1', b'')

        # Act
        for i in range(5):
            resp = self.bs.put_block(self.container_name,
                                     'blob1',
                                     u'block {0}'.format(i).encode('utf-8'),
                                     str(i))
            self.assertIsNone(resp)

        # Assert

    @record
    def test_put_block_unicode(self):
        # Arrange
        self._create_container_and_blob(
            self.container_name, 'blob1', b'')

        # Act
        with self.assertRaises(TypeError):
            resp = self.bs.put_block(self.container_name, 'blob1', u'??', '1')

        # Assert

    @record
    def test_put_block_list(self):
        # Arrange
        self._create_container_and_blob(
            self.container_name, 'blob1', b'')
        self.bs.put_block(self.container_name, 'blob1', b'AAA', '1')
        self.bs.put_block(self.container_name, 'blob1', b'BBB', '2')
        self.bs.put_block(self.container_name, 'blob1', b'CCC', '3')

        # Act
        resp = self.bs.put_block_list(
            self.container_name, 'blob1', ['1', '2', '3'])

        # Assert
        self.assertIsNone(resp)
        blob = self.bs.get_blob(self.container_name, 'blob1')
        self.assertEqual(blob, b'AAABBBCCC')

    @record
    def test_put_block_list_invalid_block_id(self):
        # Arrange
        self._create_container_and_blob(
            self.container_name, 'blob1', b'')
        self.bs.put_block(self.container_name, 'blob1', b'AAA', '1')
        self.bs.put_block(self.container_name, 'blob1', b'BBB', '2')
        self.bs.put_block(self.container_name, 'blob1', b'CCC', '3')

        # Act
        try:
            resp = self.bs.put_block_list(
                self.container_name, 'blob1', ['1', '2', '4'])
            self.assertTrue(False)
        except AzureHttpError as e:
            self.assertGreaterEqual(
                str(e).find('specified block list is invalid'), 0)

        # Assert

    @record
    def test_get_block_list_no_blocks(self):
        # Arrange
        self._create_container_and_blob(
            self.container_name, 'blob1', b'')

        # Act
        block_list = self.bs.get_block_list(
            self.container_name, 'blob1', None, 'all')

        # Assert
        self.assertIsNotNone(block_list)
        self.assertIsInstance(block_list, BlobBlockList)
        self.assertEqual(len(block_list.uncommitted_blocks), 0)
        self.assertEqual(len(block_list.committed_blocks), 0)

    @record
    def test_get_block_list_uncommitted_blocks(self):
        # Arrange
        self._create_container_and_blob(
            self.container_name, 'blob1', b'')
        self.bs.put_block(self.container_name, 'blob1', b'AAA', '1')
        self.bs.put_block(self.container_name, 'blob1', b'BBB', '2')
        self.bs.put_block(self.container_name, 'blob1', b'CCC', '3')

        # Act
        block_list = self.bs.get_block_list(
            self.container_name, 'blob1', None, 'all')

        # Assert
        self.assertIsNotNone(block_list)
        self.assertIsInstance(block_list, BlobBlockList)
        self.assertEqual(len(block_list.uncommitted_blocks), 3)
        self.assertEqual(len(block_list.committed_blocks), 0)
        self.assertEqual(block_list.uncommitted_blocks[0].id, '1')
        self.assertEqual(block_list.uncommitted_blocks[0].size, 3)
        self.assertEqual(block_list.uncommitted_blocks[1].id, '2')
        self.assertEqual(block_list.uncommitted_blocks[1].size, 3)
        self.assertEqual(block_list.uncommitted_blocks[2].id, '3')
        self.assertEqual(block_list.uncommitted_blocks[2].size, 3)

    @record
    def test_get_block_list_committed_blocks(self):
        # Arrange
        self._create_container_and_blob(
            self.container_name, 'blob1', b'')
        self.bs.put_block(self.container_name, 'blob1', b'AAA', '1')
        self.bs.put_block(self.container_name, 'blob1', b'BBB', '2')
        self.bs.put_block(self.container_name, 'blob1', b'CCC', '3')
        self.bs.put_block_list(self.container_name, 'blob1', ['1', '2', '3'])

        # Act
        block_list = self.bs.get_block_list(
            self.container_name, 'blob1', None, 'all')

        # Assert
        self.assertIsNotNone(block_list)
        self.assertIsInstance(block_list, BlobBlockList)
        self.assertEqual(len(block_list.uncommitted_blocks), 0)
        self.assertEqual(len(block_list.committed_blocks), 3)
        self.assertEqual(block_list.committed_blocks[0].id, '1')
        self.assertEqual(block_list.committed_blocks[0].size, 3)
        self.assertEqual(block_list.committed_blocks[1].id, '2')
        self.assertEqual(block_list.committed_blocks[1].size, 3)
        self.assertEqual(block_list.committed_blocks[2].id, '3')
        self.assertEqual(block_list.committed_blocks[2].size, 3)

    @record
    def test_put_blob_from_bytes(self):
        # Arrange
        self._create_container(self.container_name)

        # Act
        data = b'abcdefghijklmnopqrstuvwxyz'
        resp = self.bs.put_blob_from_bytes(
            self.container_name, 'blob1', data)

        # Assert
        self.assertIsNone(resp)
        self.assertEqual(data, self.bs.get_blob(self.container_name, 'blob1'))

    @record
    def test_put_blob_from_bytes_with_progress(self):
        # Arrange
        self._create_container(self.container_name)
        data = b'abcdefghijklmnopqrstuvwxyz'

        # Act
        progress = []

        def callback(current, total):
            progress.append((current, total))

        resp = self.bs.put_blob_from_bytes(
            self.container_name, 'blob1', data, progress_callback=callback)

        # Assert
        self.assertIsNone(resp)
        self.assertEqual(data, self.bs.get_blob(self.container_name, 'blob1'))
        self.assertEqual(progress, self._get_expected_progress(len(data)))

    @record
    def test_put_blob_from_bytes_with_index(self):
        # Arrange
        self._create_container(self.container_name)

        # Act
        data = b'abcdefghijklmnopqrstuvwxyz'
        resp = self.bs.put_blob_from_bytes(
            self.container_name, 'blob1', data, 3)

        # Assert
        self.assertIsNone(resp)
        self.assertEqual(b'defghijklmnopqrstuvwxyz',
                         self.bs.get_blob(self.container_name, 'blob1'))

    @record
    def test_put_blob_from_bytes_with_index_and_count(self):
        # Arrange
        self._create_container(self.container_name)

        # Act
        data = b'abcdefghijklmnopqrstuvwxyz'
        resp = self.bs.put_blob_from_bytes(
            self.container_name, 'blob1', data, 3, 5)

        # Assert
        self.assertIsNone(resp)
        self.assertEqual(
            b'defgh', self.bs.get_blob(self.container_name, 'blob1'))

    @record
    def test_put_blob_from_bytes_with_index_and_count_and_properties(self):
        # Arrange
        self._create_container(self.container_name)

        # Act
        data = b'abcdefghijklmnopqrstuvwxyz'
        resp = self.bs.put_blob_from_bytes(
            self.container_name, 'blob1', data, 3, 5,
            x_ms_blob_content_type='image/png',
            x_ms_blob_content_language='spanish')

        # Assert
        self.assertIsNone(resp)
        self.assertEqual(
            b'defgh', self.bs.get_blob(self.container_name, 'blob1'))
        props = self.bs.get_blob_properties(self.container_name, 'blob1')
        self.assertEqual(props['content-type'], 'image/png')
        self.assertEqual(props['content-language'], 'spanish')

    @record
    def test_put_blob_from_bytes_chunked_upload(self):
        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_binary_data()

        # Act
        resp = self.bs.put_blob_from_bytes(
            self.container_name, blob_name, data)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, len(data))
        self.assertBlobEqual(self.container_name, blob_name, data)

    def test_put_blob_from_bytes_chunked_upload_parallel(self):
        # parallel tests introduce random order of requests, can only run live
        if TestMode.need_recordingfile(self.test_mode):
            return

        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_binary_data()

        # Act
        resp = self.bs.put_blob_from_bytes(
            self.container_name, blob_name, data,
            max_connections=10)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, len(data))
        self.assertBlobEqual(self.container_name, blob_name, data)

    @record
    def test_put_blob_from_bytes_chunked_upload_with_properties(self):
        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_binary_data()

        # Act
        resp = self.bs.put_blob_from_bytes(
            self.container_name, blob_name, data,
            x_ms_blob_content_type='image/png',
            x_ms_blob_content_language='spanish')

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, len(data))
        self.assertBlobEqual(self.container_name, blob_name, data)
        props = self.bs.get_blob_properties(self.container_name, 'blob1')
        self.assertEqual(props['content-type'], 'image/png')
        self.assertEqual(props['content-language'], 'spanish')

    @record
    def test_put_blob_from_bytes_with_progress_chunked_upload(self):
        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_binary_data()

        # Act
        progress = []

        def callback(current, total):
            progress.append((current, total))

        resp = self.bs.put_blob_from_bytes(
            self.container_name, blob_name, data, progress_callback=callback)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, len(data))
        self.assertBlobEqual(self.container_name, blob_name, data)
        self.assertEqual(progress, self._get_expected_progress(len(data)))

    @record
    def test_put_blob_from_bytes_chunked_upload_with_index_and_count(self):
        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_binary_data()
        index = 33
        blob_size = len(data) - 66

        # Act
        resp = self.bs.put_blob_from_bytes(
            self.container_name, blob_name, data, index, blob_size)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, blob_size)
        self.assertBlobEqual(self.container_name, blob_name,
                             data[index:index + blob_size])

    @record
    def test_put_blob_from_path_chunked_upload(self):
        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_binary_data()
        file_path = 'blob_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)

        # Act
        resp = self.bs.put_blob_from_path(
            self.container_name, blob_name, file_path)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, len(data))
        self.assertBlobEqual(self.container_name, blob_name, data)

    def test_put_blob_from_path_chunked_upload_parallel(self):
        # parallel tests introduce random order of requests, can only run live
        if TestMode.need_recordingfile(self.test_mode):
            return

        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_binary_data()
        file_path = 'blob_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)

        # Act
        resp = self.bs.put_blob_from_path(
            self.container_name, blob_name, file_path,
            max_connections=10)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, len(data))
        self.assertBlobEqual(self.container_name, blob_name, data)

    @record
    def test_put_blob_from_path_with_progress_chunked_upload(self):
        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_binary_data()
        file_path = 'blob_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)

        # Act
        progress = []

        def callback(current, total):
            progress.append((current, total))

        resp = self.bs.put_blob_from_path(
            self.container_name, blob_name, file_path,
            progress_callback=callback)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, len(data))
        self.assertBlobEqual(self.container_name, blob_name, data)
        self.assertEqual(progress, self._get_expected_progress(len(data)))

    @record
    def test_put_blob_from_path_chunked_upload_with_properties(self):
        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_binary_data()
        file_path = 'blob_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)

        # Act
        resp = self.bs.put_blob_from_path(
            self.container_name, blob_name, file_path,
            x_ms_blob_content_type='image/png',
            x_ms_blob_content_language='spanish')

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, len(data))
        self.assertBlobEqual(self.container_name, blob_name, data)
        props = self.bs.get_blob_properties(self.container_name, blob_name)
        self.assertEqual(props['content-type'], 'image/png')
        self.assertEqual(props['content-language'], 'spanish')

    @record
    def test_put_blob_from_file_chunked_upload(self):
        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_binary_data()
        file_path = 'blob_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)

        # Act
        with open(file_path, 'rb') as stream:
            resp = self.bs.put_blob_from_file(
                self.container_name, blob_name, stream)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, len(data))
        self.assertBlobEqual(self.container_name, blob_name, data)

    def test_put_blob_from_file_chunked_upload_parallel(self):
        # parallel tests introduce random order of requests, can only run live
        if TestMode.need_recordingfile(self.test_mode):
            return

        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_binary_data()
        file_path = 'blob_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)

        # Act
        with open(file_path, 'rb') as stream:
            resp = self.bs.put_blob_from_file(
                self.container_name, blob_name, stream,
                max_connections=10)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, len(data))
        self.assertBlobEqual(self.container_name, blob_name, data)

    @record
    def test_put_blob_from_file_non_seekable_chunked_upload_known_size(self):
        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_binary_data()
        file_path = 'blob_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)
        blob_size = len(data) - 66

        # Act
        with open(file_path, 'rb') as stream:
            non_seekable_file = StorageBlockBlobTest.NonSeekableFile(stream)
            resp = self.bs.put_blob_from_file(
                self.container_name, blob_name, non_seekable_file,
                count=blob_size, max_connections=1)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, blob_size)
        self.assertBlobEqual(self.container_name, blob_name, data[:blob_size])

    @record
    def test_put_blob_from_file_non_seekable_chunked_upload_unknown_size(self):
        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_binary_data()
        file_path = 'blob_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)

        # Act
        with open(file_path, 'rb') as stream:
            non_seekable_file = StorageBlockBlobTest.NonSeekableFile(stream)
            resp = self.bs.put_blob_from_file(
                self.container_name, blob_name, non_seekable_file,
                max_connections=1)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, len(data))
        self.assertBlobEqual(self.container_name, blob_name, data)

    def test_put_blob_from_file_non_seekable_chunked_upload_parallel(self):
        # parallel tests introduce random order of requests, can only run live
        if TestMode.need_recordingfile(self.test_mode):
            return

        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_binary_data()
        file_path = 'blob_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)

        # Act
        with open(file_path, 'rb') as stream:
            non_seekable_file = StorageBlockBlobTest.NonSeekableFile(stream)

            # Parallel uploads require that the file be seekable
            with self.assertRaises(AttributeError):
                resp = self.bs.put_blob_from_file(
                    self.container_name, blob_name, non_seekable_file,
                    max_connections=10)

        # Assert

    @record
    def test_put_blob_from_file_with_progress_chunked_upload(self):
        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_binary_data()
        file_path = 'blob_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)

        # Act
        progress = []

        def callback(current, total):
            progress.append((current, total))

        with open(file_path, 'rb') as stream:
            resp = self.bs.put_blob_from_file(
                self.container_name, blob_name, stream,
                progress_callback=callback)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, len(data))
        self.assertBlobEqual(self.container_name, blob_name, data)
        self.assertEqual(
            progress,
            self._get_expected_progress(len(data), unknown_size=True))

    def test_put_blob_from_file_with_progress_chunked_upload_parallel(self):
        # parallel tests introduce random order of requests, can only run live
        if TestMode.need_recordingfile(self.test_mode):
            return

        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_binary_data()
        file_path = 'blob_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)

        # Act
        progress = []

        def callback(current, total):
            progress.append((current, total))

        with open(file_path, 'rb') as stream:
            resp = self.bs.put_blob_from_file(
                self.container_name, blob_name, stream,
                progress_callback=callback,
                max_connections=5)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, len(data))
        self.assertBlobEqual(self.container_name, blob_name, data)
        self.assertEqual(progress, sorted(progress))
        self.assertGreater(len(progress), 0)

    @record
    def test_put_blob_from_file_chunked_upload_with_count(self):
        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_binary_data()
        file_path = 'blob_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)

        # Act
        blob_size = len(data) - 301
        with open(file_path, 'rb') as stream:
            resp = self.bs.put_blob_from_file(
                self.container_name, blob_name, stream, blob_size)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, blob_size)
        self.assertBlobEqual(self.container_name, blob_name, data[:blob_size])

    def test_put_blob_from_file_chunked_upload_with_count_parallel(self):
        # parallel tests introduce random order of requests, can only run live
        if TestMode.need_recordingfile(self.test_mode):
            return

        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_binary_data()
        file_path = 'blob_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)

        # Act
        blob_size = len(data) - 301
        with open(file_path, 'rb') as stream:
            resp = self.bs.put_blob_from_file(
                self.container_name, blob_name, stream, blob_size,
                max_connections=10)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, blob_size)
        self.assertBlobEqual(self.container_name, blob_name, data[:blob_size])

    @record
    def test_put_blob_from_file_chunked_upload_with_count_and_properties(self):
        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_binary_data()
        file_path = 'blob_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)

        # Act
        blob_size = len(data) - 301
        with open(file_path, 'rb') as stream:
            resp = self.bs.put_blob_from_file(
                self.container_name, blob_name, stream, blob_size,
                x_ms_blob_content_type='image/png',
                x_ms_blob_content_language='spanish')

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, blob_size)
        self.assertBlobEqual(self.container_name, blob_name, data[:blob_size])
        props = self.bs.get_blob_properties(self.container_name, blob_name)
        self.assertEqual(props['content-type'], 'image/png')
        self.assertEqual(props['content-language'], 'spanish')

    @record
    def test_put_blob_from_file_chunked_upload_with_properties(self):
        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_binary_data()
        file_path = 'blob_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)

        # Act
        with open(file_path, 'rb') as stream:
            resp = self.bs.put_blob_from_file(
                self.container_name, blob_name, stream,
                x_ms_blob_content_type='image/png',
                x_ms_blob_content_language='spanish')

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, len(data))
        self.assertBlobEqual(self.container_name, blob_name, data)
        props = self.bs.get_blob_properties(self.container_name, blob_name)
        self.assertEqual(props['content-type'], 'image/png')
        self.assertEqual(props['content-language'], 'spanish')

    @record
    def test_put_blob_from_text(self):
        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        text = u'hello ????? world'
        data = text.encode('utf-8')

        # Act
        resp = self.bs.put_blob_from_text(
            self.container_name, blob_name, text)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, len(data))
        self.assertBlobEqual(self.container_name, blob_name, data)

    @record
    def test_put_blob_from_text_with_encoding(self):
        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        text = u'hello ????? world'
        data = text.encode('utf-16')

        # Act
        resp = self.bs.put_blob_from_text(
            self.container_name, blob_name, text, 'utf-16')

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, len(data))
        self.assertBlobEqual(self.container_name, blob_name, data)

    @record
    def test_put_blob_from_text_with_encoding_and_progress(self):
        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        text = u'hello ????? world'
        data = text.encode('utf-16')

        # Act
        progress = []

        def callback(current, total):
            progress.append((current, total))

        resp = self.bs.put_blob_from_text(
            self.container_name, blob_name, text, 'utf-16',
            progress_callback=callback)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, len(data))
        self.assertBlobEqual(self.container_name, blob_name, data)
        self.assertEqual(progress, self._get_expected_progress(len(data)))

    @record
    def test_put_blob_from_text_chunked_upload(self):
        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_text_data()
        encoded_data = data.encode('utf-8')

        # Act
        resp = self.bs.put_blob_from_text(
            self.container_name, blob_name, data)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(
            self.container_name, blob_name, len(encoded_data))
        self.assertBlobEqual(self.container_name, blob_name, encoded_data)

    def test_put_blob_from_text_chunked_upload_parallel(self):
        # parallel tests introduce random order of requests, can only run live
        if TestMode.need_recordingfile(self.test_mode):
            return

        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_text_data()
        encoded_data = data.encode('utf-8')

        # Act
        resp = self.bs.put_blob_from_text(
            self.container_name, blob_name, data,
            max_connections=10)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(
            self.container_name, blob_name, len(encoded_data))
        self.assertBlobEqual(self.container_name, blob_name, encoded_data)

#------------------------------------------------------------------------------
if __name__ == '__main__':
    unittest.main()
