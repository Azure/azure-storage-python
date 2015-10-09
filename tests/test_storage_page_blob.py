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
    PageBlobService,
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


class StoragePageBlobTest(StorageTestCase):

    def setUp(self):
        super(StoragePageBlobTest, self).setUp()

        self.bs = self._create_storage_service(PageBlobService, self.settings)

        if self.settings.REMOTE_STORAGE_ACCOUNT_NAME and self.settings.REMOTE_STORAGE_ACCOUNT_KEY:
            self.bs2 = self._create_storage_service(
                PageBlobService,
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

        return super(StoragePageBlobTest, self).tearDown()

    #--Helpers-----------------------------------------------------------------
    def _create_container(self, container_name):
        self.bs.create_container(container_name, None, None, True)

    def _create_container_and_blob(self, container_name, blob_name,
                                   content_length):
        self._create_container(container_name)
        resp = self.bs.put_blob(self.container_name, blob_name,
                                x_ms_blob_content_length=str(content_length))
        self.assertIsNone(resp)

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

    def _get_oversized_page_blob_binary_data(self):
        '''Returns random binary data exceeding the size threshold for
        chunking blob upload.'''
        size = self.bs._BLOB_MAX_DATA_SIZE + 16384
        return self._get_random_bytes(size)

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

    #--Test cases for page blobs --------------------------------------------

    @record
    def test_put_blob(self):
        # Arrange
        self._create_container(self.container_name)

        # Act
        resp = self.bs.put_blob(self.container_name, 'blob1',
                                '1024')

        # Assert
        self.assertIsNone(resp)

    @record
    def test_put_page_with_lease_id(self):
        # Arrange
        self._create_container_and_blob(
            self.container_name, 'blob1', 512)
        lease = self.bs.acquire_blob_lease(self.container_name, 'blob1')
        lease_id = lease['x-ms-lease-id']

        # Act        
        data = b'abcdefghijklmnop' * 32
        resp = self.bs.put_page(
            self.container_name, 'blob1', data, 'bytes=0-511', 'update',
            x_ms_lease_id=lease_id)

        # Assert
        blob = self.bs.get_blob(
            self.container_name, 'blob1', x_ms_lease_id=lease_id)
        self.assertEqual(blob, data)

    @record
    def test_put_blob_with_metadata(self):
        # Arrange
        self._create_container(self.container_name)

        # Act
        data = b'hello world'
        resp = self.bs.put_blob(
            self.container_name, 'blob1', 512,
            x_ms_meta_name_values={'hello': 'world', 'number': '42'})

        # Assert
        self.assertIsNone(resp)
        md = self.bs.get_blob_metadata(self.container_name, 'blob1')
        self.assertEqual(md['x-ms-meta-hello'], 'world')
        self.assertEqual(md['x-ms-meta-number'], '42')

    @record
    def test_put_page_update(self):
        # Arrange
        self._create_container_and_blob(
            self.container_name, 'blob1', 1024)

        # Act
        data = b'abcdefghijklmnop' * 32
        resp = self.bs.put_page(
            self.container_name, 'blob1', data, 'bytes=0-511', 'update')

        # Assert
        self.assertIsNone(resp)

    @record
    def test_put_page_clear(self):
        # Arrange
        self._create_container_and_blob(
            self.container_name, 'blob1', 1024)

        # Act
        resp = self.bs.put_page(
            self.container_name, 'blob1', b'', 'bytes=0-511', 'clear')

        # Assert
        self.assertIsNone(resp)

    @record
    def test_put_page_if_sequence_number_lt_success(self):
        # Arrange
        self._create_container(self.container_name)
        data = b'ab' * 256
        start_sequence = 10
        self.bs.put_blob(self.container_name, 'blob1', 512,
                         x_ms_blob_sequence_number=start_sequence)

        # Act
        self.bs.put_page(self.container_name, 'blob1', data, 'bytes=0-511',
                         'update',
                         x_ms_if_sequence_number_lt=start_sequence + 1)

        # Assert
        self.assertBlobEqual(self.container_name, 'blob1', data)

    @record
    def test_put_page_if_sequence_number_lt_failure(self):
        # Arrange
        self._create_container(self.container_name)
        data = b'ab' * 256
        start_sequence = 10
        self.bs.put_blob(self.container_name, 'blob1', 512,
                         x_ms_blob_sequence_number=start_sequence)

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.put_page(self.container_name, 'blob1', data, 'bytes=0-511',
                             'update',
                             x_ms_if_sequence_number_lt=start_sequence)

        # Assert

    @record
    def test_put_page_if_sequence_number_lte_success(self):
        # Arrange
        self._create_container(self.container_name)
        data = b'ab' * 256
        start_sequence = 10
        self.bs.put_blob(self.container_name, 'blob1', 512,
                         x_ms_blob_sequence_number=start_sequence)

        # Act
        self.bs.put_page(self.container_name, 'blob1', data, 'bytes=0-511',
                         'update', x_ms_if_sequence_number_lte=start_sequence)

        # Assert
        self.assertBlobEqual(self.container_name, 'blob1', data)

    @record
    def test_put_page_if_sequence_number_lte_failure(self):
        # Arrange
        self._create_container(self.container_name)
        data = b'ab' * 256
        start_sequence = 10
        self.bs.put_blob(self.container_name, 'blob1', 512,
                         x_ms_blob_sequence_number=start_sequence)

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.put_page(self.container_name, 'blob1', data, 'bytes=0-511',
                             'update',
                             x_ms_if_sequence_number_lte=start_sequence - 1)

        # Assert

    @record
    def test_put_page_if_sequence_number_eq_success(self):
        # Arrange
        self._create_container(self.container_name)
        data = b'ab' * 256
        start_sequence = 10
        self.bs.put_blob(self.container_name, 'blob1', 512,
                         x_ms_blob_sequence_number=start_sequence)

        # Act
        self.bs.put_page(self.container_name, 'blob1', data, 'bytes=0-511',
                         'update', x_ms_if_sequence_number_eq=start_sequence)

        # Assert
        self.assertBlobEqual(self.container_name, 'blob1', data)

    @record
    def test_put_page_if_sequence_number_eq_failure(self):
        # Arrange
        self._create_container(self.container_name)
        data = b'ab' * 256
        start_sequence = 10
        self.bs.put_blob(self.container_name, 'blob1', 512,
                         x_ms_blob_sequence_number=start_sequence)

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.put_page(self.container_name, 'blob1', data, 'bytes=0-511',
                             'update',
                             x_ms_if_sequence_number_eq=start_sequence - 1)

        # Assert

    @record
    def test_put_page_unicode(self):
        # Arrange
        self._create_container_and_blob(self.container_name, 'blob1', 512)

        # Act
        data = u'abcdefghijklmnop' * 32
        with self.assertRaises(TypeError):
            self.bs.put_page(self.container_name, 'blob1',
                             data, 'bytes=0-511', 'update')

        # Assert

    @record
    def test_get_page_ranges_no_pages(self):
        # Arrange
        self._create_container_and_blob(
            self.container_name, 'blob1', 1024)

        # Act
        ranges = self.bs.get_page_ranges(self.container_name, 'blob1')

        # Assert
        self.assertIsNotNone(ranges)
        self.assertIsInstance(ranges, PageList)
        self.assertEqual(len(ranges.page_ranges), 0)

    @record
    def test_get_page_ranges_2_pages(self):
        # Arrange
        self._create_container_and_blob(
            self.container_name, 'blob1', 2048)
        data = b'abcdefghijklmnop' * 32
        resp1 = self.bs.put_page(
            self.container_name, 'blob1', data, 'bytes=0-511', 'update')
        resp2 = self.bs.put_page(
            self.container_name, 'blob1', data, 'bytes=1024-1535', 'update')

        # Act
        ranges = self.bs.get_page_ranges(self.container_name, 'blob1')

        # Assert
        self.assertIsNotNone(ranges)
        self.assertIsInstance(ranges, PageList)
        self.assertEqual(len(ranges.page_ranges), 2)
        self.assertEqual(ranges.page_ranges[0].start, 0)
        self.assertEqual(ranges.page_ranges[0].end, 511)
        self.assertEqual(ranges.page_ranges[1].start, 1024)
        self.assertEqual(ranges.page_ranges[1].end, 1535)

    @record
    def test_get_page_ranges_iter(self):
        # Arrange
        self._create_container_and_blob(
            self.container_name, 'blob1', 2048)
        data = b'abcdefghijklmnop' * 32
        resp1 = self.bs.put_page(
            self.container_name, 'blob1', data, 'bytes=0-511', 'update')
        resp2 = self.bs.put_page(
            self.container_name, 'blob1', data, 'bytes=1024-1535', 'update')

        # Act
        ranges = self.bs.get_page_ranges(self.container_name, 'blob1')
        for range in ranges:
            pass

        # Assert
        self.assertEqual(len(ranges), 2)
        self.assertIsInstance(ranges[0], PageRange)
        self.assertIsInstance(ranges[1], PageRange)

    @record
    def test_put_blob_from_bytes(self):
        # Arrange
        self._create_container(self.container_name)

        # Act
        data = self._get_random_bytes(2048)
        resp = self.bs.put_blob_from_bytes(
            self.container_name, 'blob1', data)

        # Assert
        self.assertIsNone(resp)
        self.assertEqual(data, self.bs.get_blob(self.container_name, 'blob1'))

    @record
    def test_put_blob_from_bytes_with_progress(self):
        # Arrange
        self._create_container(self.container_name)

        # Act
        progress = []

        def callback(current, total):
            progress.append((current, total))

        data = self._get_random_bytes(2048)
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
        index = 1024

        # Act
        data = self._get_random_bytes(2048)
        resp = self.bs.put_blob_from_bytes(
            self.container_name, 'blob1', data, index)

        # Assert
        self.assertIsNone(resp)
        self.assertEqual(data[index:],
                         self.bs.get_blob(self.container_name, 'blob1'))

    @record
    def test_put_blob_from_bytes_with_index_and_count(self):
        # Arrange
        self._create_container(self.container_name)
        index = 512
        count = 1024

        # Act
        data = self._get_random_bytes(2048)
        resp = self.bs.put_blob_from_bytes(
            self.container_name, 'blob1', data, index, count)

        # Assert
        self.assertIsNone(resp)
        self.assertEqual(data[index:index + count],
                         self.bs.get_blob(self.container_name, 'blob1'))

    @record
    def test_put_blob_from_bytes_chunked_upload(self):
        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_page_blob_binary_data()

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
        data = self._get_oversized_page_blob_binary_data()

        # Act
        resp = self.bs.put_blob_from_bytes(
            self.container_name, blob_name, data,
            max_connections=10)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, len(data))
        self.assertBlobEqual(self.container_name, blob_name, data)

    @record
    def test_put_blob_from_bytes_chunked_upload_with_index_and_count(self):
        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_page_blob_binary_data()
        index = 512
        count = len(data) - 1024

        # Act
        resp = self.bs.put_blob_from_bytes(
            self.container_name, blob_name, data, index, count)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, count)
        self.assertBlobEqual(self.container_name,
                             blob_name, data[index:index + count])

    def test_put_blob_from_bytes_chunked_upload_with_index_and_count_parallel(self):
        # parallel tests introduce random order of requests, can only run live
        if TestMode.need_recordingfile(self.test_mode):
            return

        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_page_blob_binary_data()
        index = 512
        count = len(data) - 1024

        # Act
        resp = self.bs.put_blob_from_bytes(
            self.container_name, blob_name, data, index, count,
            max_connections=10)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, count)
        self.assertBlobEqual(self.container_name,
                             blob_name, data[index:index + count])

    @record
    def test_put_blob_from_path_chunked_upload(self):
        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_page_blob_binary_data()
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
        data = self._get_oversized_page_blob_binary_data()
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
        data = self._get_oversized_page_blob_binary_data()
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
    def test_put_blob_from_file_chunked_upload(self):
        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_page_blob_binary_data()
        file_path = 'blob_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)

        # Act
        blob_size = len(data)
        with open(file_path, 'rb') as stream:
            resp = self.bs.put_blob_from_file(
                self.container_name, blob_name, stream, blob_size)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, blob_size)
        self.assertBlobEqual(self.container_name, blob_name, data[:blob_size])

    def test_put_blob_from_file_chunked_upload_parallel(self):
        # parallel tests introduce random order of requests, can only run live
        if TestMode.need_recordingfile(self.test_mode):
            return

        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_page_blob_binary_data()
        file_path = 'blob_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)

        # Act
        blob_size = len(data)
        with open(file_path, 'rb') as stream:
            resp = self.bs.put_blob_from_file(
                self.container_name, blob_name, stream, blob_size,
                max_connections=10)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, blob_size)
        self.assertBlobEqual(self.container_name, blob_name, data[:blob_size])

    @record
    def test_put_blob_from_file_non_seekable_chunked_upload(self):
        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_page_blob_binary_data()
        file_path = 'blob_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)

        # Act
        blob_size = len(data)
        with open(file_path, 'rb') as stream:
            non_seekable_file = StoragePageBlobTest.NonSeekableFile(stream)
            resp = self.bs.put_blob_from_file(
                self.container_name, blob_name, non_seekable_file, blob_size,
                max_connections=1)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, blob_size)
        self.assertBlobEqual(self.container_name, blob_name, data[:blob_size])

    def test_put_blob_from_file_non_seekable_chunked_upload_parallel(self):
        # parallel tests introduce random order of requests, can only run live
        if TestMode.need_recordingfile(self.test_mode):
            return

        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_page_blob_binary_data()
        file_path = 'blob_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)

        # Act
        blob_size = len(data)
        with open(file_path, 'rb') as stream:
            non_seekable_file = StoragePageBlobTest.NonSeekableFile(stream)

            # Parallel uploads require that the file be seekable
            with self.assertRaises(AttributeError):
                resp = self.bs.put_blob_from_file(
                    self.container_name, blob_name, non_seekable_file, blob_size,
                    max_connections=10)

        # Assert

    @record
    def test_put_blob_from_file_with_progress_chunked_upload(self):
        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_page_blob_binary_data()
        file_path = 'blob_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)

        # Act
        progress = []

        def callback(current, total):
            progress.append((current, total))

        blob_size = len(data)
        with open(file_path, 'rb') as stream:
            resp = self.bs.put_blob_from_file(
                self.container_name, blob_name, stream, blob_size,
                progress_callback=callback)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, blob_size)
        self.assertBlobEqual(self.container_name, blob_name, data[:blob_size])
        self.assertEqual(progress, self._get_expected_progress(len(data)))

    def test_put_blob_from_file_with_progress_chunked_upload_parallel(self):
        # parallel tests introduce random order of requests, can only run live
        if TestMode.need_recordingfile(self.test_mode):
            return

        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_page_blob_binary_data()
        file_path = 'blob_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)

        # Act
        progress = []

        def callback(current, total):
            progress.append((current, total))

        blob_size = len(data)
        with open(file_path, 'rb') as stream:
            resp = self.bs.put_blob_from_file(
                self.container_name, blob_name, stream, blob_size,
                progress_callback=callback,
                max_connections=5)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, blob_size)
        self.assertBlobEqual(self.container_name, blob_name, data[:blob_size])
        self.assertEqual(progress, sorted(progress))
        self.assertGreater(len(progress), 0)

    @record
    def test_put_blob_from_file_chunked_upload_truncated(self):
        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_page_blob_binary_data()
        file_path = 'blob_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)

        # Act
        blob_size = len(data) - 512
        with open(file_path, 'rb') as stream:
            resp = self.bs.put_blob_from_file(
                self.container_name, blob_name, stream, blob_size)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, blob_size)
        self.assertBlobEqual(self.container_name, blob_name, data[:blob_size])

    def test_put_blob_from_file_chunked_upload_truncated_parallel(self):
        # parallel tests introduce random order of requests, can only run live
        if TestMode.need_recordingfile(self.test_mode):
            return

        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_page_blob_binary_data()
        file_path = 'blob_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)

        # Act
        blob_size = len(data) - 512
        with open(file_path, 'rb') as stream:
            resp = self.bs.put_blob_from_file(
                self.container_name, blob_name, stream, blob_size,
                max_connections=10)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, blob_size)
        self.assertBlobEqual(self.container_name, blob_name, data[:blob_size])

    @record
    def test_put_blob_from_file_with_progress_chunked_upload_truncated(self):
        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_page_blob_binary_data()
        file_path = 'blob_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)

        # Act
        progress = []

        def callback(current, total):
            progress.append((current, total))

        blob_size = len(data) - 512
        with open(file_path, 'rb') as stream:
            resp = self.bs.put_blob_from_file(
                self.container_name, blob_name, stream, blob_size,
                progress_callback=callback)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, blob_size)
        self.assertBlobEqual(self.container_name, blob_name, data[:blob_size])
        self.assertEqual(progress, self._get_expected_progress(blob_size))


#------------------------------------------------------------------------------
if __name__ == '__main__':
    unittest.main()
