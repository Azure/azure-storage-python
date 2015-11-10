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

from azure.common import AzureMissingResourceHttpError
from azure.storage.blob import (
    AppendBlobService,
    Settings,
)
from tests.common_recordingtestcase import (
    TestMode,
    record,
)
from tests.testcase import StorageTestCase


#------------------------------------------------------------------------------


class StorageAppendBlobTest(StorageTestCase):

    def setUp(self):
        super(StorageAppendBlobTest, self).setUp()

        self.bs = self._create_storage_service(AppendBlobService, self.settings)

        # test chunking functionality by reducing the threshold
        # for chunking and the size of each chunk, otherwise
        # the tests would take too long to execute
        self.bs._BLOB_MAX_DATA_SIZE = 64 * 1024
        self.bs._BLOB_MAX_CHUNK_DATA_SIZE = 4 * 1024

        self.container_name = self.get_resource_name('utcontainer')
        self.container_lease_id = None

    def tearDown(self):
        if not self.is_playback():
            try:
                self.bs.delete_container(self.container_name)
            except:
                pass

        for tmp_file in ['blob_input.temp.dat',
                         'blob_input1.temp.dat',
                         'blob_input2.temp.dat',
                         'blob_output.temp.dat']:
            if os.path.isfile(tmp_file):
                try:
                    os.remove(tmp_file)
                except:
                    pass

        return super(StorageAppendBlobTest, self).tearDown()

    #--Helpers-----------------------------------------------------------------
    def _create_container(self, container_name):
        self.bs.create_container(container_name, None, None, True)

    def _create_container_and_blob(self, container_name, blob_name):
        self._create_container(container_name)
        resp = self.bs.create_blob(container_name, blob_name)
        self.assertIsNone(resp)

    def _blob_exists(self, container_name, blob_name):
        resp = self.bs.list_blobs(container_name)
        for blob in resp:
            if blob.name == blob_name:
                return True
        return False

    def _wait_for_async_copy(self, container_name, blob_name):
        count = 0
        props, _ = self.bs.get_blob_properties(container_name, blob_name)
        while props.copy.status != 'success':
            count = count + 1
            if count > 5:
                self.assertTrue(
                    False, 'Timed out waiting for async copy to complete.')
            self.sleep(5)
            props, _ = self.bs.get_blob_properties(container_name, blob_name)
        self.assertEqual(props.copy.status, 'success')

    def assertBlobEqual(self, container_name, blob_name, expected_data):
        actual_data = self.bs.get_blob(container_name, blob_name)
        self.assertEqual(actual_data, expected_data)

    def assertBlobLengthEqual(self, container_name, blob_name, expected_length):
        props, _ = self.bs.get_blob_properties(container_name, blob_name)
        self.assertEqual(props.content_length, expected_length)

    def _get_oversized_binary_data(self):
        '''Returns random binary data exceeding the size threshold for
        chunking blob upload.'''
        size = self.bs._BLOB_MAX_DATA_SIZE + 12345
        return self._get_random_bytes(size)

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
        words = [u'hello', u'world', u'python', u'啊齄丂狛狜']
        while (len(text) < size):
            index = rand.randint(0, len(words) - 1)
            text = text + u' ' + words[index]

        return text

    def _get_expected_progress(self, blob_size, unknown_size=False):
        result = []
        index = 0
        total = None if unknown_size else blob_size
        while (index < blob_size):
            result.append((index, total))
            index += self.bs._BLOB_MAX_CHUNK_DATA_SIZE
        result.append((blob_size, total))
        return result

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
        resp = self.bs.create_blob(self.container_name, 'blob1')

        # Assert
        self.assertIsNone(resp)

    @record
    def test_put_blob_with_lease_id(self):
        # Arrange
        self._create_container_and_blob(
            self.container_name, 'blob1')
        lease = self.bs.acquire_blob_lease(self.container_name, 'blob1')
        lease_id = lease['x-ms-lease-id']

        # Act
        resp = self.bs.create_blob(
            self.container_name, 'blob1', lease_id=lease_id)

        # Assert
        self.assertIsNone(resp)

    @record
    def test_put_blob_with_metadata(self):
        # Arrange
        self._create_container(self.container_name)

        # Act
        resp = self.bs.create_blob(
            self.container_name, 'blob1',
            metadata={'hello': 'world', 'number': '42'})

        # Assert
        self.assertIsNone(resp)
        md = self.bs.get_blob_metadata(self.container_name, 'blob1')
        self.assertEqual(md['x-ms-meta-hello'], 'world')
        self.assertEqual(md['x-ms-meta-number'], '42')

    @record
    def test_append_block(self):
        # Arrange
        self._create_container_and_blob(self.container_name, 'blob1')

        # Act
        for i in range(5):
            resp = self.bs.append_block(self.container_name,
                                        'blob1',
                                        u'block {0}'.format(i).encode('utf-8'))
            keys = (
                'x-ms-blob-append-offset',
                'x-ms-blob-committed-block-count',
            )
            
            self.assertDictContainsKeys(keys, resp)

        # Assert
        blob = self.bs.get_blob(self.container_name, 'blob1')
        self.assertEqual(b'block 0block 1block 2block 3block 4', blob)

    def assertDictContainsKeys(self, keys, dictionary, msg=None):
        if all (k in dictionary for k in keys):
            return
        
        standardMsg = ''
        if missing:
            standardMsg = 'Missing: some keys were not found in dictionary.'

        self.fail(self._formatMessage(msg, standardMsg))

    @record
    def test_append_block_unicode(self):
        # Arrange
        self._create_container_and_blob(self.container_name, 'blob1')

        # Act
        with self.assertRaises(TypeError):
            resp = self.bs.append_block(self.container_name, 'blob1', u'啊齄丂狛狜')

        # Assert

    @record
    def test_append_blob_from_bytes(self):
        # Arrange
        self._create_container(self.container_name)

        # Act
        data = b'abcdefghijklmnopqrstuvwxyz'
        resp = self.bs.append_blob_from_bytes(
            self.container_name, 'blob1', data,
            maxsize_condition=len(data))

        # Assert
        self.assertIsNone(resp)
        self.assertEqual(data, self.bs.get_blob(self.container_name, 'blob1'))

    @record
    def test_append_blob_from_bytes_with_progress(self):
        # Arrange
        self._create_container(self.container_name)
        data = b'abcdefghijklmnopqrstuvwxyz'

        # Act
        progress = []

        def callback(current, total):
            progress.append((current, total))

        resp = self.bs.append_blob_from_bytes(
            self.container_name, 'blob1', data, progress_callback=callback)

        # Assert
        self.assertIsNone(resp)
        self.assertEqual(data, self.bs.get_blob(self.container_name, 'blob1'))
        self.assertEqual(progress, self._get_expected_progress(len(data)))

    @record
    def test_append_blob_from_bytes_with_index(self):
        # Arrange
        self._create_container(self.container_name)

        # Act
        data = b'abcdefghijklmnopqrstuvwxyz'
        resp = self.bs.append_blob_from_bytes(
            self.container_name, 'blob1', data, 3)

        # Assert
        self.assertIsNone(resp)
        self.assertEqual(b'defghijklmnopqrstuvwxyz',
                         self.bs.get_blob(self.container_name, 'blob1'))

    @record
    def test_append_blob_from_bytes_with_index_and_count(self):
        # Arrange
        self._create_container(self.container_name)

        # Act
        data = b'abcdefghijklmnopqrstuvwxyz'
        resp = self.bs.append_blob_from_bytes(
            self.container_name, 'blob1', data, 3, 5)

        # Assert
        self.assertIsNone(resp)
        self.assertEqual(
            b'defgh', self.bs.get_blob(self.container_name, 'blob1'))

    @record
    def test_append_blob_from_bytes_with_index_and_count_and_properties(self):
        # Arrange
        self._create_container(self.container_name)

        # Act
        data = b'abcdefghijklmnopqrstuvwxyz'
        resp = self.bs.append_blob_from_bytes(
            self.container_name, 'blob1', data, 3, 5,
            settings=Settings(
                content_type='image/png',
                content_language='spanish'))

        # Assert
        self.assertIsNone(resp)
        self.assertEqual(
            b'defgh', self.bs.get_blob(self.container_name, 'blob1'))
        props, _ = self.bs.get_blob_properties(self.container_name, 'blob1')
        self.assertEqual(props.settings.content_type, 'image/png')
        self.assertEqual(props.settings.content_language, 'spanish')

    @record
    def test_append_blob_from_bytes_chunked_upload(self):
        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_binary_data()

        # Act
        resp = self.bs.append_blob_from_bytes(
            self.container_name, blob_name, data)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, len(data))
        self.assertBlobEqual(self.container_name, blob_name, data)

    @record
    def test_append_blob_from_bytes_chunked_upload_with_properties(self):
        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_binary_data()

        # Act
        resp = self.bs.append_blob_from_bytes(
            self.container_name, blob_name, data,
            settings=Settings(
                content_type='image/png',
                content_language='spanish'))

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, len(data))
        self.assertBlobEqual(self.container_name, blob_name, data)
        props, _ = self.bs.get_blob_properties(self.container_name, 'blob1')
        self.assertEqual(props.settings.content_type, 'image/png')
        self.assertEqual(props.settings.content_language, 'spanish')

    @record
    def test_append_blob_from_bytes_with_progress_chunked_upload(self):
        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_binary_data()

        # Act
        progress = []

        def callback(current, total):
            progress.append((current, total))

        resp = self.bs.append_blob_from_bytes(
            self.container_name, blob_name, data, progress_callback=callback)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, len(data))
        self.assertBlobEqual(self.container_name, blob_name, data)
        self.assertEqual(progress, self._get_expected_progress(len(data)))

    @record
    def test_append_blob_from_bytes_chunked_upload_with_index_and_count(self):
        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_binary_data()
        index = 33
        blob_size = len(data) - 66

        # Act
        resp = self.bs.append_blob_from_bytes(
            self.container_name, blob_name, data, index, blob_size)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, blob_size)
        self.assertBlobEqual(self.container_name, blob_name,
                             data[index:index + blob_size])

    @record
    def test_append_blob_from_path_chunked_upload(self):
        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_binary_data()
        file_path = 'blob_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)

        # Act
        resp = self.bs.append_blob_from_path(
            self.container_name, blob_name, file_path)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, len(data))
        self.assertBlobEqual(self.container_name, blob_name, data)

    @record
    def test_append_blob_from_path_with_progress_chunked_upload(self):
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

        resp = self.bs.append_blob_from_path(
            self.container_name, blob_name, file_path,
            progress_callback=callback)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, len(data))
        self.assertBlobEqual(self.container_name, blob_name, data)
        self.assertEqual(progress, self._get_expected_progress(len(data)))

    @record
    def test_append_blob_from_path_chunked_upload_with_properties(self):
        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_binary_data()
        file_path = 'blob_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)

        # Act
        resp = self.bs.append_blob_from_path(
            self.container_name, blob_name, file_path,
            settings=Settings(            
                content_type='image/png',
                content_language='spanish'))

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, len(data))
        self.assertBlobEqual(self.container_name, blob_name, data)
        props, _ = self.bs.get_blob_properties(self.container_name, blob_name)
        self.assertEqual(props.settings.content_type, 'image/png')
        self.assertEqual(props.settings.content_language, 'spanish')

    @record
    def test_append_blob_from_stream_chunked_upload(self):
        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_binary_data()
        file_path = 'blob_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)

        # Act
        with open(file_path, 'rb') as stream:
            resp = self.bs.append_blob_from_stream(
                self.container_name, blob_name, stream)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, len(data))
        self.assertBlobEqual(self.container_name, blob_name, data)

    @record
    def test_append_blob_from_stream_non_seekable_chunked_upload_known_size(self):
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
            non_seekable_file = StorageAppendBlobTest.NonSeekableFile(stream)
            resp = self.bs.append_blob_from_stream(
                self.container_name, blob_name, non_seekable_file,
                count=blob_size)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, blob_size)
        self.assertBlobEqual(self.container_name, blob_name, data[:blob_size])

    @record
    def test_append_blob_from_stream_non_seekable_chunked_upload_unknown_size(self):
        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_binary_data()
        file_path = 'blob_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)

        # Act
        with open(file_path, 'rb') as stream:
            non_seekable_file = StorageAppendBlobTest.NonSeekableFile(stream)
            resp = self.bs.append_blob_from_stream(
                self.container_name, blob_name, non_seekable_file)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, len(data))
        self.assertBlobEqual(self.container_name, blob_name, data)

    @record
    def test_append_blob_from_stream_with_multiple_appends(self):
        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_binary_data()
        file_path1 = 'blob_input1.temp.dat'
        file_path2 = 'blob_input2.temp.dat'
        with open(file_path1, 'wb') as stream1:
            stream1.write(data)
        with open(file_path2, 'wb') as stream2:
            stream2.write(data)

        # Act
        with open(file_path1, 'rb') as stream1:
            self.bs.append_blob_from_stream(
                self.container_name, blob_name, stream1)
        with open(file_path2, 'rb') as stream2:
            resp = self.bs.append_blob_from_stream(
                self.container_name, blob_name, stream2)

        # Assert
        data = data * 2
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, len(data))
        self.assertBlobEqual(self.container_name, blob_name, data)

    @record
    def test_append_blob_from_stream_fail(self):
        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_binary_data()
        file_path = 'blob_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)

        # Act
        with self.assertRaises(AzureMissingResourceHttpError):
            with open(file_path, 'rb') as stream:
                self.bs.append_blob_from_stream(
                    self.container_name, blob_name, stream,
                    create_if_not_exist=False)

        # Assert

    @record
    def test_append_blob_from_stream_chunked_upload_with_count(self):
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
            resp = self.bs.append_blob_from_stream(
                self.container_name, blob_name, stream, blob_size)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, blob_size)
        self.assertBlobEqual(self.container_name, blob_name, data[:blob_size])

    def test_append_blob_from_stream_chunked_upload_with_count_parallel(self):
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
            resp = self.bs.append_blob_from_stream(
                self.container_name, blob_name, stream, blob_size)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, blob_size)
        self.assertBlobEqual(self.container_name, blob_name, data[:blob_size])

    @record
    def test_append_blob_from_stream_chunked_upload_with_properties(self):
        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_binary_data()
        file_path = 'blob_input.temp.dat'
        with open(file_path, 'wb') as stream:
            stream.write(data)

        # Act
        with open(file_path, 'rb') as stream:
            resp = self.bs.append_blob_from_stream(
                self.container_name, blob_name, stream,
                settings=Settings(
                    content_type='image/png',
                    content_language='spanish'))

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, len(data))
        self.assertBlobEqual(self.container_name, blob_name, data)
        props, _ = self.bs.get_blob_properties(self.container_name, blob_name)
        self.assertEqual(props.settings.content_type, 'image/png')
        self.assertEqual(props.settings.content_language, 'spanish')

    @record
    def test_append_blob_from_text(self):
        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        text = u'hello 啊齄丂狛狜 world'
        data = text.encode('utf-8')

        # Act
        resp = self.bs.append_blob_from_text(
            self.container_name, blob_name, text)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, len(data))
        self.assertBlobEqual(self.container_name, blob_name, data)

    @record
    def test_append_blob_from_text_with_encoding(self):
        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        text = u'hello 啊齄丂狛狜 world'
        data = text.encode('utf-16')

        # Act
        resp = self.bs.append_blob_from_text(
            self.container_name, blob_name, text, 'utf-16')

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, len(data))
        self.assertBlobEqual(self.container_name, blob_name, data)

    @record
    def test_append_blob_from_text_with_encoding_and_progress(self):
        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        text = u'hello 啊齄丂狛狜 world'
        data = text.encode('utf-16')

        # Act
        progress = []

        def callback(current, total):
            progress.append((current, total))

        resp = self.bs.append_blob_from_text(
            self.container_name, blob_name, text, 'utf-16',
            progress_callback=callback)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(self.container_name, blob_name, len(data))
        self.assertBlobEqual(self.container_name, blob_name, data)
        self.assertEqual(progress, self._get_expected_progress(len(data)))

    @record
    def test_append_blob_from_text_chunked_upload(self):
        # Arrange
        self._create_container(self.container_name)
        blob_name = 'blob1'
        data = self._get_oversized_text_data()
        encoded_data = data.encode('utf-8')

        # Act
        resp = self.bs.append_blob_from_text(
            self.container_name, blob_name, data)

        # Assert
        self.assertIsNone(resp)
        self.assertBlobLengthEqual(
            self.container_name, blob_name, len(encoded_data))
        self.assertBlobEqual(self.container_name, blob_name, encoded_data)

#------------------------------------------------------------------------------
if __name__ == '__main__':
    unittest.main()
