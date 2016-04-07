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
import unittest

from azure.storage.blob import (
    Blob,
    BlockBlobService,
)
from tests.testcase import (
    StorageTestCase,
    TestMode,
    record,
)

#------------------------------------------------------------------------------
TEST_BLOB_PREFIX = 'blob'
FILE_PATH = 'blob_output.temp.dat'
#------------------------------------------------------------------------------

class StorageGetBlobTest(StorageTestCase):

    def setUp(self):
        super(StorageGetBlobTest, self).setUp()

        self.bs = self._create_storage_service(BlockBlobService, self.settings)

        self.container_name = self.get_resource_name('utcontainer')

        if not self.is_playback():
            self.bs.create_container(self.container_name)

        self.byte_blob = self.get_resource_name('byteblob')
        self.byte_data = self.get_random_bytes(64 * 1024 + 1)

        if not self.is_playback():
            self.bs.create_blob_from_bytes(self.container_name, self.byte_blob, self.byte_data)

        # test chunking functionality by reducing the threshold
        # for chunking and the size of each chunk, otherwise
        # the tests would take too long to execute
        self.bs.MAX_SINGLE_GET_SIZE = 64 * 1024
        self.bs.MAX_CHUNK_GET_SIZE = 4 * 1024

    def tearDown(self):
        if not self.is_playback():
            try:
                self.bs.delete_container(self.container_name)
            except:
                pass

        if os.path.isfile(FILE_PATH):
            try:
                os.remove(FILE_PATH)
            except:
                pass

        return super(StorageGetBlobTest, self).tearDown()

    #--Helpers-----------------------------------------------------------------

    def _get_blob_reference(self):
        return self.get_resource_name(TEST_BLOB_PREFIX)

    class NonSeekableFile(object):
        def __init__(self, wrapped_file):
            self.wrapped_file = wrapped_file

        def write(self, data):
            self.wrapped_file.write(data)

        def read(self, count):
            return self.wrapped_file.read(count)

    #-- Get test cases for blobs ----------------------------------------------

    @record
    def test_unicode_get_blob_unicode_data(self):
        # Arrange
        blob_data = u'hello world啊齄丂狛狜'.encode('utf-8')
        blob_name = self._get_blob_reference()
        self.bs.create_blob_from_bytes(self.container_name, blob_name, blob_data)

        # Act
        blob = self.bs.get_blob_to_bytes(self.container_name, blob_name)

        # Assert
        self.assertIsInstance(blob, Blob)
        self.assertEqual(blob.content, blob_data)

    @record
    def test_unicode_get_blob_binary_data(self):
        # Arrange
        base64_data = 'AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+P0BBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWltcXV5fYGFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6e3x9fn+AgYKDhIWGh4iJiouMjY6PkJGSk5SVlpeYmZqbnJ2en6ChoqOkpaanqKmqq6ytrq+wsbKztLW2t7i5uru8vb6/wMHCw8TFxsfIycrLzM3Oz9DR0tPU1dbX2Nna29zd3t/g4eLj5OXm5+jp6uvs7e7v8PHy8/T19vf4+fr7/P3+/wABAgMEBQYHCAkKCwwNDg8QERITFBUWFxgZGhscHR4fICEiIyQlJicoKSorLC0uLzAxMjM0NTY3ODk6Ozw9Pj9AQUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVpbXF1eX2BhYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ent8fX5/gIGCg4SFhoeIiYqLjI2Oj5CRkpOUlZaXmJmam5ydnp+goaKjpKWmp6ipqqusra6vsLGys7S1tre4ubq7vL2+v8DBwsPExcbHyMnKy8zNzs/Q0dLT1NXW19jZ2tvc3d7f4OHi4+Tl5ufo6err7O3u7/Dx8vP09fb3+Pn6+/z9/v8AAQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyAhIiMkJSYnKCkqKywtLi8wMTIzNDU2Nzg5Ojs8PT4/QEFCQ0RFRkdISUpLTE1OT1BRUlNUVVZXWFlaW1xdXl9gYWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXp7fH1+f4CBgoOEhYaHiImKi4yNjo+QkZKTlJWWl5iZmpucnZ6foKGio6SlpqeoqaqrrK2ur7CxsrO0tba3uLm6u7y9vr/AwcLDxMXGx8jJysvMzc7P0NHS09TV1tfY2drb3N3e3+Dh4uPk5ebn6Onq6+zt7u/w8fLz9PX29/j5+vv8/f7/AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+P0BBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWltcXV5fYGFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6e3x9fn+AgYKDhIWGh4iJiouMjY6PkJGSk5SVlpeYmZqbnJ2en6ChoqOkpaanqKmqq6ytrq+wsbKztLW2t7i5uru8vb6/wMHCw8TFxsfIycrLzM3Oz9DR0tPU1dbX2Nna29zd3t/g4eLj5OXm5+jp6uvs7e7v8PHy8/T19vf4+fr7/P3+/w=='       
        binary_data = base64.b64decode(base64_data)

        blob_name = self._get_blob_reference()
        self.bs.create_blob_from_bytes(self.container_name, blob_name, binary_data)

        # Act
        blob = self.bs.get_blob_to_bytes(self.container_name, blob_name)

        # Assert
        self.assertIsInstance(blob, Blob)
        self.assertEqual(blob.content, binary_data)

    @record
    def test_get_blob_to_bytes(self):
        # Arrange

        # Act
        blob = self.bs.get_blob_to_bytes(self.container_name, self.byte_blob)

        # Assert
        self.assertEqual(self.byte_data, blob.content)

    def test_get_blob_to_bytes_parallel(self):
        # parallel tests introduce random order of requests, can only run live
        if TestMode.need_recording_file(self.test_mode):
            return

        # Arrange

        # Act
        blob = self.bs.get_blob_to_bytes(self.container_name, self.byte_blob, max_connections=2)

        # Assert
        self.assertEqual(self.byte_data, blob.content)

    @record
    def test_get_blob_to_bytes_with_progress(self):
        # Arrange

        # Act
        progress = []

        def callback(current, total):
            progress.append((current, total))

        blob = self.bs.get_blob_to_bytes(self.container_name, self.byte_blob, progress_callback=callback)

        # Assert
        self.assertEqual(self.byte_data, blob.content)
        self.assert_download_progress(len(self.byte_data), self.bs.MAX_CHUNK_GET_SIZE, progress)

    @record
    def test_get_blob_to_bytes_with_progress_parallel(self):
        # parallel tests introduce random order of requests, can only run live
        if TestMode.need_recording_file(self.test_mode):
            return

        # Arrange

        # Act
        progress = []

        def callback(current, total):
            progress.append((current, total))

        blob = self.bs.get_blob_to_bytes(self.container_name, self.byte_blob, progress_callback=callback, max_connections=2)

        # Assert
        self.assertEqual(self.byte_data, blob.content)
        self.assert_download_progress(len(self.byte_data), self.bs.MAX_CHUNK_GET_SIZE, progress, single_download=False)

    @record
    def test_get_blob_to_stream(self):
        # Arrange

        # Act
        with open(FILE_PATH, 'wb') as stream:
            blob = self.bs.get_blob_to_stream(
                self.container_name, self.byte_blob, stream)

        # Assert
        self.assertIsInstance(blob, Blob)
        with open(FILE_PATH, 'rb') as stream:
            actual = stream.read()
            self.assertEqual(self.byte_data, actual)

    def test_get_blob_to_stream_parallel(self):
        # parallel tests introduce random order of requests, can only run live
        if TestMode.need_recording_file(self.test_mode):
            return

        # Arrange

        # Act
        with open(FILE_PATH, 'wb') as stream:
            blob = self.bs.get_blob_to_stream(
                self.container_name, self.byte_blob, stream, max_connections=2)

        # Assert
        self.assertIsInstance(blob, Blob)
        with open(FILE_PATH, 'rb') as stream:
            actual = stream.read()
            self.assertEqual(self.byte_data, actual)

    def test_get_blob_to_stream_non_seekable(self):
        # parallel tests introduce random order of requests, can only run live
        if TestMode.need_recording_file(self.test_mode):
            return

        # Arrange

        # Act
        with open(FILE_PATH, 'wb') as stream:
            non_seekable_stream = StorageGetBlobTest.NonSeekableFile(stream)
            blob = self.bs.get_blob_to_stream(self.container_name, self.byte_blob, non_seekable_stream)

        # Assert
        self.assertIsInstance(blob, Blob)
        with open(FILE_PATH, 'rb') as stream:
            actual = stream.read()
            self.assertEqual(self.byte_data, actual)

    def test_get_blob_to_stream_non_seekable_parallel(self):
        # parallel tests introduce random order of requests, can only run live
        if TestMode.need_recording_file(self.test_mode):
            return

        # Arrange

        # Act
        with open(FILE_PATH, 'wb') as stream:
            non_seekable_stream = StorageGetBlobTest.NonSeekableFile(stream)

            with self.assertRaises(BaseException):
                blob = self.bs.get_blob_to_stream(
                    self.container_name, self.byte_blob, non_seekable_stream, max_connections=2)

        # Assert

    @record
    def test_get_blob_to_stream_with_progress(self):
        # Arrange

        # Act
        progress = []

        def callback(current, total):
            progress.append((current, total))

        with open(FILE_PATH, 'wb') as stream:
            blob = self.bs.get_blob_to_stream(
                self.container_name, self.byte_blob, stream, progress_callback=callback)

        # Assert
        self.assertIsInstance(blob, Blob)
        with open(FILE_PATH, 'rb') as stream:
            actual = stream.read()
            self.assertEqual(self.byte_data, actual)
        self.assert_download_progress(len(self.byte_data), self.bs.MAX_CHUNK_GET_SIZE, progress)

    def test_get_blob_to_stream_with_progress_parallel(self):
        # parallel tests introduce random order of requests, can only run live
        if TestMode.need_recording_file(self.test_mode):
            return

        # Arrange

        # Act
        progress = []

        def callback(current, total):
            progress.append((current, total))

        with open(FILE_PATH, 'wb') as stream:
            blob = self.bs.get_blob_to_stream(
                self.container_name, self.byte_blob, stream,
                progress_callback=callback,
                max_connections=5)

        # Assert
        self.assertIsInstance(blob, Blob)
        with open(FILE_PATH, 'rb') as stream:
            actual = stream.read()
            self.assertEqual(self.byte_data, actual)
        self.assert_download_progress(len(self.byte_data), self.bs.MAX_CHUNK_GET_SIZE, progress, single_download=False)

    @record
    def test_get_blob_to_path(self):
        # Arrange

        # Act
        blob = self.bs.get_blob_to_path(self.container_name, self.byte_blob, FILE_PATH)

        # Assert
        self.assertIsInstance(blob, Blob)
        with open(FILE_PATH, 'rb') as stream:
            actual = stream.read()
            self.assertEqual(self.byte_data, actual)

    def test_get_blob_to_path_parallel(self):
        # parallel tests introduce random order of requests, can only run live
        if TestMode.need_recording_file(self.test_mode):
            return

        # Arrange

        # Act
        blob = self.bs.get_blob_to_path(
            self.container_name, self.byte_blob, FILE_PATH, max_connections=5)

        # Assert
        self.assertIsInstance(blob, Blob)
        with open(FILE_PATH, 'rb') as stream:
            actual = stream.read()
            self.assertEqual(self.byte_data, actual)

    def test_ranged_get_blob_to_path(self):
        # parallel tests introduce random order of requests, can only run live
        if TestMode.need_recording_file(self.test_mode):
            return

        # Arrange

        # Act
        blob = self.bs.get_blob_to_path(
            self.container_name, self.byte_blob, FILE_PATH, start_range=1, end_range=3,
            range_get_content_md5=True, max_connections=5)

        # Assert
        self.assertIsInstance(blob, Blob)
        with open(FILE_PATH, 'rb') as stream:
            actual = stream.read()
            self.assertEqual(self.byte_data[1:4], actual)

    def test_ranged_get_blob_to_path_parallel(self):
        # parallel tests introduce random order of requests, can only run live
        if TestMode.need_recording_file(self.test_mode):
            return

        # Arrange

        # Act
        blob = self.bs.get_blob_to_path(
            self.container_name, self.byte_blob, FILE_PATH, start_range=0,
            max_connections=5)

        # Assert
        self.assertIsInstance(blob, Blob)
        with open(FILE_PATH, 'rb') as stream:
            actual = stream.read()
            self.assertEqual(self.byte_data, actual)

    def test_ranged_get_blob_to_path_md5_without_end_range_fail(self):
        # parallel tests introduce random order of requests, can only run live
        if TestMode.need_recording_file(self.test_mode):
            return

        # Arrange

        # Act
        with self.assertRaises(ValueError):
            blob = self.bs.get_blob_to_path(
                self.container_name, self.byte_blob, FILE_PATH, start_range=1,
                range_get_content_md5=True, max_connections=5)

        # Assert

    @record
    def test_get_blob_to_path_with_progress(self):
        # Arrange

        # Act
        progress = []

        def callback(current, total):
            progress.append((current, total))

        blob = self.bs.get_blob_to_path(
            self.container_name, self.byte_blob, FILE_PATH, progress_callback=callback)

        # Assert
        self.assertIsInstance(blob, Blob)
        with open(FILE_PATH, 'rb') as stream:
            actual = stream.read()
            self.assertEqual(self.byte_data, actual)
        self.assert_download_progress(len(self.byte_data), self.bs.MAX_CHUNK_GET_SIZE, progress)

    @record
    def test_get_blob_to_path_with_progress_parallel(self):
        # parallel tests introduce random order of requests, can only run live
        if TestMode.need_recording_file(self.test_mode):
            return

        # Arrange

        # Act
        progress = []

        def callback(current, total):
            progress.append((current, total))

        blob = self.bs.get_blob_to_path(
            self.container_name, self.byte_blob, FILE_PATH,
            progress_callback=callback, max_connections=2)

        # Assert
        self.assertIsInstance(blob, Blob)
        with open(FILE_PATH, 'rb') as stream:
            actual = stream.read()
            self.assertEqual(self.byte_data, actual)
        self.assert_download_progress(len(self.byte_data), self.bs.MAX_CHUNK_GET_SIZE, progress, single_download=False)

    @record
    def test_get_blob_to_path_with_mode(self):
        # Arrange
        with open(FILE_PATH, 'wb') as stream:
            stream.write(b'abcdef')

        # Act
        blob = self.bs.get_blob_to_path(
            self.container_name, self.byte_blob, FILE_PATH, 'a+b')

        # Assert
        self.assertIsInstance(blob, Blob)
        with open(FILE_PATH, 'rb') as stream:
            actual = stream.read()
            self.assertEqual(b'abcdef' + self.byte_data, actual)

    @record
    def test_get_blob_to_path_with_mode_parallel(self):
        # Arrange
        with open(FILE_PATH, 'wb') as stream:
            stream.write(b'abcdef')

        # Act
        blob = self.bs.get_blob_to_path(
            self.container_name, self.byte_blob, FILE_PATH, 'a+b')

        # Assert
        self.assertIsInstance(blob, Blob)
        with open(FILE_PATH, 'rb') as stream:
            actual = stream.read()
            self.assertEqual(b'abcdef' + self.byte_data, actual)

    @record
    def test_get_blob_to_text(self):
        # Arrange
        text_blob = self.get_resource_name('textblob')
        text_data = self.get_random_text_data(self.bs.MAX_SINGLE_GET_SIZE + 1)
        self.bs.create_blob_from_text(self.container_name, text_blob, text_data)

        # Act
        blob = self.bs.get_blob_to_text(self.container_name, text_blob)

        # Assert
        self.assertEqual(text_data, blob.content)

    def test_get_blob_to_text_parallel(self):
        # parallel tests introduce random order of requests, can only run live
        if TestMode.need_recording_file(self.test_mode):
            return

        # Arrange
        text_blob = self.get_resource_name('textblob')
        text_data = self.get_random_text_data(self.bs.MAX_SINGLE_GET_SIZE + 1)
        self.bs.create_blob_from_text(self.container_name, text_blob, text_data)

        # Act
        blob = self.bs.get_blob_to_text(self.container_name, text_blob, max_connections=5)

        # Assert
        self.assertEqual(text_data, blob.content)

    @record
    def test_get_blob_to_text_with_progress(self):
        # Arrange
        text_blob = self.get_resource_name('textblob')
        text_data = self.get_random_text_data(self.bs.MAX_SINGLE_GET_SIZE + 1)
        self.bs.create_blob_from_text(self.container_name, text_blob, text_data)

        # Act
        progress = []

        def callback(current, total):
            progress.append((current, total))

        blob = self.bs.get_blob_to_text(
            self.container_name, text_blob, progress_callback=callback)

        # Assert
        self.assertEqual(text_data, blob.content)
        self.assert_download_progress(len(text_data.encode('utf-8')), self.bs.MAX_CHUNK_GET_SIZE, progress)

    @record
    def test_get_blob_to_text_with_encoding(self):
        # Arrange
        text = u'hello 啊齄丂狛狜 world'
        data = text.encode('utf-16')
        blob_name = self._get_blob_reference()
        self.bs.create_blob_from_bytes(self.container_name, blob_name, data)

        # Act
        blob = self.bs.get_blob_to_text(self.container_name, blob_name, 'utf-16')

        # Assert
        self.assertEqual(text, blob.content)

    @record
    def test_get_blob_to_text_with_encoding_and_progress(self):
        # Arrange
        text = u'hello 啊齄丂狛狜 world'
        data = text.encode('utf-16')
        blob_name = self._get_blob_reference()
        self.bs.create_blob_from_bytes(self.container_name, blob_name, data)

        # Act
        progress = []

        def callback(current, total):
            progress.append((current, total))

        blob = self.bs.get_blob_to_text(
            self.container_name, blob_name, 'utf-16', progress_callback=callback)

        # Assert
        self.assertEqual(text, blob.content)
        self.assert_download_progress(len(data), self.bs.MAX_CHUNK_GET_SIZE, progress)

#------------------------------------------------------------------------------
if __name__ == '__main__':
    unittest.main()
