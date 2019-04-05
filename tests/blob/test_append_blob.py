# coding: utf-8

# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------
import os
import unittest
from datetime import (
    timedelta,
    datetime,
)

from azure.common import (
    AzureHttpError,
    AzureConflictHttpError,
)

from azure.storage.blob import (
    AppendBlobService,
    BlobPermissions,

)
from tests.testcase import (
    StorageTestCase,
    TestMode,
    record,
)

from azure.storage.common._common_conversion import _get_content_md5

# ------------------------------------------------------------------------------
TEST_BLOB_PREFIX = 'blob'
FILE_PATH = 'blob_input.temp.dat'
LARGE_BLOB_SIZE = 64 * 1024
SOURCE_BLOB_SIZE = 8 * 1024


# ------------------------------------------------------------------------------

class StorageAppendBlobTest(StorageTestCase):

    def setUp(self):
        super(StorageAppendBlobTest, self).setUp()

        self.bs = self._create_storage_service(AppendBlobService, self.settings)
        self.container_name = self.get_resource_name('utcontainer')

        # create source blob to be copied from
        self.source_blob_name = self.get_resource_name('srcblob')
        self.source_blob_data = self.get_random_bytes(SOURCE_BLOB_SIZE)

        if not self.is_playback():
            self.bs.create_container(self.container_name)
            self.bs.create_blob(self.container_name, self.source_blob_name)
            self.bs.append_blob_from_bytes(self.container_name, self.source_blob_name, self.source_blob_data)

        # test chunking functionality by reducing the size of each chunk,
        # otherwise the tests would take too long to execute
        self.bs.MAX_BLOCK_SIZE = 4 * 1024

        # generate a SAS so that it is accessible with a URL
        sas_token = self.bs.generate_blob_shared_access_signature(
            self.container_name,
            self.source_blob_name,
            permission=BlobPermissions.READ,
            expiry=datetime.utcnow() + timedelta(hours=1),
        )
        self.source_blob_url = self.bs.make_blob_url(self.container_name, self.source_blob_name, sas_token=sas_token)

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

        return super(StorageAppendBlobTest, self).tearDown()

    # --Helpers-----------------------------------------------------------------
    def _get_blob_reference(self):
        return self.get_resource_name(TEST_BLOB_PREFIX)

    def _create_blob(self):
        blob_name = self._get_blob_reference()
        self.bs.create_blob(self.container_name, blob_name)
        return blob_name

    def assertBlobEqual(self, container_name, blob_name, expected_data):
        actual_data = self.bs.get_blob_to_bytes(container_name, blob_name)
        self.assertEqual(actual_data.content, expected_data)

    class NonSeekableFile(object):
        def __init__(self, wrapped_file):
            self.wrapped_file = wrapped_file

        def write(self, data):
            self.wrapped_file.write(data)

        def read(self, count):
            return self.wrapped_file.read(count)

    # --Test cases for block blobs --------------------------------------------

    @record
    def test_create_blob(self):
        # Arrange
        blob_name = self._get_blob_reference()

        # Act
        create_resp = self.bs.create_blob(self.container_name, blob_name)
        blob = self.bs.get_blob_properties(self.container_name, blob_name)

        # Assert
        self.assertTrue(self.bs.exists(self.container_name, blob_name))
        self.assertEqual(blob.properties.etag, create_resp.etag)
        self.assertEqual(blob.properties.last_modified, create_resp.last_modified)

    @record
    def test_create_blob_with_lease_id(self):
        # Arrange
        blob_name = self._create_blob()
        lease_id = self.bs.acquire_blob_lease(self.container_name, blob_name)

        # Act
        self.bs.create_blob(self.container_name, blob_name, lease_id=lease_id)

        # Assert
        self.assertTrue(self.bs.exists(self.container_name, blob_name))

    @record
    def test_create_blob_with_metadata(self):
        # Arrange
        metadata = {'hello': 'world', 'number': '42'}
        blob_name = self._get_blob_reference()

        # Act
        self.bs.create_blob(self.container_name, blob_name, metadata=metadata)

        # Assert
        md = self.bs.get_blob_metadata(self.container_name, blob_name)
        self.assertDictEqual(md, metadata)

    @record
    def test_append_block(self):
        # Arrange
        blob_name = self._create_blob()

        # Act
        for i in range(5):
            resp = self.bs.append_block(self.container_name, blob_name,
                                        u'block {0}'.format(i).encode('utf-8'))
            self.assertEqual(resp.append_offset, 7 * i)
            self.assertEqual(resp.committed_block_count, i + 1)
            self.assertIsNotNone(resp.etag)
            self.assertIsNotNone(resp.last_modified)

        # Assert
        self.assertBlobEqual(self.container_name, blob_name, b'block 0block 1block 2block 3block 4')

    @record
    def test_append_block_unicode(self):
        # Arrange
        blob_name = self._create_blob()

        # Act
        with self.assertRaises(TypeError):
            resp = self.bs.append_block(self.container_name, blob_name, u'啊齄丂狛狜')

        # Assert

    @record
    def test_append_block_with_md5(self):
        # Arrange
        blob_name = self._create_blob()

        # Act
        resp = self.bs.append_block(self.container_name, blob_name,
                                    b'block',
                                    validate_content=True)

        # Assert

    @record
    def test_append_block_from_url(self):
        # Arrange
        dest_blob_name = self.get_resource_name('destblob')
        self.bs.create_blob(self.container_name, dest_blob_name)

        # Act: make append block from url calls
        resp = self.bs.append_block_from_url(self.container_name, dest_blob_name, self.source_blob_url,
                                             source_range_start=0, source_range_end=4 * 1024 - 1)
        self.assertEqual(resp.append_offset, 0)
        self.assertEqual(resp.committed_block_count, 1)
        self.assertIsNotNone(resp.etag)
        self.assertIsNotNone(resp.last_modified)

        resp = self.bs.append_block_from_url(self.container_name, dest_blob_name, self.source_blob_url,
                                             source_range_start=4 * 1024, source_range_end=SOURCE_BLOB_SIZE - 1)
        self.assertEqual(resp.append_offset, 4 * 1024)
        self.assertEqual(resp.committed_block_count, 2)
        self.assertIsNotNone(resp.etag)
        self.assertIsNotNone(resp.last_modified)

        # Assert the destination blob is constructed correctly
        blob = self.bs.get_blob_properties(self.container_name, dest_blob_name)
        self.assertBlobEqual(self.container_name, dest_blob_name, self.source_blob_data)
        self.assertEqual(blob.properties.etag, resp.etag)
        self.assertEqual(blob.properties.last_modified, resp.last_modified)

    @record
    def test_append_block_from_url_and_validate_content_md5(self):
        # Arrange
        src_md5 = _get_content_md5(self.source_blob_data)
        dest_blob_name = self.get_resource_name('destblob')
        self.bs.create_blob(self.container_name, dest_blob_name)

        # Act part 1: make append block from url calls with correct md5
        resp = self.bs.append_block_from_url(self.container_name, dest_blob_name, self.source_blob_url,
                                             source_range_start=0, source_range_end=SOURCE_BLOB_SIZE - 1,
                                             source_content_md5=src_md5)
        self.assertEqual(resp.append_offset, 0)
        self.assertEqual(resp.committed_block_count, 1)
        self.assertIsNotNone(resp.etag)
        self.assertIsNotNone(resp.last_modified)

        # Assert the destination blob is constructed correctly
        blob = self.bs.get_blob_properties(self.container_name, dest_blob_name)
        self.assertBlobEqual(self.container_name, dest_blob_name, self.source_blob_data)
        self.assertEqual(blob.properties.etag, resp.etag)
        self.assertEqual(blob.properties.last_modified, resp.last_modified)

        # Act part 2: put block from url with wrong md5
        with self.assertRaises(AzureHttpError):
            self.bs.append_block_from_url(self.container_name, dest_blob_name, self.source_blob_url,
                                          source_range_start=0, source_range_end=SOURCE_BLOB_SIZE - 1,
                                          source_content_md5=_get_content_md5(b"POTATO"))

    @record
    def test_append_block_from_url_with_source_if_modified(self):
        # Arrange
        dest_blob_name = self.get_resource_name('destblob')
        resource_properties = self.bs.create_blob(self.container_name, dest_blob_name)

        # Act part 1: make append block from url calls
        resp = self.bs.append_block_from_url(self.container_name, dest_blob_name, self.source_blob_url,
                                             source_range_start=0, source_range_end=SOURCE_BLOB_SIZE - 1,
                                             source_if_modified_since=resource_properties.last_modified - timedelta(
                                                 hours=15))
        self.assertEqual(resp.append_offset, 0)
        self.assertEqual(resp.committed_block_count, 1)
        self.assertIsNotNone(resp.etag)
        self.assertIsNotNone(resp.last_modified)

        # Assert the destination blob is constructed correctly
        blob = self.bs.get_blob_properties(self.container_name, dest_blob_name)
        self.assertBlobEqual(self.container_name, dest_blob_name, self.source_blob_data)
        self.assertEqual(blob.properties.etag, resp.etag)
        self.assertEqual(blob.properties.last_modified, resp.last_modified)

        # Act part 2: put block from url with failing condition
        with self.assertRaises(AzureHttpError):
            self.bs.append_block_from_url(self.container_name, dest_blob_name, self.source_blob_url,
                                          source_range_start=0, source_range_end=SOURCE_BLOB_SIZE - 1,
                                          source_if_modified_since=resource_properties.last_modified)

    @record
    def test_append_block_from_url_with_source_if_unmodified(self):
        # Arrange
        dest_blob_name = self.get_resource_name('destblob')
        resource_properties = self.bs.create_blob(self.container_name, dest_blob_name)

        # Act part 1: make append block from url calls
        resp = self.bs.append_block_from_url(self.container_name, dest_blob_name, self.source_blob_url,
                                             source_range_start=0, source_range_end=SOURCE_BLOB_SIZE - 1,
                                             source_if_unmodified_since=resource_properties.last_modified)
        self.assertEqual(resp.append_offset, 0)
        self.assertEqual(resp.committed_block_count, 1)
        self.assertIsNotNone(resp.etag)
        self.assertIsNotNone(resp.last_modified)

        # Assert the destination blob is constructed correctly
        blob = self.bs.get_blob_properties(self.container_name, dest_blob_name)
        self.assertBlobEqual(self.container_name, dest_blob_name, self.source_blob_data)
        self.assertEqual(blob.properties.etag, resp.etag)
        self.assertEqual(blob.properties.last_modified, resp.last_modified)

        # Act part 2: put block from url with failing condition
        with self.assertRaises(AzureHttpError):
            self.bs.append_block_from_url(self.container_name, dest_blob_name, self.source_blob_url,
                                          source_range_start=0, source_range_end=SOURCE_BLOB_SIZE - 1,
                                          if_unmodified_since=resource_properties.last_modified - timedelta(
                                              hours=15))

    @record
    def test_append_block_from_url_with_source_if_match(self):
        # Arrange
        dest_blob_name = self.get_resource_name('destblob')
        self.bs.create_blob(self.container_name, dest_blob_name)
        src_blob_resource_properties = self.bs.get_blob_properties(self.container_name, self.source_blob_name).properties

        # Act part 1: make append block from url calls
        resp = self.bs.append_block_from_url(self.container_name, dest_blob_name, self.source_blob_url,
                                             source_range_start=0, source_range_end=SOURCE_BLOB_SIZE - 1,
                                             source_if_match=src_blob_resource_properties.etag)
        self.assertEqual(resp.append_offset, 0)
        self.assertEqual(resp.committed_block_count, 1)
        self.assertIsNotNone(resp.etag)
        self.assertIsNotNone(resp.last_modified)

        # Assert the destination blob is constructed correctly
        blob = self.bs.get_blob_properties(self.container_name, dest_blob_name)
        self.assertBlobEqual(self.container_name, dest_blob_name, self.source_blob_data)
        self.assertEqual(blob.properties.etag, resp.etag)
        self.assertEqual(blob.properties.last_modified, resp.last_modified)

        # Act part 2: put block from url with failing condition
        with self.assertRaises(AzureHttpError):
            self.bs.append_block_from_url(self.container_name, dest_blob_name, self.source_blob_url,
                                          source_range_start=0, source_range_end=SOURCE_BLOB_SIZE - 1,
                                          source_if_match='0x111111111111111')

    @record
    def test_append_block_from_url_with_source_if_none_match(self):
        # Arrange
        dest_blob_name = self.get_resource_name('destblob')
        self.bs.create_blob(self.container_name, dest_blob_name)
        src_blob_resource_properties = self.bs.get_blob_properties(self.container_name,
                                                                   self.source_blob_name).properties

        # Act part 1: make append block from url calls
        resp = self.bs.append_block_from_url(self.container_name, dest_blob_name, self.source_blob_url,
                                             source_range_start=0, source_range_end=SOURCE_BLOB_SIZE - 1,
                                             source_if_none_match='0x111111111111111')
        self.assertEqual(resp.append_offset, 0)
        self.assertEqual(resp.committed_block_count, 1)
        self.assertIsNotNone(resp.etag)
        self.assertIsNotNone(resp.last_modified)

        # Assert the destination blob is constructed correctly
        blob = self.bs.get_blob_properties(self.container_name, dest_blob_name)
        self.assertBlobEqual(self.container_name, dest_blob_name, self.source_blob_data)
        self.assertEqual(blob.properties.etag, resp.etag)
        self.assertEqual(blob.properties.last_modified, resp.last_modified)

        # Act part 2: put block from url with failing condition
        with self.assertRaises(AzureHttpError):
            self.bs.append_block_from_url(self.container_name, dest_blob_name, self.source_blob_url,
                                          source_range_start=0, source_range_end=SOURCE_BLOB_SIZE - 1,
                                          source_if_none_match=src_blob_resource_properties.etag)

    @record
    def test_append_block_from_url_with_maxsize_condition(self):
        # Arrange
        dest_blob_name = self.get_resource_name('destblob')
        self.bs.create_blob(self.container_name, dest_blob_name)

        # Act part 1: make append block from url calls
        resp = self.bs.append_block_from_url(self.container_name, dest_blob_name, self.source_blob_url,
                                             source_range_start=0, source_range_end=SOURCE_BLOB_SIZE - 1,
                                             maxsize_condition=SOURCE_BLOB_SIZE + 1)
        self.assertEqual(resp.append_offset, 0)
        self.assertEqual(resp.committed_block_count, 1)
        self.assertIsNotNone(resp.etag)
        self.assertIsNotNone(resp.last_modified)

        # Assert the destination blob is constructed correctly
        blob = self.bs.get_blob_properties(self.container_name, dest_blob_name)
        self.assertBlobEqual(self.container_name, dest_blob_name, self.source_blob_data)
        self.assertEqual(blob.properties.etag, resp.etag)
        self.assertEqual(blob.properties.last_modified, resp.last_modified)

        # Act part 2: put block from url with failing condition
        with self.assertRaises(AzureHttpError):
            self.bs.append_block_from_url(self.container_name, dest_blob_name, self.source_blob_url,
                                          source_range_start=0, source_range_end=SOURCE_BLOB_SIZE - 1,
                                          maxsize_condition=SOURCE_BLOB_SIZE + 1)

    @record
    def test_append_block_from_url_with_appendpos_condition(self):
        # Arrange
        dest_blob_name = self.get_resource_name('destblob')
        self.bs.create_blob(self.container_name, dest_blob_name)

        # Act part 1: make append block from url calls
        resp = self.bs.append_block_from_url(self.container_name, dest_blob_name, self.source_blob_url,
                                             source_range_start=0, source_range_end=SOURCE_BLOB_SIZE - 1,
                                             appendpos_condition=0)
        self.assertEqual(resp.append_offset, 0)
        self.assertEqual(resp.committed_block_count, 1)
        self.assertIsNotNone(resp.etag)
        self.assertIsNotNone(resp.last_modified)

        # Assert the destination blob is constructed correctly
        blob = self.bs.get_blob_properties(self.container_name, dest_blob_name)
        self.assertBlobEqual(self.container_name, dest_blob_name, self.source_blob_data)
        self.assertEqual(blob.properties.etag, resp.etag)
        self.assertEqual(blob.properties.last_modified, resp.last_modified)

        # Act part 2: put block from url with failing condition
        with self.assertRaises(AzureHttpError):
            self.bs.append_block_from_url(self.container_name, dest_blob_name, self.source_blob_url,
                                          source_range_start=0, source_range_end=SOURCE_BLOB_SIZE - 1,
                                          appendpos_condition=0)

    @record
    def test_append_block_from_url_with_if_modified(self):
        # Arrange
        dest_blob_name = self.get_resource_name('destblob')
        resource_properties = self.bs.create_blob(self.container_name, dest_blob_name)

        # Act part 1: make append block from url calls
        resp = self.bs.append_block_from_url(self.container_name, dest_blob_name, self.source_blob_url,
                                             source_range_start=0, source_range_end=SOURCE_BLOB_SIZE - 1,
                                             if_modified_since=resource_properties.last_modified - timedelta(
                                                 minutes=15))
        self.assertEqual(resp.append_offset, 0)
        self.assertEqual(resp.committed_block_count, 1)
        self.assertIsNotNone(resp.etag)
        self.assertIsNotNone(resp.last_modified)

        # Assert the destination blob is constructed correctly
        blob = self.bs.get_blob_properties(self.container_name, dest_blob_name)
        self.assertBlobEqual(self.container_name, dest_blob_name, self.source_blob_data)
        self.assertEqual(blob.properties.etag, resp.etag)
        self.assertEqual(blob.properties.last_modified, resp.last_modified)

        # Act part 2: put block from url with failing condition
        with self.assertRaises(AzureHttpError):
            self.bs.append_block_from_url(self.container_name, dest_blob_name, self.source_blob_url,
                                          source_range_start=0, source_range_end=SOURCE_BLOB_SIZE - 1,
                                          if_modified_since=resource_properties.last_modified)

    @record
    def test_append_block_from_url_with_if_unmodified(self):
        # Arrange
        dest_blob_name = self.get_resource_name('destblob')
        resource_properties = self.bs.create_blob(self.container_name, dest_blob_name)

        # Act part 1: make append block from url calls
        resp = self.bs.append_block_from_url(self.container_name, dest_blob_name, self.source_blob_url,
                                             source_range_start=0, source_range_end=SOURCE_BLOB_SIZE - 1,
                                             if_unmodified_since=resource_properties.last_modified)
        self.assertEqual(resp.append_offset, 0)
        self.assertEqual(resp.committed_block_count, 1)
        self.assertIsNotNone(resp.etag)
        self.assertIsNotNone(resp.last_modified)

        # Assert the destination blob is constructed correctly
        blob = self.bs.get_blob_properties(self.container_name, dest_blob_name)
        self.assertBlobEqual(self.container_name, dest_blob_name, self.source_blob_data)
        self.assertEqual(blob.properties.etag, resp.etag)
        self.assertEqual(blob.properties.last_modified, resp.last_modified)

        # Act part 2: put block from url with failing condition
        with self.assertRaises(AzureHttpError):
            self.bs.append_block_from_url(self.container_name, dest_blob_name, self.source_blob_url,
                                          source_range_start=0, source_range_end=SOURCE_BLOB_SIZE - 1,
                                          if_unmodified_since=resource_properties.last_modified - timedelta(
                                              minutes=15))

    @record
    def test_append_block_from_url_with_if_match(self):
        # Arrange
        dest_blob_name = self.get_resource_name('destblob')
        resource_properties = self.bs.create_blob(self.container_name, dest_blob_name)

        # Act part 1: make append block from url calls
        resp = self.bs.append_block_from_url(self.container_name, dest_blob_name, self.source_blob_url,
                                             source_range_start=0, source_range_end=SOURCE_BLOB_SIZE - 1,
                                             if_match=resource_properties.etag)
        self.assertEqual(resp.append_offset, 0)
        self.assertEqual(resp.committed_block_count, 1)
        self.assertIsNotNone(resp.etag)
        self.assertIsNotNone(resp.last_modified)

        # Assert the destination blob is constructed correctly
        blob = self.bs.get_blob_properties(self.container_name, dest_blob_name)
        self.assertBlobEqual(self.container_name, dest_blob_name, self.source_blob_data)
        self.assertEqual(blob.properties.etag, resp.etag)
        self.assertEqual(blob.properties.last_modified, resp.last_modified)

        # Act part 2: put block from url with failing condition
        with self.assertRaises(AzureHttpError):
            self.bs.append_block_from_url(self.container_name, dest_blob_name, self.source_blob_url,
                                          source_range_start=0, source_range_end=SOURCE_BLOB_SIZE - 1,
                                          if_match='0x111111111111111')

    @record
    def test_append_block_from_url_with_if_none_match(self):
        # Arrange
        dest_blob_name = self.get_resource_name('destblob')
        resource_properties = self.bs.create_blob(self.container_name, dest_blob_name)

        # Act part 1: make append block from url calls
        resp = self.bs.append_block_from_url(self.container_name, dest_blob_name, self.source_blob_url,
                                             source_range_start=0, source_range_end=SOURCE_BLOB_SIZE - 1,
                                             if_none_match='0x111111111111111')
        self.assertEqual(resp.append_offset, 0)
        self.assertEqual(resp.committed_block_count, 1)
        self.assertIsNotNone(resp.etag)
        self.assertIsNotNone(resp.last_modified)

        # Assert the destination blob is constructed correctly
        blob = self.bs.get_blob_properties(self.container_name, dest_blob_name)
        self.assertBlobEqual(self.container_name, dest_blob_name, self.source_blob_data)
        self.assertEqual(blob.properties.etag, resp.etag)
        self.assertEqual(blob.properties.last_modified, resp.last_modified)

        # Act part 2: put block from url with failing condition
        with self.assertRaises(AzureHttpError):
            self.bs.append_block_from_url(self.container_name, dest_blob_name, self.source_blob_url,
                                          source_range_start=0, source_range_end=SOURCE_BLOB_SIZE - 1,
                                          if_none_match=blob.properties.etag)

    @record
    def test_append_blob_from_bytes(self):
        # Arrange
        blob_name = self._create_blob()

        # Act
        data = b'abcdefghijklmnopqrstuvwxyz'
        append_resp = self.bs.append_blob_from_bytes(self.container_name, blob_name, data)
        blob = self.bs.get_blob_properties(self.container_name, blob_name)

        # Assert
        self.assertBlobEqual(self.container_name, blob_name, data)
        self.assertEqual(blob.properties.etag, append_resp.etag)
        self.assertEqual(blob.properties.last_modified, append_resp.last_modified)

    @record
    def test_append_blob_from_0_bytes(self):
        # Arrange
        blob_name = self._create_blob()

        # Act
        data = b''
        append_resp = self.bs.append_blob_from_bytes(self.container_name, blob_name, data)

        # Assert
        self.assertBlobEqual(self.container_name, blob_name, data)
        # appending nothing should not make any network call
        self.assertIsNone(append_resp.etag)
        self.assertIsNone(append_resp.last_modified)

    @record
    def test_append_blob_from_bytes_with_progress(self):
        # Arrange
        blob_name = self._create_blob()
        data = b'abcdefghijklmnopqrstuvwxyz'

        # Act
        progress = []

        def callback(current, total):
            progress.append((current, total))

        self.bs.append_blob_from_bytes(self.container_name, blob_name, data, progress_callback=callback)

        # Assert
        self.assertBlobEqual(self.container_name, blob_name, data)
        self.assert_upload_progress(len(data), self.bs.MAX_BLOCK_SIZE, progress)

    @record
    def test_append_blob_from_bytes_with_index(self):
        # Arrange
        blob_name = self._create_blob()

        # Act
        data = b'abcdefghijklmnopqrstuvwxyz'
        self.bs.append_blob_from_bytes(self.container_name, blob_name, data, 3)

        # Assert
        self.assertBlobEqual(self.container_name, blob_name, data[3:])

    @record
    def test_append_blob_from_bytes_with_index_and_count(self):
        # Arrange
        blob_name = self._create_blob()

        # Act
        data = b'abcdefghijklmnopqrstuvwxyz'
        self.bs.append_blob_from_bytes(self.container_name, blob_name, data, 3, 5)

        # Assert
        self.assertBlobEqual(self.container_name, blob_name, data[3:8])

    @record
    def test_append_blob_from_bytes_chunked_upload(self):
        # Arrange
        blob_name = self._create_blob()
        data = self.get_random_bytes(LARGE_BLOB_SIZE)

        # Act
        append_resp = self.bs.append_blob_from_bytes(self.container_name, blob_name, data)
        blob = self.bs.get_blob_properties(self.container_name, blob_name)

        # Assert
        self.assertBlobEqual(self.container_name, blob_name, data)
        self.assertEqual(blob.properties.etag, append_resp.etag)
        self.assertEqual(blob.properties.last_modified, append_resp.last_modified)

    @record
    def test_append_blob_from_bytes_with_progress_chunked_upload(self):
        # Arrange
        blob_name = self._create_blob()
        data = self.get_random_bytes(LARGE_BLOB_SIZE)

        # Act
        progress = []

        def callback(current, total):
            progress.append((current, total))

        self.bs.append_blob_from_bytes(self.container_name, blob_name, data, progress_callback=callback)

        # Assert
        self.assertBlobEqual(self.container_name, blob_name, data)
        self.assert_upload_progress(len(data), self.bs.MAX_BLOCK_SIZE, progress)

    @record
    def test_append_blob_from_bytes_chunked_upload_with_index_and_count(self):
        # Arrange
        blob_name = self._create_blob()
        data = self.get_random_bytes(LARGE_BLOB_SIZE)
        index = 33
        blob_size = len(data) - 66

        # Act
        self.bs.append_blob_from_bytes(self.container_name, blob_name, data, index, blob_size)

        # Assert
        self.assertBlobEqual(self.container_name, blob_name, data[index:index + blob_size])

    @record
    def test_append_blob_from_path_chunked_upload(self):
        # Arrange
        blob_name = self._create_blob()
        data = self.get_random_bytes(LARGE_BLOB_SIZE)
        with open(FILE_PATH, 'wb') as stream:
            stream.write(data)

        # Act
        append_resp = self.bs.append_blob_from_path(self.container_name, blob_name, FILE_PATH)
        blob = self.bs.get_blob_properties(self.container_name, blob_name)

        # Assert
        self.assertBlobEqual(self.container_name, blob_name, data)
        self.assertEqual(blob.properties.etag, append_resp.etag)
        self.assertEqual(blob.properties.last_modified, append_resp.last_modified)

    @record
    def test_append_blob_from_path_with_progress_chunked_upload(self):
        # Arrange
        blob_name = self._create_blob()
        data = self.get_random_bytes(LARGE_BLOB_SIZE)
        with open(FILE_PATH, 'wb') as stream:
            stream.write(data)

        # Act
        progress = []

        def callback(current, total):
            progress.append((current, total))

        self.bs.append_blob_from_path(self.container_name, blob_name, FILE_PATH, progress_callback=callback)

        # Assert
        self.assertBlobEqual(self.container_name, blob_name, data)
        self.assert_upload_progress(len(data), self.bs.MAX_BLOCK_SIZE, progress)

    @record
    def test_append_blob_from_stream_chunked_upload(self):
        # Arrange
        blob_name = self._create_blob()
        data = self.get_random_bytes(LARGE_BLOB_SIZE)
        with open(FILE_PATH, 'wb') as stream:
            stream.write(data)

        # Act
        with open(FILE_PATH, 'rb') as stream:
            append_resp = self.bs.append_blob_from_stream(self.container_name, blob_name, stream)
        blob = self.bs.get_blob_properties(self.container_name, blob_name)

        # Assert
        self.assertBlobEqual(self.container_name, blob_name, data)
        self.assertEqual(blob.properties.etag, append_resp.etag)
        self.assertEqual(blob.properties.last_modified, append_resp.last_modified)

    @record
    def test_append_blob_from_stream_non_seekable_chunked_upload_known_size(self):
        # Arrange
        blob_name = self._create_blob()
        data = self.get_random_bytes(LARGE_BLOB_SIZE)
        with open(FILE_PATH, 'wb') as stream:
            stream.write(data)
        blob_size = len(data) - 66

        # Act
        with open(FILE_PATH, 'rb') as stream:
            non_seekable_file = StorageAppendBlobTest.NonSeekableFile(stream)
            self.bs.append_blob_from_stream(self.container_name, blob_name,
                                            non_seekable_file, count=blob_size)

        # Assert
        self.assertBlobEqual(self.container_name, blob_name, data[:blob_size])

    @record
    def test_append_blob_from_stream_non_seekable_chunked_upload_unknown_size(self):
        # Arrange
        blob_name = self._create_blob()
        data = self.get_random_bytes(LARGE_BLOB_SIZE)
        with open(FILE_PATH, 'wb') as stream:
            stream.write(data)

        # Act
        with open(FILE_PATH, 'rb') as stream:
            non_seekable_file = StorageAppendBlobTest.NonSeekableFile(stream)
            self.bs.append_blob_from_stream(self.container_name, blob_name, non_seekable_file)

        # Assert
        self.assertBlobEqual(self.container_name, blob_name, data)

    @record
    def test_append_blob_from_stream_with_multiple_appends(self):
        # Arrange
        blob_name = self._create_blob()
        data = self.get_random_bytes(LARGE_BLOB_SIZE)
        with open(FILE_PATH, 'wb') as stream1:
            stream1.write(data)
        with open(FILE_PATH, 'wb') as stream2:
            stream2.write(data)

        # Act
        with open(FILE_PATH, 'rb') as stream1:
            self.bs.append_blob_from_stream(self.container_name, blob_name, stream1)
        with open(FILE_PATH, 'rb') as stream2:
            self.bs.append_blob_from_stream(self.container_name, blob_name, stream2)

        # Assert
        data = data * 2
        self.assertBlobEqual(self.container_name, blob_name, data)

    @record
    def test_append_blob_from_stream_chunked_upload_with_count(self):
        # Arrange
        blob_name = self._create_blob()
        data = self.get_random_bytes(LARGE_BLOB_SIZE)
        with open(FILE_PATH, 'wb') as stream:
            stream.write(data)

        # Act
        blob_size = len(data) - 301
        with open(FILE_PATH, 'rb') as stream:
            self.bs.append_blob_from_stream(self.container_name, blob_name, stream, blob_size)

        # Assert
        self.assertBlobEqual(self.container_name, blob_name, data[:blob_size])

    def test_append_blob_from_stream_chunked_upload_with_count_parallel(self):
        # parallel tests introduce random order of requests, can only run live
        if TestMode.need_recording_file(self.test_mode):
            return

        # Arrange
        blob_name = self._create_blob()
        data = self.get_random_bytes(LARGE_BLOB_SIZE)
        with open(FILE_PATH, 'wb') as stream:
            stream.write(data)

        # Act
        blob_size = len(data) - 301
        with open(FILE_PATH, 'rb') as stream:
            append_resp = self.bs.append_blob_from_stream(self.container_name, blob_name, stream, blob_size)
        blob = self.bs.get_blob_properties(self.container_name, blob_name)

        # Assert
        self.assertBlobEqual(self.container_name, blob_name, data[:blob_size])
        self.assertEqual(blob.properties.etag, append_resp.etag)
        self.assertEqual(blob.properties.last_modified, append_resp.last_modified)

    @record
    def test_append_blob_from_text(self):
        # Arrange
        blob_name = self._create_blob()
        text = u'hello 啊齄丂狛狜 world'
        data = text.encode('utf-8')

        # Act
        append_resp = self.bs.append_blob_from_text(self.container_name, blob_name, text)
        blob = self.bs.get_blob_properties(self.container_name, blob_name)

        # Assert
        self.assertBlobEqual(self.container_name, blob_name, data)
        self.assertEqual(blob.properties.etag, append_resp.etag)
        self.assertEqual(blob.properties.last_modified, append_resp.last_modified)

    @record
    def test_append_blob_from_text_with_encoding(self):
        # Arrange
        blob_name = self._create_blob()
        text = u'hello 啊齄丂狛狜 world'
        data = text.encode('utf-16')

        # Act
        self.bs.append_blob_from_text(self.container_name, blob_name, text, 'utf-16')

        # Assert
        self.assertBlobEqual(self.container_name, blob_name, data)

    @record
    def test_append_blob_from_text_with_encoding_and_progress(self):
        # Arrange
        blob_name = self._create_blob()
        text = u'hello 啊齄丂狛狜 world'
        data = text.encode('utf-16')

        # Act
        progress = []

        def callback(current, total):
            progress.append((current, total))

        self.bs.append_blob_from_text(self.container_name, blob_name, text, 'utf-16',
                                      progress_callback=callback)

        # Assert
        self.assert_upload_progress(len(data), self.bs.MAX_BLOCK_SIZE, progress)

    @record
    def test_append_blob_from_text_chunked_upload(self):
        # Arrange
        blob_name = self._create_blob()
        data = self.get_random_text_data(LARGE_BLOB_SIZE)
        encoded_data = data.encode('utf-8')

        # Act
        self.bs.append_blob_from_text(self.container_name, blob_name, data)

        # Assert
        self.assertBlobEqual(self.container_name, blob_name, encoded_data)

    @record
    def test_append_blob_with_md5(self):
        # Arrange
        blob_name = self._create_blob()
        data = b'hello world'

        # Act
        self.bs.append_blob_from_bytes(self.container_name, blob_name, data,
                                       validate_content=True)

        # Assert


# ------------------------------------------------------------------------------
if __name__ == '__main__':
    unittest.main()
