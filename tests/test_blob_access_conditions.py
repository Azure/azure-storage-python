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
import datetime
import os
import unittest

from azure.common import AzureHttpError
from azure.storage.blob import (
    Blob,
    BlobBlock,
    BlockBlobService,
    PageBlobService,
    AppendBlobService,
    PageRange,
    ContentSettings,
)
from tests.testcase import (
    StorageTestCase,
    record,
)

#------------------------------------------------------------------------------

class StorageBlobAccessConditionsTest(StorageTestCase):

    def setUp(self):
        super(StorageBlobAccessConditionsTest, self).setUp()

        self.bs = self._create_storage_service(BlockBlobService, self.settings)
        self.pbs = self._create_storage_service(PageBlobService, self.settings)
        self.abs = self._create_storage_service(AppendBlobService, self.settings)
        self.container_name = self.get_resource_name('utcontainer')

    def tearDown(self):
        if not self.is_playback():
            try:
                self.bs.delete_container(self.container_name)
            except:
                pass

        for tmp_file in ['blob_input.temp.dat', 'blob_output.temp.dat']:
            if os.path.isfile(tmp_file):
                try:
                    os.remove(tmp_file)
                except:
                    pass

        return super(StorageBlobAccessConditionsTest, self).tearDown()

    #--Helpers-----------------------------------------------------------------
    def _create_container(self, container_name):
        self.bs.create_container(container_name, None, None, True)

    def _create_container_and_block_blob(self, container_name, blob_name,
                                         blob_data):
        self._create_container(container_name)
        resp = self.bs.create_blob_from_bytes(container_name, blob_name, blob_data)
        self.assertIsNotNone(resp.etag)

    def _create_container_and_page_blob(self, container_name, blob_name,
                                        content_length):
        self._create_container(container_name)
        resp = self.pbs.create_blob(self.container_name, blob_name, str(content_length))

    def _create_container_and_append_blob(self, container_name, blob_name):
        self._create_container(container_name)
        resp = self.abs.create_blob(container_name, blob_name)

    #--Test cases for blob service --------------------------------------------
    @record
    def test_set_container_metadata_with_if_modified(self):    
        # Arrange
        self.bs.create_container(self.container_name)
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))

        # Act
        metadata = {'hello': 'world', 'number': '43'}
        self.bs.set_container_metadata(self.container_name, metadata, if_modified_since=test_datetime)

        # Assert
        md = self.bs.get_container_metadata(self.container_name)
        self.assertDictEqual(metadata, md)

    @record
    def test_set_container_metadata_with_if_modified_fail(self):    
        # Arrange
        contianer_name = self.container_name
        self.bs.create_container(self.container_name)
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))

        # Act
        with self.assertRaises(AzureHttpError):
            metadata = {'hello': 'world', 'number': '43'}
            self.bs.set_container_metadata(self.container_name, metadata, if_modified_since=test_datetime)

        # Assert

    @record
    def test_set_container_acl_with_if_modified(self):
        # Arrange
        self.bs.create_container(self.container_name)
        test_datetime = (datetime.datetime.utcnow() -
                    datetime.timedelta(minutes=15))
        # Act
        self.bs.set_container_acl(self.container_name, if_modified_since=test_datetime) 

        # Assert
        acl = self.bs.get_container_acl(self.container_name)
        self.assertIsNotNone(acl)

    @record
    def test_set_container_acl_with_if_modified_fail(self):
        # Arrange
        self.bs.create_container(self.container_name)
        test_datetime = (datetime.datetime.utcnow() +
                    datetime.timedelta(minutes=15))
        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.set_container_acl(self.container_name, if_modified_since=test_datetime) 

        # Assert

    @record
    def test_set_container_acl_with_if_unmodified(self):
        # Arrange
        self.bs.create_container(self.container_name)
        test_datetime = (datetime.datetime.utcnow() +
                    datetime.timedelta(minutes=15))
        # Act
        self.bs.set_container_acl(self.container_name, if_unmodified_since=test_datetime) 

        # Assert
        acl = self.bs.get_container_acl(self.container_name)
        self.assertIsNotNone(acl)

    @record
    def test_set_container_acl_with_if_unmodified_fail(self):
        # Arrange
        self.bs.create_container(self.container_name)
        test_datetime = (datetime.datetime.utcnow() -
                    datetime.timedelta(minutes=15))
        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.set_container_acl(self.container_name, if_unmodified_since=test_datetime) 

        # Assert

    @record
    def test_lease_container_acquire_with_if_modified(self):
        # Arrange
        self.bs.create_container(self.container_name)
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))

        # Act
        self.bs.acquire_container_lease(self.container_name, if_modified_since=test_datetime) 
        self.bs.break_container_lease(self.container_name)

        # Assert

    @record
    def test_lease_container_acquire_with_if_modified_fail(self):
        # Arrange
        self.bs.create_container(self.container_name)
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.acquire_container_lease(self.container_name,
                                            if_modified_since=test_datetime) 

        # Assert

    @record
    def test_lease_container_acquire_with_if_unmodified(self):
        # Arrange
        self.bs.create_container(self.container_name)
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))

        # Act
        self.bs.acquire_container_lease(self.container_name, if_unmodified_since=test_datetime) 
        self.bs.break_container_lease(self.container_name)

        # Assert

    @record
    def test_lease_container_acquire_with_if_unmodified_fail(self):
        # Arrange
        self.bs.create_container(self.container_name)
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.acquire_container_lease(self.container_name,
                                            if_unmodified_since=test_datetime) 

        # Assert

    @record
    def test_delete_container_with_if_modified(self):
        # Arrange
        self.bs.create_container(self.container_name)
        test_datetime = (datetime.datetime.utcnow() -
                    datetime.timedelta(minutes=15))
        # Act
        deleted = self.bs.delete_container(self.container_name,
                                           if_modified_since=test_datetime) 

        # Assert
        self.assertTrue(deleted)
        exists = self.bs.exists(self.container_name)
        self.assertFalse(exists)

    @record
    def test_delete_container_with_if_modified_fail(self):
        # Arrange
        self.bs.create_container(self.container_name)
        test_datetime = (datetime.datetime.utcnow() +
                    datetime.timedelta(minutes=15))
        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.delete_container(self.container_name,
                                     if_modified_since=test_datetime) 

        # Assert

    @record
    def test_delete_container_with_if_unmodified(self):
        # Arrange
        self.bs.create_container(self.container_name)
        test_datetime = (datetime.datetime.utcnow() +
                    datetime.timedelta(minutes=15))
        # Act
        deleted = self.bs.delete_container(self.container_name,
                                           if_unmodified_since=test_datetime) 

        # Assert
        self.assertTrue(deleted)
        exists = self.bs.exists(self.container_name)
        self.assertFalse(exists)

    @record
    def test_delete_container_with_if_unmodified_fail(self):
        # Arrange
        self.bs.create_container(self.container_name)
        test_datetime = (datetime.datetime.utcnow() -
                    datetime.timedelta(minutes=15))
        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.delete_container(self.container_name,
                                     if_unmodified_since=test_datetime)

    @record
    def test_put_blob_with_if_modified(self):
        # Arrange
        data = b'hello world'
        self._create_container_and_block_blob(
            self.container_name, 'blob1', data)
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))

        # Act
        resp = self.bs.create_blob_from_bytes(
            self.container_name, 'blob1', data,
            if_modified_since=test_datetime)

        # Assert
        self.assertIsNotNone(resp.etag)

    @record
    def test_put_blob_with_if_modified_fail(self):
        # Arrange
        data = b'hello world'
        self._create_container_and_block_blob(
            self.container_name, 'blob1', data)
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.create_blob_from_bytes(
                self.container_name, 'blob1', data,
                if_modified_since=test_datetime)

        # Assert

    @record
    def test_put_blob_with_if_unmodified(self):
        # Arrange
        data = b'hello world'
        self._create_container_and_block_blob(
            self.container_name, 'blob1', data)
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))

        # Act
        resp = self.bs.create_blob_from_bytes(
            self.container_name, 'blob1', data,
            if_unmodified_since=test_datetime)

        # Assert
        self.assertIsNotNone(resp.etag)

    @record
    def test_put_blob_with_if_unmodified_fail(self):
        # Arrange
        data = b'hello world'
        self._create_container_and_block_blob(
            self.container_name, 'blob1', data)
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.create_blob_from_bytes(
                self.container_name, 'blob1', data,
                if_unmodified_since=test_datetime)

        # Assert

    @record
    def test_put_blob_with_if_match(self):
        # Arrange
        data = b'hello world'
        self._create_container_and_block_blob(
            self.container_name, 'blob1', data)
        etag = self.bs.get_blob_properties(self.container_name, 'blob1').properties.etag
        
        # Act
        resp = self.bs.create_blob_from_bytes(
            self.container_name, 'blob1', data,
            if_match=etag)

        # Assert
        self.assertIsNotNone(resp.etag)

    @record
    def test_put_blob_with_if_match_fail(self):
        # Arrange
        data = b'hello world'
        self._create_container_and_block_blob(
            self.container_name, 'blob1', data)
        
        # Act
        with self.assertRaises(AzureHttpError):
            resp = self.bs.create_blob_from_bytes(
                self.container_name, 'blob1', data,
                if_match='0x111111111111111')

        # Assert

    @record
    def test_put_blob_with_if_none_match(self):
        # Arrange
        data = b'hello world'
        self._create_container_and_block_blob(
            self.container_name, 'blob1', data)
        
        # Act
        resp = self.bs.create_blob_from_bytes(
            self.container_name, 'blob1', data,
            if_none_match='0x111111111111111')

        # Assert
        self.assertIsNotNone(resp.etag)

    @record
    def test_put_blob_with_if_none_match_fail(self):
        # Arrange
        data = b'hello world'
        self._create_container_and_block_blob(
            self.container_name, 'blob1', data)
        etag = self.bs.get_blob_properties(self.container_name, 'blob1').properties.etag

        # Act
        with self.assertRaises(AzureHttpError):
            resp = self.bs.create_blob_from_bytes(
                self.container_name, 'blob1', data,
                if_none_match=etag)

        # Assert

    @record
    def test_get_blob_with_if_modified(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))

        # Act
        blob = self.bs.get_blob_to_bytes(self.container_name, 'blob1',
                                if_modified_since=test_datetime)

        # Assert
        self.assertIsInstance(blob, Blob)
        self.assertEqual(blob.content, b'hello world')

    @record
    def test_get_blob_with_if_modified_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.get_blob_to_bytes(self.container_name, 'blob1',
                             if_modified_since=test_datetime)

        # Assert

    @record
    def test_get_blob_with_if_unmodified(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))

        # Act
        blob = self.bs.get_blob_to_bytes(self.container_name, 'blob1',
                                if_unmodified_since=test_datetime)

        # Assert
        self.assertIsInstance(blob, Blob)
        self.assertEqual(blob.content, b'hello world')

    @record
    def test_get_blob_with_if_unmodified_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.get_blob_to_bytes(self.container_name, 'blob1',
                             if_unmodified_since=test_datetime)

        # Assert

    @record
    def test_get_blob_with_if_match(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        etag = self.bs.get_blob_properties(self.container_name, 'blob1').properties.etag

        # Act
        blob = self.bs.get_blob_to_bytes(self.container_name, 'blob1', if_match=etag)

        # Assert
        self.assertIsInstance(blob, Blob)
        self.assertEqual(blob.content, b'hello world')

    @record
    def test_get_blob_with_if_match_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.get_blob_to_bytes(self.container_name, 'blob1',
                             if_match='0x111111111111111')

        # Assert

    @record
    def test_get_blob_with_if_none_match(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')

        # Act
        blob = self.bs.get_blob_to_bytes(self.container_name, 'blob1',
                                if_none_match='0x111111111111111')

        # Assert
        self.assertIsInstance(blob, Blob)
        self.assertEqual(blob.content, b'hello world')

    @record
    def test_get_blob_with_if_none_match_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        etag = self.bs.get_blob_properties(self.container_name, 'blob1').properties.etag

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.get_blob_to_bytes(self.container_name, 'blob1',
                             if_none_match=etag)

        # Assert

    @record
    def test_set_blob_properties_with_if_modified(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))
        # Act
        content_settings=ContentSettings(
            content_language='spanish',
            content_disposition='inline')
        self.bs.set_blob_properties(self.container_name, 'blob1', content_settings, if_modified_since=test_datetime)

        # Assert
        properties = self.bs.get_blob_properties(self.container_name, 'blob1').properties
        self.assertEqual(content_settings.content_language, properties.content_settings.content_language)
        self.assertEqual(content_settings.content_disposition, properties.content_settings.content_disposition)

    @record
    def test_set_blob_properties_with_if_modified_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))
        # Act
        with self.assertRaises(AzureHttpError):
            content_settings=ContentSettings(
                content_language='spanish',
                content_disposition='inline')
            self.bs.set_blob_properties(self.container_name, 'blob1', content_settings, if_modified_since=test_datetime)

        # Assert

    @record
    def test_set_blob_properties_with_if_unmodified(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))
        # Act
        content_settings=ContentSettings(
            content_language='spanish',
            content_disposition='inline')
        self.bs.set_blob_properties(self.container_name, 'blob1', content_settings, if_unmodified_since=test_datetime)

        # Assert
        properties = self.bs.get_blob_properties(self.container_name, 'blob1').properties
        self.assertEqual(content_settings.content_language, properties.content_settings.content_language)
        self.assertEqual(content_settings.content_disposition, properties.content_settings.content_disposition)

    @record
    def test_set_blob_properties_with_if_unmodified_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))
        # Act
        with self.assertRaises(AzureHttpError):
            content_settings=ContentSettings(
                content_language='spanish',
                content_disposition='inline')
            self.bs.set_blob_properties(self.container_name, 'blob1', content_settings, if_unmodified_since=test_datetime)

        # Assert

    @record
    def test_set_blob_properties_with_if_match(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        etag = self.bs.get_blob_properties(self.container_name, 'blob1').properties.etag

        # Act
        content_settings=ContentSettings(
            content_language='spanish',
            content_disposition='inline')
        self.bs.set_blob_properties(self.container_name, 'blob1', content_settings, if_match=etag)

        # Assert
        properties = self.bs.get_blob_properties(self.container_name, 'blob1').properties
        self.assertEqual(content_settings.content_language, properties.content_settings.content_language)
        self.assertEqual(content_settings.content_disposition, properties.content_settings.content_disposition)

    @record
    def test_set_blob_properties_with_if_match_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')

        # Act
        with self.assertRaises(AzureHttpError):
            content_settings=ContentSettings(
                content_language='spanish',
                content_disposition='inline')
            self.bs.set_blob_properties(self.container_name, 'blob1', content_settings, if_match='0x111111111111111')

        # Assert

    @record
    def test_set_blob_properties_with_if_none_match(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')

        # Act
        content_settings=ContentSettings(
            content_language='spanish',
            content_disposition='inline')
        self.bs.set_blob_properties(self.container_name, 'blob1', content_settings, if_none_match='0x111111111111111')

        # Assert
        properties = self.bs.get_blob_properties(self.container_name, 'blob1').properties
        self.assertEqual(content_settings.content_language, properties.content_settings.content_language)
        self.assertEqual(content_settings.content_disposition, properties.content_settings.content_disposition)

    @record
    def test_set_blob_properties_with_if_none_match_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        etag = self.bs.get_blob_properties(self.container_name, 'blob1').properties.etag

        # Act
        with self.assertRaises(AzureHttpError):
            content_settings=ContentSettings(
                content_language='spanish',
                content_disposition='inline')
            self.bs.set_blob_properties(self.container_name, 'blob1', content_settings, if_none_match=etag)

        # Assert

    @record
    def test_get_blob_properties_with_if_modified(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))
        # Act
        blob = self.bs.get_blob_properties(self.container_name, 'blob1',
                                            if_modified_since=test_datetime)

        # Assert
        self.assertIsInstance(blob, Blob)
        self.assertEqual(blob.properties.blob_type, 'BlockBlob')
        self.assertEqual(blob.properties.content_length, 11)
        self.assertEqual(blob.properties.lease.status, 'unlocked')

    @record
    def test_get_blob_properties_with_if_modified_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))
        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.get_blob_properties(self.container_name, 'blob1',
                                        if_modified_since=test_datetime)

        # Assert

    @record
    def test_get_blob_properties_with_if_unmodified(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))
        # Act
        blob = self.bs.get_blob_properties(self.container_name, 'blob1',
                                            if_unmodified_since=test_datetime)

        # Assert
        self.assertIsNotNone(blob)
        self.assertEqual(blob.properties.blob_type, 'BlockBlob')
        self.assertEqual(blob.properties.content_length, 11)
        self.assertEqual(blob.properties.lease.status, 'unlocked')

    @record
    def test_get_blob_properties_with_if_unmodified_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))
        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.get_blob_properties(self.container_name, 'blob1',
                                        if_unmodified_since=test_datetime)

        # Assert

    @record
    def test_get_blob_properties_with_if_match(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        etag = self.bs.get_blob_properties(self.container_name, 'blob1').properties.etag

        # Act
        blob = self.bs.get_blob_properties(self.container_name, 'blob1',
                                            if_match=etag)

        # Assert
        self.assertIsNotNone(blob)
        self.assertEqual(blob.properties.blob_type, 'BlockBlob')
        self.assertEqual(blob.properties.content_length, 11)
        self.assertEqual(blob.properties.lease.status, 'unlocked')

    @record
    def test_get_blob_properties_with_if_match_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.get_blob_properties(self.container_name, 'blob1',
                                        if_match='0x111111111111111')

        # Assert

    @record
    def test_get_blob_properties_with_if_none_match(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')

        # Act
        blob = self.bs.get_blob_properties(self.container_name, 'blob1',
                                           if_none_match='0x111111111111111')

        # Assert
        self.assertIsNotNone(blob)
        self.assertEqual(blob.properties.blob_type, 'BlockBlob')
        self.assertEqual(blob.properties.content_length, 11)
        self.assertEqual(blob.properties.lease.status, 'unlocked')

    @record
    def test_get_blob_properties_with_if_none_match_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        etag = self.bs.get_blob_properties(self.container_name, 'blob1').properties.etag

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.get_blob_properties(self.container_name, 'blob1',
                                        if_none_match=etag)

        # Assert

    @record
    def test_get_blob_metadata_with_if_modified(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))

        # Act
        md = self.bs.get_blob_metadata(self.container_name, 'blob1',
                                       if_modified_since=test_datetime)

        # Assert
        self.assertIsNotNone(md)

    @record
    def test_get_blob_metadata_with_if_modified_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.get_blob_metadata(self.container_name, 'blob1',
                                       if_modified_since=test_datetime)

        # Assert

    @record
    def test_get_blob_metadata_with_if_unmodified(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))

        # Act
        md = self.bs.get_blob_metadata(self.container_name, 'blob1',
                                       if_unmodified_since=test_datetime)

        # Assert
        self.assertIsNotNone(md)

    @record
    def test_get_blob_metadata_with_if_unmodified_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.get_blob_metadata(self.container_name, 'blob1',
                                       if_unmodified_since=test_datetime)

        # Assert

    @record
    def test_get_blob_metadata_with_if_match(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        etag = self.bs.get_blob_properties(self.container_name, 'blob1').properties.etag

        # Act
        md = self.bs.get_blob_metadata(self.container_name, 'blob1', if_match=etag)

        # Assert
        self.assertIsNotNone(md)

    @record
    def test_get_blob_metadata_with_if_match_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.get_blob_metadata(self.container_name, 'blob1',
                                       if_match='0x111111111111111')

        # Assert

    @record
    def test_get_blob_metadata_with_if_none_match(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')

        # Act
        md = self.bs.get_blob_metadata(self.container_name, 'blob1',
                                       if_none_match='0x111111111111111')

        # Assert
        self.assertIsNotNone(md)

    @record
    def test_get_blob_metadata_with_if_none_match_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        etag = self.bs.get_blob_properties(self.container_name, 'blob1').properties.etag

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.get_blob_metadata(self.container_name, 'blob1',
                                       if_none_match=etag)

        # Assert

    @record
    def test_set_blob_metadata_with_if_modified(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))

        # Act
        metadata = {'hello': 'world', 'number': '42'}
        self.bs.set_blob_metadata(self.container_name, 'blob1', metadata, if_modified_since=test_datetime)

        # Assert
        md = self.bs.get_blob_metadata(self.container_name, 'blob1')
        self.assertDictEqual(metadata, md)

    @record
    def test_set_blob_metadata_with_if_modified_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))

        # Act
        with self.assertRaises(AzureHttpError):
            metadata = {'hello': 'world', 'number': '42'}
            self.bs.set_blob_metadata(self.container_name, 'blob1', metadata, if_modified_since=test_datetime)

        # Assert

    @record
    def test_set_blob_metadata_with_if_unmodified(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))

        # Act
        metadata = {'hello': 'world', 'number': '42'}
        self.bs.set_blob_metadata(self.container_name, 'blob1', metadata, if_unmodified_since=test_datetime)

        # Assert
        md = self.bs.get_blob_metadata(self.container_name, 'blob1')
        self.assertDictEqual(metadata, md)

    @record
    def test_set_blob_metadata_with_if_unmodified_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))

        # Act
        with self.assertRaises(AzureHttpError):
            metadata = {'hello': 'world', 'number': '42'}
            self.bs.set_blob_metadata(self.container_name, 'blob1', metadata, if_unmodified_since=test_datetime)

        # Assert

    @record
    def test_set_blob_metadata_with_if_match(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        etag = self.bs.get_blob_properties(self.container_name, 'blob1').properties.etag
        
        # Act
        metadata = {'hello': 'world', 'number': '42'}
        self.bs.set_blob_metadata(self.container_name, 'blob1', metadata, if_match=etag)

        # Assert
        md = self.bs.get_blob_metadata(self.container_name, 'blob1')
        self.assertDictEqual(metadata, md)

    @record
    def test_set_blob_metadata_with_if_match_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        
        # Act
        with self.assertRaises(AzureHttpError):
            metadata = {'hello': 'world', 'number': '42'}
            self.bs.set_blob_metadata(self.container_name, 'blob1', metadata, if_match='0x111111111111111')

        # Assert

    @record
    def test_set_blob_metadata_with_if_none_match(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        
        # Act
        metadata = {'hello': 'world', 'number': '42'}
        self.bs.set_blob_metadata(self.container_name, 'blob1', metadata, if_none_match='0x111111111111111')

        # Assert
        md = self.bs.get_blob_metadata(self.container_name, 'blob1')
        self.assertDictEqual(metadata, md)

    @record
    def test_set_blob_metadata_with_if_none_match_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        etag = self.bs.get_blob_properties(self.container_name, 'blob1').properties.etag
        
        # Act
        with self.assertRaises(AzureHttpError):
            metadata = {'hello': 'world', 'number': '42'}
            self.bs.set_blob_metadata(self.container_name, 'blob1', metadata, if_none_match=etag)

        # Assert

    @record
    def test_delete_blob_with_if_modified(self):
        # Arrange
        test_datetime = (datetime.datetime.utcnow() -
                    datetime.timedelta(minutes=15))
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')

        # Act
        resp = self.bs.delete_blob(self.container_name, 'blob1',
                                   if_modified_since=test_datetime)

        # Assert
        self.assertIsNone(resp)

    @record
    def test_delete_blob_with_if_modified_fail(self):
        # Arrange
        test_datetime = (datetime.datetime.utcnow() +
                    datetime.timedelta(minutes=15))
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.delete_blob(self.container_name, 'blob1',
                                if_modified_since=test_datetime)

        # Assert

    @record
    def test_delete_blob_with_if_unmodified(self):
        # Arrange
        test_datetime = (datetime.datetime.utcnow() +
                    datetime.timedelta(minutes=15))
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')

        # Act
        resp = self.bs.delete_blob(self.container_name, 'blob1',
                                   if_unmodified_since=test_datetime)

        # Assert
        self.assertIsNone(resp)

    @record
    def test_delete_blob_with_if_unmodified_fail(self):
        # Arrange
        test_datetime = (datetime.datetime.utcnow() -
                    datetime.timedelta(minutes=15))
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.delete_blob(self.container_name, 'blob1',
                                if_unmodified_since=test_datetime)

        # Assert

    @record
    def test_delete_blob_with_if_match(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        etag = self.bs.get_blob_properties(self.container_name, 'blob1').properties.etag

        # Act
        resp = self.bs.delete_blob(self.container_name, 'blob1', if_match=etag)

        # Assert
        self.assertIsNone(resp)

    @record
    def test_delete_blob_with_if_match_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.delete_blob(self.container_name, 'blob1',
                                if_match='0x111111111111111')

        # Assert

    @record
    def test_delete_blob_with_if_none_match(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')

        # Act
        resp = self.bs.delete_blob(self.container_name, 'blob1',
                                   if_none_match='0x111111111111111')

        # Assert
        self.assertIsNone(resp)

    @record
    def test_delete_blob_with_if_none_match_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        etag = self.bs.get_blob_properties(self.container_name, 'blob1').properties.etag

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.delete_blob(self.container_name, 'blob1', if_none_match=etag)

        # Assert

    @record
    def test_snapshot_blob_with_if_modified(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))

        # Act
        resp = self.bs.snapshot_blob(self.container_name, 'blob1',
                                     if_modified_since=test_datetime)

        # Assert
        self.assertIsNotNone(resp)
        self.assertIsNotNone(resp.snapshot)

    @record
    def test_snapshot_blob_with_if_modified_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.snapshot_blob(self.container_name, 'blob1',
                                  if_modified_since=test_datetime)

        # Assert

    @record
    def test_snapshot_blob_with_if_unmodified(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))

        # Act
        resp = self.bs.snapshot_blob(self.container_name, 'blob1',
                                     if_unmodified_since=test_datetime)

        # Assert
        self.assertIsNotNone(resp)
        self.assertIsNotNone(resp.snapshot)

    @record
    def test_snapshot_blob_with_if_unmodified_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.snapshot_blob(self.container_name, 'blob1',
                                  if_unmodified_since=test_datetime)

        # Assert

    @record
    def test_snapshot_blob_with_if_match(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        etag = self.bs.get_blob_properties(self.container_name, 'blob1').properties.etag

        # Act
        resp = self.bs.snapshot_blob(self.container_name, 'blob1',
                                     if_match=etag)

        # Assert
        self.assertIsNotNone(resp)
        self.assertIsNotNone(resp.snapshot)

    @record
    def test_snapshot_blob_with_if_match_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.snapshot_blob(self.container_name, 'blob1',
                                  if_match='0x111111111111111')

        # Assert

    @record
    def test_snapshot_blob_with_if_none_match(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')

        # Act
        resp = self.bs.snapshot_blob(self.container_name, 'blob1',
                                     if_none_match='0x111111111111111')

        # Assert
        self.assertIsNotNone(resp)
        self.assertIsNotNone(resp.snapshot)

    @record
    def test_snapshot_blob_with_if_none_match_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        etag = self.bs.get_blob_properties(self.container_name, 'blob1').properties.etag

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.snapshot_blob(self.container_name, 'blob1',
                                  if_none_match=etag)

        # Assert

    @record
    def test_lease_blob_with_if_modified(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_lease_id = '00000000-1111-2222-3333-444444444444'
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))

        # Act
        resp1 = self.bs.acquire_blob_lease(
            self.container_name, 'blob1',
            if_modified_since=test_datetime,
            proposed_lease_id=test_lease_id)

        self.bs.break_blob_lease( self.container_name, 'blob1')

        # Assert
        self.assertIsNotNone(resp1)

    @record
    def test_lease_blob_with_if_modified_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.acquire_blob_lease(
                self.container_name, 'blob1',
                if_modified_since=test_datetime)

        # Assert

    @record
    def test_lease_blob_with_if_unmodified(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_lease_id = '00000000-1111-2222-3333-444444444444'
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))

        # Act
        resp1 = self.bs.acquire_blob_lease(
            self.container_name, 'blob1',
            if_unmodified_since=test_datetime,
            proposed_lease_id=test_lease_id)

        self.bs.break_blob_lease(self.container_name, 'blob1')

        # Assert
        self.assertIsNotNone(resp1)

    @record
    def test_lease_blob_with_if_unmodified_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.acquire_blob_lease(
                self.container_name, 'blob1',
                if_unmodified_since=test_datetime)

        # Assert

    @record
    def test_lease_blob_with_if_match(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        etag = self.bs.get_blob_properties(self.container_name, 'blob1').properties.etag
        test_lease_id = '00000000-1111-2222-3333-444444444444'

        # Act
        resp1 = self.bs.acquire_blob_lease(
            self.container_name, 'blob1',
            proposed_lease_id=test_lease_id,
            if_match=etag)

        self.bs.break_blob_lease(self.container_name, 'blob1')

        # Assert
        self.assertIsNotNone(resp1)

    @record
    def test_lease_blob_with_if_match_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.acquire_blob_lease(
                self.container_name, 'blob1', if_match='0x111111111111111')

        # Assert

    @record
    def test_lease_blob_with_if_none_match(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_lease_id = '00000000-1111-2222-3333-444444444444'

        # Act
        resp1 = self.bs.acquire_blob_lease(
            self.container_name, 'blob1',
            proposed_lease_id=test_lease_id,
            if_none_match='0x111111111111111')

        self.bs.break_blob_lease(self.container_name, 'blob1')


        # Assert
        self.assertIsNotNone(resp1)

    @record
    def test_lease_blob_with_if_none_match_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        etag = self.bs.get_blob_properties(self.container_name, 'blob1').properties.etag

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.acquire_blob_lease(
                self.container_name, 'blob1', if_none_match=etag)

        # Assert

    @record
    def test_put_block_list_with_if_modified(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'')
        self.bs.put_block(self.container_name, 'blob1', b'AAA', '1')
        self.bs.put_block(self.container_name, 'blob1', b'BBB', '2')
        self.bs.put_block(self.container_name, 'blob1', b'CCC', '3')
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))

        # Act
        block_list = [BlobBlock(id='1'), BlobBlock(id='2'), BlobBlock(id='3')]
        self.bs.put_block_list(self.container_name, 'blob1', block_list, if_modified_since=test_datetime)

        # Assert
        blob = self.bs.get_blob_to_bytes(self.container_name, 'blob1')
        self.assertEqual(blob.content, b'AAABBBCCC')

    @record
    def test_put_block_list_with_if_modified_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'')
        self.bs.put_block(self.container_name, 'blob1', b'AAA', '1')
        self.bs.put_block(self.container_name, 'blob1', b'BBB', '2')
        self.bs.put_block(self.container_name, 'blob1', b'CCC', '3')
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.put_block_list(
            self.container_name, 'blob1', [BlobBlock(id='1'), BlobBlock(id='2'), BlobBlock(id='3')],
                if_modified_since=test_datetime)

        # Assert

    @record
    def test_put_block_list_with_if_unmodified(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'')
        self.bs.put_block(self.container_name, 'blob1', b'AAA', '1')
        self.bs.put_block(self.container_name, 'blob1', b'BBB', '2')
        self.bs.put_block(self.container_name, 'blob1', b'CCC', '3')
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))

        # Act
        block_list = [BlobBlock(id='1'), BlobBlock(id='2'), BlobBlock(id='3')]
        self.bs.put_block_list(self.container_name, 'blob1', block_list, if_unmodified_since=test_datetime)

        # Assert
        blob = self.bs.get_blob_to_bytes(self.container_name, 'blob1')
        self.assertEqual(blob.content, b'AAABBBCCC')

    @record
    def test_put_block_list_with_if_unmodified_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'')
        self.bs.put_block(self.container_name, 'blob1', b'AAA', '1')
        self.bs.put_block(self.container_name, 'blob1', b'BBB', '2')
        self.bs.put_block(self.container_name, 'blob1', b'CCC', '3')
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.put_block_list(
            self.container_name, 'blob1', [BlobBlock(id='1'), BlobBlock(id='2'), BlobBlock(id='3')],
                if_unmodified_since=test_datetime)

        # Assert

    @record
    def test_put_block_list_with_if_match(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'')
        self.bs.put_block(self.container_name, 'blob1', b'AAA', '1')
        self.bs.put_block(self.container_name, 'blob1', b'BBB', '2')
        self.bs.put_block(self.container_name, 'blob1', b'CCC', '3')
        etag = self.bs.get_blob_properties(self.container_name, 'blob1').properties.etag

        # Act
        block_list = [BlobBlock(id='1'), BlobBlock(id='2'), BlobBlock(id='3')]
        self.bs.put_block_list(self.container_name, 'blob1', block_list, if_match=etag)

        # Assert
        blob = self.bs.get_blob_to_bytes(self.container_name, 'blob1')
        self.assertEqual(blob.content, b'AAABBBCCC')

    @record
    def test_put_block_list_with_if_match_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'')
        self.bs.put_block(self.container_name, 'blob1', b'AAA', '1')
        self.bs.put_block(self.container_name, 'blob1', b'BBB', '2')
        self.bs.put_block(self.container_name, 'blob1', b'CCC', '3')

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.put_block_list(
            self.container_name, 'blob1', [BlobBlock(id='1'), BlobBlock(id='2'), BlobBlock(id='3')],
                if_match='0x111111111111111')

        # Assert

    @record
    def test_put_block_list_with_if_none_match(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'')
        self.bs.put_block(self.container_name, 'blob1', b'AAA', '1')
        self.bs.put_block(self.container_name, 'blob1', b'BBB', '2')
        self.bs.put_block(self.container_name, 'blob1', b'CCC', '3')

        # Act
        block_list = [BlobBlock(id='1'), BlobBlock(id='2'), BlobBlock(id='3')]
        self.bs.put_block_list(self.container_name, 'blob1', block_list, if_none_match='0x111111111111111')

        # Assert
        blob = self.bs.get_blob_to_bytes(self.container_name, 'blob1')
        self.assertEqual(blob.content, b'AAABBBCCC')

    @record
    def test_put_block_list_with_if_none_match_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'')
        self.bs.put_block(self.container_name, 'blob1', b'AAA', '1')
        self.bs.put_block(self.container_name, 'blob1', b'BBB', '2')
        self.bs.put_block(self.container_name, 'blob1', b'CCC', '3')
        etag = self.bs.get_blob_properties(self.container_name, 'blob1').properties.etag

        # Act
        with self.assertRaises(AzureHttpError):
            block_list = [BlobBlock(id='1'), BlobBlock(id='2'), BlobBlock(id='3')]
            self.bs.put_block_list(self.container_name, 'blob1', block_list, if_none_match=etag)

        # Assert

    @record
    def test_update_page_with_if_modified(self):
        # Arrange
        self._create_container_and_page_blob(
            self.container_name, 'blob1', 1024)
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))
        data = b'abcdefghijklmnop' * 32

        # Act
        self.pbs.update_page(self.container_name, 'blob1', data, 0, 511, if_modified_since=test_datetime)

        # Assert

    @record
    def test_update_page_with_if_modified_fail(self):
        # Arrange
        self._create_container_and_page_blob(
            self.container_name, 'blob1', 1024)
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))
        data = b'abcdefghijklmnop' * 32

        # Act
        with self.assertRaises(AzureHttpError):
            self.pbs.update_page(self.container_name, 'blob1', data, 0, 511, if_modified_since=test_datetime)

        # Assert

    @record
    def test_update_page_with_if_unmodified(self):
        # Arrange
        self._create_container_and_page_blob(
            self.container_name, 'blob1', 1024)
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))
        data = b'abcdefghijklmnop' * 32

        # Act
        self.pbs.update_page(self.container_name, 'blob1', data, 0, 511, if_unmodified_since=test_datetime)

        # Assert

    @record
    def test_update_page_with_if_unmodified_fail(self):
        # Arrange
        self._create_container_and_page_blob(
            self.container_name, 'blob1', 1024)
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))
        data = b'abcdefghijklmnop' * 32

        # Act
        with self.assertRaises(AzureHttpError):
            self.pbs.update_page(self.container_name, 'blob1', data, 0, 511, if_unmodified_since=test_datetime)

        # Assert

    @record
    def test_update_page_with_if_match(self):
        # Arrange
        self._create_container_and_page_blob(
            self.container_name, 'blob1', 1024)
        data = b'abcdefghijklmnop' * 32
        etag = self.bs.get_blob_properties(self.container_name, 'blob1').properties.etag

        # Act
        self.pbs.update_page(self.container_name, 'blob1', data, 0, 511, if_match=etag)

        # Assert

    @record
    def test_update_page_with_if_match_fail(self):
        # Arrange
        self._create_container_and_page_blob(
            self.container_name, 'blob1', 1024)
        data = b'abcdefghijklmnop' * 32

        # Act
        with self.assertRaises(AzureHttpError):
            self.pbs.update_page(self.container_name, 'blob1', data, 0, 511, if_match='0x111111111111111')

        # Assert

    @record
    def test_update_page_with_if_none_match(self):
        # Arrange
        self._create_container_and_page_blob(
            self.container_name, 'blob1', 1024)
        data = b'abcdefghijklmnop' * 32

        # Act
        self.pbs.update_page(self.container_name, 'blob1', data, 0, 511, if_none_match='0x111111111111111')

        # Assert

    @record
    def test_update_page_with_if_none_match_fail(self):
        # Arrange
        self._create_container_and_page_blob(
            self.container_name, 'blob1', 1024)
        data = b'abcdefghijklmnop' * 32
        etag = self.pbs.get_blob_properties(self.container_name, 'blob1').properties.etag

        # Act
        with self.assertRaises(AzureHttpError):
            self.pbs.update_page(self.container_name, 'blob1', data, 0, 511, if_none_match=etag)

        # Assert

    @record
    def test_get_page_ranges_iter_with_if_modified(self):
        # Arrange
        self._create_container_and_page_blob(
            self.container_name, 'blob1', 2048)
        data = b'abcdefghijklmnop' * 32
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))
        self.pbs.update_page(self.container_name, 'blob1', data, 0, 511)
        self.pbs.update_page(self.container_name, 'blob1', data, 1024, 1535)

        # Act
        ranges = self.pbs.get_page_ranges(self.container_name, 'blob1',
                                          if_modified_since=test_datetime)
        for byte_range in ranges:
            pass

        # Assert
        self.assertEqual(len(ranges), 2)
        self.assertIsInstance(ranges[0], PageRange)
        self.assertIsInstance(ranges[1], PageRange)

    @record
    def test_get_page_ranges_iter_with_if_modified_fail(self):
        # Arrange
        self._create_container_and_page_blob(
            self.container_name, 'blob1', 2048)
        data = b'abcdefghijklmnop' * 32
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))
        self.pbs.update_page(self.container_name, 'blob1', data, 0, 511)
        self.pbs.update_page(self.container_name, 'blob1', data, 1024, 1535)

        # Act
        with self.assertRaises(AzureHttpError):
            self.pbs.get_page_ranges(self.container_name, 'blob1',
                                    if_modified_since=test_datetime)

        # Assert

    @record
    def test_get_page_ranges_iter_with_if_unmodified(self):
        # Arrange
        self._create_container_and_page_blob(
            self.container_name, 'blob1', 2048)
        data = b'abcdefghijklmnop' * 32
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))
        self.pbs.update_page(self.container_name, 'blob1', data, 0, 511)
        self.pbs.update_page(self.container_name, 'blob1', data, 1024, 1535)

        # Act
        ranges = self.pbs.get_page_ranges(self.container_name, 'blob1',
                                         if_unmodified_since=test_datetime)
        for byte_range in ranges:
            pass

        # Assert
        self.assertEqual(len(ranges), 2)
        self.assertIsInstance(ranges[0], PageRange)
        self.assertIsInstance(ranges[1], PageRange)

    @record
    def test_get_page_ranges_iter_with_if_unmodified_fail(self):
        # Arrange
        self._create_container_and_page_blob(
            self.container_name, 'blob1', 2048)
        data = b'abcdefghijklmnop' * 32
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))
        self.pbs.update_page(self.container_name, 'blob1', data, 0, 511)
        self.pbs.update_page(self.container_name, 'blob1', data, 1024, 1535)

        # Act
        with self.assertRaises(AzureHttpError):
            self.pbs.get_page_ranges(self.container_name, 'blob1',
                                    if_unmodified_since=test_datetime)

        # Assert

    @record
    def test_get_page_ranges_iter_with_if_match(self):
        # Arrange
        self._create_container_and_page_blob(
            self.container_name, 'blob1', 2048)
        data = b'abcdefghijklmnop' * 32
        self.pbs.update_page(self.container_name, 'blob1', data, 0, 511)
        self.pbs.update_page(self.container_name, 'blob1', data, 1024, 1535)
        etag = self.pbs.get_blob_properties(self.container_name, 'blob1').properties.etag

        # Act
        ranges = self.pbs.get_page_ranges(self.container_name, 'blob1',
                                         if_match=etag)
        for byte_range in ranges:
            pass

        # Assert
        self.assertEqual(len(ranges), 2)
        self.assertIsInstance(ranges[0], PageRange)
        self.assertIsInstance(ranges[1], PageRange)

    @record
    def test_get_page_ranges_iter_with_if_match_fail(self):
        # Arrange
        self._create_container_and_page_blob(
            self.container_name, 'blob1', 2048)
        data = b'abcdefghijklmnop' * 32
        self.pbs.update_page(self.container_name, 'blob1', data, 0, 511)
        self.pbs.update_page(self.container_name, 'blob1', data, 1024, 1535)

        # Act
        with self.assertRaises(AzureHttpError):
            self.pbs.get_page_ranges(self.container_name, 'blob1',
                                    if_match='0x111111111111111')

        # Assert

    @record
    def test_get_page_ranges_iter_with_if_none_match(self):
        # Arrange
        self._create_container_and_page_blob(
            self.container_name, 'blob1', 2048)
        data = b'abcdefghijklmnop' * 32
        self.pbs.update_page(self.container_name, 'blob1', data, 0, 511)
        self.pbs.update_page(self.container_name, 'blob1', data, 1024, 1535)

        # Act
        ranges = self.pbs.get_page_ranges(self.container_name, 'blob1',
                                         if_none_match='0x111111111111111')
        for byte_range in ranges:
            pass

        # Assert
        self.assertEqual(len(ranges), 2)
        self.assertIsInstance(ranges[0], PageRange)
        self.assertIsInstance(ranges[1], PageRange)

    @record
    def test_get_page_ranges_iter_with_if_none_match_fail(self):
        # Arrange
        self._create_container_and_page_blob(
            self.container_name, 'blob1', 2048)
        data = b'abcdefghijklmnop' * 32
        self.pbs.update_page(self.container_name, 'blob1', data, 0, 511)
        self.pbs.update_page(self.container_name, 'blob1', data, 1024, 1535)
        etag = self.pbs.get_blob_properties(self.container_name, 'blob1').properties.etag

        # Act
        with self.assertRaises(AzureHttpError):
            self.pbs.get_page_ranges(self.container_name, 'blob1',
                                    if_none_match=etag)

        # Assert

    @record
    def test_append_block_with_if_modified(self):
        # Arrange
        self._create_container_and_append_blob(self.container_name, 'blob1')
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))
        # Act
        for i in range(5):
            resp = self.abs.append_block(
                self.container_name, 'blob1',
                u'block {0}'.format(i).encode('utf-8'),
                if_modified_since=test_datetime)
            self.assertIsNotNone(resp)

        # Assert
        blob = self.bs.get_blob_to_bytes(self.container_name, 'blob1')
        self.assertEqual(b'block 0block 1block 2block 3block 4', blob.content)

    @record
    def test_append_block_with_if_modified_fail(self):
        # Arrange
        self._create_container_and_append_blob(self.container_name, 'blob1')
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))
        # Act
        with self.assertRaises(AzureHttpError):
            for i in range(5):
                resp = self.abs.append_block(
                    self.container_name, 'blob1',
                    u'block {0}'.format(i).encode('utf-8'),
                    if_modified_since=test_datetime)

        # Assert

    @record
    def test_append_block_with_if_unmodified(self):
        # Arrange
        self._create_container_and_append_blob(self.container_name, 'blob1')
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))
        # Act
        for i in range(5):
            resp = self.abs.append_block(
                self.container_name, 'blob1',
                u'block {0}'.format(i).encode('utf-8'),
                if_unmodified_since=test_datetime)
            self.assertIsNotNone(resp)

        # Assert
        blob = self.bs.get_blob_to_bytes(self.container_name, 'blob1')
        self.assertEqual(b'block 0block 1block 2block 3block 4', blob.content)

    @record
    def test_append_block_with_if_unmodified_fail(self):
        # Arrange
        self._create_container_and_append_blob(self.container_name, 'blob1')
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))
        # Act
        with self.assertRaises(AzureHttpError):
            for i in range(5):
                resp = self.abs.append_block(
                    self.container_name, 'blob1',
                    u'block {0}'.format(i).encode('utf-8'),
                    if_unmodified_since=test_datetime)

        # Assert

    @record
    def test_append_block_with_if_match(self):
        # Arrange
        self._create_container_and_append_blob(self.container_name, 'blob1')

        # Act
        for i in range(5):
            etag = self.abs.get_blob_properties(self.container_name, 'blob1').properties.etag
            resp = self.abs.append_block(
                self.container_name, 'blob1',
                u'block {0}'.format(i).encode('utf-8'),
                if_match=etag)
            self.assertIsNotNone(resp)

        # Assert
        blob = self.bs.get_blob_to_bytes(self.container_name, 'blob1')
        self.assertEqual(b'block 0block 1block 2block 3block 4', blob.content)

    @record
    def test_append_block_with_if_match_fail(self):
        # Arrange
        self._create_container_and_append_blob(self.container_name, 'blob1')

        # Act
        with self.assertRaises(AzureHttpError):
            for i in range(5):
                resp = self.abs.append_block(
                    self.container_name, 'blob1',
                    u'block {0}'.format(i).encode('utf-8'),
                    if_match='0x111111111111111')

        # Assert

    @record
    def test_append_block_with_if_none_match(self):
        # Arrange
        self._create_container_and_append_blob(self.container_name, 'blob1')

        # Act
        for i in range(5):
            resp = self.abs.append_block(
                self.container_name, 'blob1',
                u'block {0}'.format(i).encode('utf-8'),
                if_none_match='0x8D2C9167D53FC2C')
            self.assertIsNotNone(resp)

        # Assert
        blob = self.bs.get_blob_to_bytes(self.container_name, 'blob1')
        self.assertEqual(b'block 0block 1block 2block 3block 4', blob.content)

    @record
    def test_append_block_with_if_none_match_fail(self):
        # Arrange
        self._create_container_and_append_blob(self.container_name, 'blob1')

        # Act
        with self.assertRaises(AzureHttpError):
            for i in range(5):
                etag = self.abs.get_blob_properties(self.container_name, 'blob1').properties.etag
                resp = self.abs.append_block(
                    self.container_name, 'blob1',
                    u'block {0}'.format(i).encode('utf-8'),
                    if_none_match=etag)

        # Assert

#------------------------------------------------------------------------------
if __name__ == '__main__':
    unittest.main()