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

class StorageBlobAccessConditionsTest(StorageTestCase):

    def setUp(self):
        super(StorageBlobAccessConditionsTest, self).setUp()

        self.bs = self._create_storage_service(BlockBlobService, self.settings)
        self.pbs = self._create_storage_service(PageBlobService, self.settings)
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
        resp = self.bs.put_blob(container_name, blob_name, blob_data)
        self.assertIsNone(resp)

    def _create_container_and_page_blob(self, container_name, blob_name,
                                        content_length):
        self._create_container(container_name)
        resp = self.pbs.put_blob(self.container_name, blob_name, str(content_length))
        self.assertIsNone(resp)

    class NonSeekableFile(object):
        def __init__(self, wrapped_file):
            self.wrapped_file = wrapped_file

        def write(self, data):
            self.wrapped_file.write(data)

        def read(self, count):
            return self.wrapped_file.read(count)

    #--Test cases for blob service --------------------------------------------
    @record
    def test_set_container_metadata_with_if_modified(self):    
        # Arrange
        self.bs.create_container(self.container_name)
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')

        # Act
        resp = self.bs.set_container_metadata(
            self.container_name,
            {'hello': 'world', 'number': '43'},
            if_modified_since=test_datetime)

        # Assert
        self.assertIsNone(resp)
        md = self.bs.get_container_metadata(self.container_name)
        self.assertIsNotNone(md)
        self.assertEqual(md['x-ms-meta-hello'], 'world')
        self.assertEqual(md['x-ms-meta-number'], '43')

    @record
    def test_set_container_metadata_with_if_modified_fail(self):    
        # Arrange
        contianer_name = self.container_name
        self.bs.create_container(self.container_name)
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.set_container_metadata(
                self.container_name,
                {'hello': 'world', 'number': '43'},
                if_modified_since=test_datetime)

        # Assert

    @record
    def test_set_container_acl_with_if_modified(self):
        # Arrange
        self.bs.create_container(self.container_name)
        test_datetime = (datetime.datetime.utcnow() -
					datetime.timedelta(minutes=15))\
                   .strftime('%a, %d %b %Y %H:%M:%S GMT')
        # Act
        resp = self.bs.set_container_acl(self.container_name,
                                         if_modified_since=test_datetime) 

        # Assert
        self.assertIsNone(resp)
        acl = self.bs.get_container_acl(self.container_name)
        self.assertIsNotNone(acl)

    @record
    def test_set_container_acl_with_if_modified_fail(self):
        # Arrange
        self.bs.create_container(self.container_name)
        test_datetime = (datetime.datetime.utcnow() +
					datetime.timedelta(minutes=15))\
                   .strftime('%a, %d %b %Y %H:%M:%S GMT')
        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.set_container_acl(self.container_name,
                                      if_modified_since=test_datetime) 

        # Assert

    @record
    def test_set_container_acl_with_if_unmodified(self):
        # Arrange
        self.bs.create_container(self.container_name)
        test_datetime = (datetime.datetime.utcnow() +
                    datetime.timedelta(minutes=15))\
                   .strftime('%a, %d %b %Y %H:%M:%S GMT')
        # Act
        resp = self.bs.set_container_acl(self.container_name,
                                         if_unmodified_since=test_datetime) 

        # Assert
        self.assertIsNone(resp)
        acl = self.bs.get_container_acl(self.container_name)
        self.assertIsNotNone(acl)

    @record
    def test_set_container_acl_with_if_unmodified_fail(self):
        # Arrange
        self.bs.create_container(self.container_name)
        test_datetime = (datetime.datetime.utcnow() -
                    datetime.timedelta(minutes=15))\
                   .strftime('%a, %d %b %Y %H:%M:%S GMT')
        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.set_container_acl(self.container_name,
                                      if_unmodified_since=test_datetime) 

        # Assert

    @record
    def test_lease_container_acquire_with_if_modified(self):
        import random
        # Arrange
        self.bs.create_container(self.container_name)
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')

        # Act
        lease = self.bs.acquire_container_lease(self.container_name,
                                                if_modified_since=test_datetime) 
        self.bs.break_container_lease(self.container_name,
                                      x_ms_lease_id=lease['x-ms-lease-id'])

        # Assert
        self.assertIsNotNone(lease)
        self.assertIsNotNone(lease['x-ms-lease-id'])

    @record
    def test_lease_container_acquire_with_if_modified_fail(self):
        # Arrange
        self.bs.create_container(self.container_name)
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')

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
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')

        # Act
        lease = self.bs.acquire_container_lease(self.container_name,
                                                if_unmodified_since=test_datetime) 
        self.bs.break_container_lease(
            self.container_name,
            x_ms_lease_id=lease['x-ms-lease-id'])

        # Assert
        self.assertIsNotNone(lease)
        self.assertIsNotNone(lease['x-ms-lease-id'])

    @record
    def test_lease_container_acquire_with_if_unmodified_fail(self):
        # Arrange
        self.bs.create_container(self.container_name)
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')

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
					datetime.timedelta(minutes=15))\
                   .strftime('%a, %d %b %Y %H:%M:%S GMT')
        # Act
        deleted = self.bs.delete_container(self.container_name,
                                           if_modified_since=test_datetime) 

        # Assert
        self.assertTrue(deleted)
        containers = self.bs.list_containers()
        self.assertNamedItemNotInContainer(containers, self.container_name)

    @record
    def test_delete_container_with_if_modified_fail(self):
        # Arrange
        self.bs.create_container(self.container_name)
        test_datetime = (datetime.datetime.utcnow() +
					datetime.timedelta(minutes=15))\
                   .strftime('%a, %d %b %Y %H:%M:%S GMT')
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
					datetime.timedelta(minutes=15))\
                   .strftime('%a, %d %b %Y %H:%M:%S GMT')
        # Act
        deleted = self.bs.delete_container(self.container_name,
                                           if_unmodified_since=test_datetime) 

        # Assert
        self.assertTrue(deleted)
        containers = self.bs.list_containers()
        self.assertNamedItemNotInContainer(containers, self.container_name)

    @record
    def test_delete_container_with_if_unmodified_fail(self):
        # Arrange
        self.bs.create_container(self.container_name)
        test_datetime = (datetime.datetime.utcnow() -
					datetime.timedelta(minutes=15))\
                   .strftime('%a, %d %b %Y %H:%M:%S GMT')
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
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')

        # Act
        resp = self.bs.put_blob(
            self.container_name, 'blob1', data, 'BlockBlob',
            if_modified_since=test_datetime)

        # Assert
        self.assertIsNone(resp)

    @record
    def test_put_blob_with_if_modified_fail(self):
        # Arrange
        data = b'hello world'
        self._create_container_and_block_blob(
            self.container_name, 'blob1', data)
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.put_blob(
                self.container_name, 'blob1', data, 'BlockBlob',
                if_modified_since=test_datetime)

        # Assert

    @record
    def test_put_blob_with_if_unmodified(self):
        # Arrange
        data = b'hello world'
        self._create_container_and_block_blob(
            self.container_name, 'blob1', data)
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')

        # Act
        resp = self.bs.put_blob(
            self.container_name, 'blob1', data, 'BlockBlob',
            if_unmodified_since=test_datetime)

        # Assert
        self.assertIsNone(resp)

    @record
    def test_put_blob_with_if_unmodified_fail(self):
        # Arrange
        data = b'hello world'
        self._create_container_and_block_blob(
            self.container_name, 'blob1', data)
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.put_blob(
                self.container_name, 'blob1', data, 'BlockBlob',
                if_unmodified_since=test_datetime)

        # Assert

    @record
    def test_put_blob_with_if_match(self):
        # Arrange
        data = b'hello world'
        self._create_container_and_block_blob(
            self.container_name, 'blob1', data)
        etag = self.bs.get_blob_properties(self.container_name, 'blob1')['ETag']
        
        # Act
        resp = self.bs.put_blob(
            self.container_name, 'blob1', data, 'BlockBlob',
            if_match=etag)

        # Assert
        self.assertIsNone(resp)

    @record
    def test_put_blob_with_if_match_fail(self):
        # Arrange
        data = b'hello world'
        self._create_container_and_block_blob(
            self.container_name, 'blob1', data)
        
        # Act
        with self.assertRaises(AzureHttpError):
            resp = self.bs.put_blob(
                self.container_name, 'blob1', data, 'BlockBlob',
                if_match='0x111111111111111')

        # Assert

    @record
    def test_put_blob_with_if_none_match(self):
        # Arrange
        data = b'hello world'
        self._create_container_and_block_blob(
            self.container_name, 'blob1', data)
        
        # Act
        resp = self.bs.put_blob(
            self.container_name, 'blob1', data, 'BlockBlob',
            if_none_match='0x111111111111111')

        # Assert
        self.assertIsNone(resp)

    @record
    def test_put_blob_with_if_none_match_fail(self):
        # Arrange
        data = b'hello world'
        self._create_container_and_block_blob(
            self.container_name, 'blob1', data)
        etag = self.bs.get_blob_properties(self.container_name, 'blob1')['ETag']

        # Act
        with self.assertRaises(AzureHttpError):
            resp = self.bs.put_blob(
                self.container_name, 'blob1', data, 'BlockBlob',
                if_none_match=etag)

        # Assert

    @record
    def test_get_blob_with_if_modified(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')

        # Act
        blob = self.bs.get_blob(self.container_name, 'blob1',
                                if_modified_since=test_datetime)

        # Assert
        self.assertIsInstance(blob, BlobResult)
        self.assertEqual(blob, b'hello world')

    @record
    def test_get_blob_with_if_modified_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.get_blob(self.container_name, 'blob1',
                             if_modified_since=test_datetime)

        # Assert

    @record
    def test_get_blob_with_if_unmodified(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')

        # Act
        blob = self.bs.get_blob(self.container_name, 'blob1',
                                if_unmodified_since=test_datetime)

        # Assert
        self.assertIsInstance(blob, BlobResult)
        self.assertEqual(blob, b'hello world')

    @record
    def test_get_blob_with_if_unmodified_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.get_blob(self.container_name, 'blob1',
                             if_unmodified_since=test_datetime)

        # Assert

    @record
    def test_get_blob_with_if_match(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        etag = self.bs.get_blob_properties(self.container_name, 'blob1')['ETag']

        # Act
        blob = self.bs.get_blob(self.container_name, 'blob1', if_match=etag)

        # Assert
        self.assertIsInstance(blob, BlobResult)
        self.assertEqual(blob, b'hello world')

    @record
    def test_get_blob_with_if_match_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.get_blob(self.container_name, 'blob1',
                             if_match='0x111111111111111')

        # Assert

    @record
    def test_get_blob_with_if_none_match(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')

        # Act
        blob = self.bs.get_blob(self.container_name, 'blob1',
                                if_none_match='0x111111111111111')

        # Assert
        self.assertIsInstance(blob, BlobResult)
        self.assertEqual(blob, b'hello world')

    @record
    def test_get_blob_with_if_none_match_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        etag = self.bs.get_blob_properties(self.container_name, 'blob1')['ETag']

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.get_blob(self.container_name, 'blob1',
                             if_none_match=etag)

        # Assert

    @record
    def test_set_blob_properties_with_if_modified(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')
        # Act
        resp = self.bs.set_blob_properties(
            self.container_name,
            'blob1',
            x_ms_blob_content_language='spanish',
            x_ms_blob_content_disposition='inline',
            if_modified_since=test_datetime,
        )

        # Assert
        self.assertIsNone(resp)
        props = self.bs.get_blob_properties(self.container_name, 'blob1')
        self.assertEqual(props['content-language'], 'spanish')
        self.assertEqual(props['content-disposition'], 'inline')

    @record
    def test_set_blob_properties_with_if_modified_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')
        # Act
        with self.assertRaises(AzureHttpError):
            resp = self.bs.set_blob_properties(
                self.container_name,
                'blob1',
                x_ms_blob_content_language='spanish',
                x_ms_blob_content_disposition='inline',
                if_modified_since=test_datetime,
            )

        # Assert

    @record
    def test_set_blob_properties_with_if_unmodified(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')
        # Act
        resp = self.bs.set_blob_properties(
            self.container_name,
            'blob1',
            x_ms_blob_content_language='spanish',
            x_ms_blob_content_disposition='inline',
            if_unmodified_since=test_datetime,
        )

        # Assert
        self.assertIsNone(resp)
        props = self.bs.get_blob_properties(self.container_name, 'blob1')
        self.assertEqual(props['content-language'], 'spanish')
        self.assertEqual(props['content-disposition'], 'inline')

    @record
    def test_set_blob_properties_with_if_unmodified_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')
        # Act
        with self.assertRaises(AzureHttpError):
            resp = self.bs.set_blob_properties(
                self.container_name,
                'blob1',
                x_ms_blob_content_language='spanish',
                x_ms_blob_content_disposition='inline',
                if_unmodified_since=test_datetime,
            )

        # Assert

    @record
    def test_set_blob_properties_with_if_match(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        etag = self.bs.get_blob_properties(self.container_name, 'blob1')['ETag']

        # Act
        resp = self.bs.set_blob_properties(
            self.container_name,
            'blob1',
            x_ms_blob_content_language='spanish',
            x_ms_blob_content_disposition='inline',
            if_match=etag,
        )

        # Assert
        self.assertIsNone(resp)
        props = self.bs.get_blob_properties(self.container_name, 'blob1')
        self.assertEqual(props['content-language'], 'spanish')
        self.assertEqual(props['content-disposition'], 'inline')

    @record
    def test_set_blob_properties_with_if_match_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')

        # Act
        with self.assertRaises(AzureHttpError):
            resp = self.bs.set_blob_properties(
                self.container_name,
                'blob1',
                x_ms_blob_content_language='spanish',
                x_ms_blob_content_disposition='inline',
                if_match='0x111111111111111',
            )

        # Assert

    @record
    def test_set_blob_properties_with_if_none_match(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')

        # Act
        resp = self.bs.set_blob_properties(
            self.container_name,
            'blob1',
            x_ms_blob_content_language='spanish',
            x_ms_blob_content_disposition='inline',
            if_none_match='0x111111111111111',
        )

        # Assert
        self.assertIsNone(resp)
        props = self.bs.get_blob_properties(self.container_name, 'blob1')
        self.assertEqual(props['content-language'], 'spanish')
        self.assertEqual(props['content-disposition'], 'inline')

    @record
    def test_set_blob_properties_with_if_none_match_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        etag = self.bs.get_blob_properties(self.container_name, 'blob1')['ETag']

        # Act
        with self.assertRaises(AzureHttpError):
            resp = self.bs.set_blob_properties(
                self.container_name,
                'blob1',
                x_ms_blob_content_language='spanish',
                x_ms_blob_content_disposition='inline',
                if_none_match=etag,
            )

        # Assert

    @record
    def test_get_blob_properties_with_if_modified(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')
        # Act
        props = self.bs.get_blob_properties(self.container_name, 'blob1',
                                            if_modified_since=test_datetime)

        # Assert
        self.assertIsNotNone(props)
        self.assertEqual(props['x-ms-blob-type'], 'BlockBlob')
        self.assertEqual(props['content-length'], '11')
        self.assertEqual(props['x-ms-lease-status'], 'unlocked')

    @record
    def test_get_blob_properties_with_if_modified_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')
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
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')
        # Act
        props = self.bs.get_blob_properties(self.container_name, 'blob1',
                                            if_unmodified_since=test_datetime)

        # Assert
        self.assertIsNotNone(props)
        self.assertEqual(props['x-ms-blob-type'], 'BlockBlob')
        self.assertEqual(props['content-length'], '11')
        self.assertEqual(props['x-ms-lease-status'], 'unlocked')

    @record
    def test_get_blob_properties_with_if_unmodified_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')
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
        etag = self.bs.get_blob_properties(self.container_name, 'blob1')['ETag']

        # Act
        props = self.bs.get_blob_properties(self.container_name, 'blob1',
                                            if_match=etag)

        # Assert
        self.assertIsNotNone(props)
        self.assertEqual(props['x-ms-blob-type'], 'BlockBlob')
        self.assertEqual(props['content-length'], '11')
        self.assertEqual(props['x-ms-lease-status'], 'unlocked')

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
        props = self.bs.get_blob_properties(self.container_name, 'blob1',
                                            if_none_match='0x111111111111111')

        # Assert
        self.assertIsNotNone(props)
        self.assertEqual(props['x-ms-blob-type'], 'BlockBlob')
        self.assertEqual(props['content-length'], '11')
        self.assertEqual(props['x-ms-lease-status'], 'unlocked')

    @record
    def test_get_blob_properties_with_if_none_match_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        etag = self.bs.get_blob_properties(self.container_name, 'blob1')['ETag']

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
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')

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
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')

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
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')

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
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')

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
        etag = self.bs.get_blob_properties(self.container_name, 'blob1')['ETag']

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
        etag = self.bs.get_blob_properties(self.container_name, 'blob1')['ETag']

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
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')

        # Act
        resp = self.bs.set_blob_metadata(
            self.container_name,
            'blob1',
            {'hello': 'world', 'number': '42', 'UP': 'UPval'},
            if_modified_since=test_datetime)

        # Assert
        self.assertIsNone(resp)
        md = self.bs.get_blob_metadata(self.container_name, 'blob1')
        self.assertEqual(3, len(md))
        self.assertEqual(md['x-ms-meta-hello'], 'world')
        self.assertEqual(md['x-ms-meta-number'], '42')
        self.assertEqual(md['x-ms-meta-up'], 'UPval')

    @record
    def test_set_blob_metadata_with_if_modified_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.set_blob_metadata(
                self.container_name,
                'blob1',
                {'hello': 'world', 'number': '42', 'UP': 'UPval'},
                if_modified_since=test_datetime)

        # Assert

    @record
    def test_set_blob_metadata_with_if_unmodified(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')

        # Act
        resp = self.bs.set_blob_metadata(
            self.container_name,
            'blob1',
            {'hello': 'world', 'number': '42', 'UP': 'UPval'},
            if_unmodified_since=test_datetime)

        # Assert
        self.assertIsNone(resp)
        md = self.bs.get_blob_metadata(self.container_name, 'blob1')
        self.assertEqual(3, len(md))
        self.assertEqual(md['x-ms-meta-hello'], 'world')
        self.assertEqual(md['x-ms-meta-number'], '42')
        self.assertEqual(md['x-ms-meta-up'], 'UPval')

    @record
    def test_set_blob_metadata_with_if_unmodified_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.set_blob_metadata(
                self.container_name,
                'blob1',
                {'hello': 'world', 'number': '42', 'UP': 'UPval'},
                if_unmodified_since=test_datetime)

        # Assert

    @record
    def test_set_blob_metadata_with_if_match(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        etag = self.bs.get_blob_properties(self.container_name, 'blob1')['ETag']
        
        # Act
        resp = self.bs.set_blob_metadata(
            self.container_name,
            'blob1',
            {'hello': 'world', 'number': '42', 'UP': 'UPval'},
            if_match=etag)

        # Assert
        self.assertIsNone(resp)
        md = self.bs.get_blob_metadata(self.container_name, 'blob1')
        self.assertEqual(3, len(md))
        self.assertEqual(md['x-ms-meta-hello'], 'world')
        self.assertEqual(md['x-ms-meta-number'], '42')
        self.assertEqual(md['x-ms-meta-up'], 'UPval')

    @record
    def test_set_blob_metadata_with_if_match_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        
        # Act
        with self.assertRaises(AzureHttpError):
                self.bs.set_blob_metadata(
                    self.container_name,
                    'blob1',
                    {'hello': 'world', 'number': '42', 'UP': 'UPval'},
                    if_match='0x111111111111111')

        # Assert

    @record
    def test_set_blob_metadata_with_if_none_match(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        
        # Act
        resp = self.bs.set_blob_metadata(
            self.container_name,
            'blob1',
            {'hello': 'world', 'number': '42', 'UP': 'UPval'},
            if_none_match='0x111111111111111')

        # Assert
        self.assertIsNone(resp)
        md = self.bs.get_blob_metadata(self.container_name, 'blob1')
        self.assertEqual(3, len(md))
        self.assertEqual(md['x-ms-meta-hello'], 'world')
        self.assertEqual(md['x-ms-meta-number'], '42')
        self.assertEqual(md['x-ms-meta-up'], 'UPval')

    @record
    def test_set_blob_metadata_with_if_none_match_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        etag = self.bs.get_blob_properties(self.container_name, 'blob1')['ETag']
        
        # Act
        with self.assertRaises(AzureHttpError):
                self.bs.set_blob_metadata(
                    self.container_name,
                    'blob1',
                    {'hello': 'world', 'number': '42', 'UP': 'UPval'},
                    if_none_match=etag)

        # Assert

    @record
    def test_delete_blob_with_if_modified(self):
        # Arrange
        test_datetime = (datetime.datetime.utcnow() -
                    datetime.timedelta(minutes=15))\
                .strftime('%a, %d %b %Y %H:%M:%S GMT')
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
                    datetime.timedelta(minutes=15))\
                .strftime('%a, %d %b %Y %H:%M:%S GMT')
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
                    datetime.timedelta(minutes=15))\
                .strftime('%a, %d %b %Y %H:%M:%S GMT')
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
                    datetime.timedelta(minutes=15))\
                .strftime('%a, %d %b %Y %H:%M:%S GMT')
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
        etag = self.bs.get_blob_properties(self.container_name, 'blob1')['ETag']

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
        etag = self.bs.get_blob_properties(self.container_name, 'blob1')['ETag']

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
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')

        # Act
        resp = self.bs.snapshot_blob(self.container_name, 'blob1',
                                     if_modified_since=test_datetime)

        # Assert
        self.assertIsNotNone(resp)
        self.assertIsNotNone(resp['x-ms-snapshot'])

    @record
    def test_snapshot_blob_with_if_modified_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')

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
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')

        # Act
        resp = self.bs.snapshot_blob(self.container_name, 'blob1',
                                     if_unmodified_since=test_datetime)

        # Assert
        self.assertIsNotNone(resp)
        self.assertIsNotNone(resp['x-ms-snapshot'])

    @record
    def test_snapshot_blob_with_if_unmodified_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')

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
        etag = self.bs.get_blob_properties(self.container_name, 'blob1')['ETag']

        # Act
        resp = self.bs.snapshot_blob(self.container_name, 'blob1',
                                     if_match=etag)

        # Assert
        self.assertIsNotNone(resp)
        self.assertIsNotNone(resp['x-ms-snapshot'])

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
        self.assertIsNotNone(resp['x-ms-snapshot'])

    @record
    def test_snapshot_blob_with_if_none_match_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        etag = self.bs.get_blob_properties(self.container_name, 'blob1')['ETag']

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
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')

        # Act
        resp1 = self.bs.acquire_blob_lease(
            self.container_name, 'blob1',
            if_modified_since=test_datetime,
            x_ms_proposed_lease_id=test_lease_id)

        self.bs.break_blob_lease(
            self.container_name, 'blob1',
            x_ms_lease_id=test_lease_id)

        # Assert
        self.assertIsNotNone(resp1)

    @record
    def test_lease_blob_with_if_modified_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')

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
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')

        # Act
        resp1 = self.bs.acquire_blob_lease(
            self.container_name, 'blob1',
            if_unmodified_since=test_datetime,
            x_ms_proposed_lease_id=test_lease_id)

        self.bs.break_blob_lease(
            self.container_name, 'blob1',
            x_ms_lease_id=test_lease_id)

        # Assert
        self.assertIsNotNone(resp1)

    @record
    def test_lease_blob_with_if_unmodified_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')

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
        etag = self.bs.get_blob_properties(self.container_name, 'blob1')['ETag']
        test_lease_id = '00000000-1111-2222-3333-444444444444'

        # Act
        resp1 = self.bs.acquire_blob_lease(
            self.container_name, 'blob1',
            x_ms_proposed_lease_id=test_lease_id,
            if_match=etag)

        self.bs.break_blob_lease(
            self.container_name, 'blob1',
            x_ms_lease_id=test_lease_id)

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
            x_ms_proposed_lease_id=test_lease_id,
            if_none_match='0x111111111111111')

        self.bs.break_blob_lease(
            self.container_name, 'blob1',
            x_ms_lease_id=test_lease_id)


        # Assert
        self.assertIsNotNone(resp1)

    @record
    def test_lease_blob_with_if_none_match_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'hello world')
        etag = self.bs.get_blob_properties(self.container_name, 'blob1')['ETag']

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
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')

        # Act
        resp = self.bs.put_block_list(
            self.container_name, 'blob1', ['1', '2', '3'],
            if_modified_since=test_datetime)

        # Assert
        self.assertIsNone(resp)
        blob = self.bs.get_blob(self.container_name, 'blob1')
        self.assertEqual(blob, b'AAABBBCCC')

    @record
    def test_put_block_list_with_if_modified_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'')
        self.bs.put_block(self.container_name, 'blob1', b'AAA', '1')
        self.bs.put_block(self.container_name, 'blob1', b'BBB', '2')
        self.bs.put_block(self.container_name, 'blob1', b'CCC', '3')
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.put_block_list(
                self.container_name, 'blob1', ['1', '2', '3'],
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
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')

        # Act
        resp = self.bs.put_block_list(
            self.container_name, 'blob1', ['1', '2', '3'],
            if_unmodified_since=test_datetime)

        # Assert
        self.assertIsNone(resp)
        blob = self.bs.get_blob(self.container_name, 'blob1')
        self.assertEqual(blob, b'AAABBBCCC')

    @record
    def test_put_block_list_with_if_unmodified_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'')
        self.bs.put_block(self.container_name, 'blob1', b'AAA', '1')
        self.bs.put_block(self.container_name, 'blob1', b'BBB', '2')
        self.bs.put_block(self.container_name, 'blob1', b'CCC', '3')
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.put_block_list(
                self.container_name, 'blob1', ['1', '2', '3'],
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
        etag = self.bs.get_blob_properties(self.container_name, 'blob1')['ETag']

        # Act
        resp = self.bs.put_block_list(
            self.container_name, 'blob1', ['1', '2', '3'],
            if_match=etag)

        # Assert
        self.assertIsNone(resp)
        blob = self.bs.get_blob(self.container_name, 'blob1')
        self.assertEqual(blob, b'AAABBBCCC')

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
                self.container_name, 'blob1', ['1', '2', '3'],
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
        resp = self.bs.put_block_list(
            self.container_name, 'blob1', ['1', '2', '3'],
            if_none_match='0x111111111111111')

        # Assert
        self.assertIsNone(resp)
        blob = self.bs.get_blob(self.container_name, 'blob1')
        self.assertEqual(blob, b'AAABBBCCC')

    @record
    def test_put_block_list_with_if_none_match_fail(self):
        # Arrange
        self._create_container_and_block_blob(
            self.container_name, 'blob1', b'')
        self.bs.put_block(self.container_name, 'blob1', b'AAA', '1')
        self.bs.put_block(self.container_name, 'blob1', b'BBB', '2')
        self.bs.put_block(self.container_name, 'blob1', b'CCC', '3')
        etag = self.bs.get_blob_properties(self.container_name, 'blob1')['ETag']

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.put_block_list(
                self.container_name, 'blob1', ['1', '2', '3'],
                if_none_match=etag)

        # Assert

    @record
    def test_put_page_with_if_modified(self):
        # Arrange
        self._create_container_and_page_blob(
            self.container_name, 'blob1', 1024)
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')
        data = b'abcdefghijklmnop' * 32

        # Act
        resp = self.pbs.put_page(
            self.container_name, 'blob1', data, 'bytes=0-511', 'update',
            if_modified_since=test_datetime)

        # Assert
        self.assertIsNone(resp)

    @record
    def test_put_page_with_if_modified_fail(self):
        # Arrange
        self._create_container_and_page_blob(
            self.container_name, 'blob1', 1024)
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')
        data = b'abcdefghijklmnop' * 32

        # Act
        with self.assertRaises(AzureHttpError):
            self.pbs.put_page(
                self.container_name, 'blob1', data, 'bytes=0-511', 'update',
                if_modified_since=test_datetime)

        # Assert

    @record
    def test_put_page_with_if_unmodified(self):
        # Arrange
        self._create_container_and_page_blob(
            self.container_name, 'blob1', 1024)
        test_datetime = (datetime.datetime.utcnow() +
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')
        data = b'abcdefghijklmnop' * 32

        # Act
        resp = self.pbs.put_page(
            self.container_name, 'blob1', data, 'bytes=0-511', 'update',
            if_unmodified_since=test_datetime)

        # Assert
        self.assertIsNone(resp)

    @record
    def test_put_page_with_if_unmodified_fail(self):
        # Arrange
        self._create_container_and_page_blob(
            self.container_name, 'blob1', 1024)
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')
        data = b'abcdefghijklmnop' * 32

        # Act
        with self.assertRaises(AzureHttpError):
            self.pbs.put_page(
                self.container_name, 'blob1', data, 'bytes=0-511', 'update',
                if_unmodified_since=test_datetime)

        # Assert

    @record
    def test_put_page_with_if_match(self):
        # Arrange
        self._create_container_and_page_blob(
            self.container_name, 'blob1', 1024)
        data = b'abcdefghijklmnop' * 32
        etag = self.bs.get_blob_properties(self.container_name, 'blob1')['ETag']

        # Act
        resp = self.pbs.put_page(
            self.container_name, 'blob1', data, 'bytes=0-511', 'update',
            if_match=etag)

        # Assert
        self.assertIsNone(resp)

    @record
    def test_put_page_with_if_match_fail(self):
        # Arrange
        self._create_container_and_page_blob(
            self.container_name, 'blob1', 1024)
        data = b'abcdefghijklmnop' * 32

        # Act
        with self.assertRaises(AzureHttpError):
            self.pbs.put_page(
                self.container_name, 'blob1', data, 'bytes=0-511', 'update',
                if_match='0x111111111111111')

        # Assert

    @record
    def test_put_page_with_if_none_match(self):
        # Arrange
        self._create_container_and_page_blob(
            self.container_name, 'blob1', 1024)
        data = b'abcdefghijklmnop' * 32

        # Act
        resp = self.pbs.put_page(
            self.container_name, 'blob1', data, 'bytes=0-511', 'update',
            if_none_match='0x111111111111111')

        # Assert
        self.assertIsNone(resp)

    @record
    def test_put_page_with_if_none_match_fail(self):
        # Arrange
        self._create_container_and_page_blob(
            self.container_name, 'blob1', 1024)
        data = b'abcdefghijklmnop' * 32
        etag = self.pbs.get_blob_properties(self.container_name, 'blob1')['ETag']

        # Act
        with self.assertRaises(AzureHttpError):
            self.pbs.put_page(
                self.container_name, 'blob1', data, 'bytes=0-511', 'update',
                if_none_match=etag)

        # Assert

    @record
    def test_get_page_ranges_iter_with_if_modified(self):
        # Arrange
        self._create_container_and_page_blob(
            self.container_name, 'blob1', 2048)
        data = b'abcdefghijklmnop' * 32
        test_datetime = (datetime.datetime.utcnow() -
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')
        resp1 = self.pbs.put_page(
            self.container_name, 'blob1', data, 'bytes=0-511', 'update')
        resp2 = self.pbs.put_page(
            self.container_name, 'blob1', data, 'bytes=1024-1535', 'update')

        # Act
        ranges = self.pbs.get_page_ranges(self.container_name, 'blob1',
                                         if_modified_since=test_datetime)
        for range in ranges:
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
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')
        resp1 = self.pbs.put_page(
            self.container_name, 'blob1', data, 'bytes=0-511', 'update')
        resp2 = self.pbs.put_page(
            self.container_name, 'blob1', data, 'bytes=1024-1535', 'update')

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
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')
        resp1 = self.pbs.put_page(
            self.container_name, 'blob1', data, 'bytes=0-511', 'update')
        resp2 = self.pbs.put_page(
            self.container_name, 'blob1', data, 'bytes=1024-1535', 'update')

        # Act
        ranges = self.pbs.get_page_ranges(self.container_name, 'blob1',
                                         if_unmodified_since=test_datetime)
        for range in ranges:
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
                         datetime.timedelta(minutes=15))\
                        .strftime('%a, %d %b %Y %H:%M:%S GMT')
        resp1 = self.pbs.put_page(
            self.container_name, 'blob1', data, 'bytes=0-511', 'update')
        resp2 = self.pbs.put_page(
            self.container_name, 'blob1', data, 'bytes=1024-1535', 'update')

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
        resp1 = self.pbs.put_page(
            self.container_name, 'blob1', data, 'bytes=0-511', 'update')
        resp2 = self.pbs.put_page(
            self.container_name, 'blob1', data, 'bytes=1024-1535', 'update')
        etag = self.pbs.get_blob_properties(self.container_name, 'blob1')['ETag']

        # Act
        ranges = self.pbs.get_page_ranges(self.container_name, 'blob1',
                                         if_match=etag)
        for range in ranges:
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
        resp1 = self.pbs.put_page(
            self.container_name, 'blob1', data, 'bytes=0-511', 'update')
        resp2 = self.pbs.put_page(
            self.container_name, 'blob1', data, 'bytes=1024-1535', 'update')

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
        resp1 = self.pbs.put_page(
            self.container_name, 'blob1', data, 'bytes=0-511', 'update')
        resp2 = self.pbs.put_page(
            self.container_name, 'blob1', data, 'bytes=1024-1535', 'update')

        # Act
        ranges = self.pbs.get_page_ranges(self.container_name, 'blob1',
                                         if_none_match='0x111111111111111')
        for range in ranges:
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
        resp1 = self.pbs.put_page(
            self.container_name, 'blob1', data, 'bytes=0-511', 'update')
        resp2 = self.pbs.put_page(
            self.container_name, 'blob1', data, 'bytes=1024-1535', 'update')
        etag = self.pbs.get_blob_properties(self.container_name, 'blob1')['ETag']

        # Act
        with self.assertRaises(AzureHttpError):
            self.pbs.get_page_ranges(self.container_name, 'blob1',
                                    if_none_match=etag)

        # Assert

#------------------------------------------------------------------------------
if __name__ == '__main__':
    unittest.main()