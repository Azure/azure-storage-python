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
import requests
import sys
import unittest
from datetime import datetime, timedelta
from azure.common import (
    AzureHttpError,
    AzureMissingResourceHttpError,
)
from azure.storage.common import (
    AccessPolicy,
    ResourceTypes,
    AccountPermissions,
)
from azure.storage.blob import (
    Blob,
    BlockBlobService,
    BlobPermissions,
    ContentSettings,
    DeleteSnapshot,
)
from tests.testcase import (
    StorageTestCase,
    TestMode,
    record,
)

#------------------------------------------------------------------------------
TEST_CONTAINER_PREFIX = 'container'
TEST_BLOB_PREFIX = 'blob'
#------------------------------------------------------------------------------

class StorageCommonBlobTest(StorageTestCase):

    def setUp(self):
        super(StorageCommonBlobTest, self).setUp()

        self.bs = self._create_storage_service(BlockBlobService, self.settings)
        self.container_name = self.get_resource_name('utcontainer')

        if not self.is_playback():
            self.bs.create_container(self.container_name)

        self.byte_data = self.get_random_bytes(1024)

        self.bs2 = self._create_remote_storage_service(BlockBlobService, self.settings)
        self.remote_container_name = None

    def tearDown(self):
        if not self.is_playback():
            try:
                self.bs.delete_container(self.container_name)
            except:
                pass

            if self.remote_container_name:
                try:
                    self.bs2.delete_container(self.remote_container_name)
                except:
                    pass

        return super(StorageCommonBlobTest, self).tearDown()

    #--Helpers-----------------------------------------------------------------
    def _get_container_reference(self):
        return self.get_resource_name(TEST_CONTAINER_PREFIX)

    def _get_blob_reference(self):
        return self.get_resource_name(TEST_BLOB_PREFIX)

    def _create_block_blob(self):
        blob_name = self._get_blob_reference()
        self.bs.create_blob_from_bytes(self.container_name, blob_name, self.byte_data)
        return blob_name

    def _create_remote_container(self):
        self.remote_container_name = self.get_resource_name('remotectnr')
        self.bs2.create_container(self.remote_container_name)

    def _create_remote_block_blob(self, blob_data=None):
        if not blob_data:
            blob_data = b'12345678' * 1024 * 1024
        source_blob_name = self._get_blob_reference()
        self.bs2.create_blob_from_bytes(
            self.remote_container_name, source_blob_name, blob_data)
        return source_blob_name

    def _wait_for_async_copy(self, container_name, blob_name):
        count = 0
        blob = self.bs.get_blob_properties(container_name, blob_name)
        while blob.properties.copy.status != 'success':
            count = count + 1
            if count > 10:
                self.fail('Timed out waiting for async copy to complete.')
            self.sleep(6)
            blob = self.bs.get_blob_properties(container_name, blob_name)
        self.assertEqual(blob.properties.copy.status, 'success')

    #-- Common test cases for blobs ----------------------------------------------
    @record
    def test_blob_exists(self):
        # Arrange
        blob_name = self._create_block_blob()

        # Act
        exists = self.bs.exists(self.container_name, blob_name)

        # Assert
        self.assertTrue(exists)

    @record
    def test_blob_container_not_exists(self):
        # Arrange
        blob_name = self._get_blob_reference()

        # Act
        exists = self.bs.exists(self._get_container_reference(), blob_name)

        # Assert
        self.assertFalse(exists)

    @record
    def test_blob_not_exists(self):
        # Arrange
        blob_name = self._get_blob_reference()

        # Act
        exists = self.bs.exists(self.container_name, blob_name)

        # Assert
        self.assertFalse(exists)

    @record
    def test_make_blob_url(self):
        # Arrange

        # Act
        res = self.bs.make_blob_url('vhds', 'my.vhd')

        # Assert
        self.assertEqual(res, 'https://' + self.settings.STORAGE_ACCOUNT_NAME
                         + '.blob.core.windows.net/vhds/my.vhd')

    @record
    def test_make_blob_url_with_protocol(self):
        # Arrange

        # Act
        res = self.bs.make_blob_url('vhds', 'my.vhd', protocol='http')

        # Assert
        self.assertEqual(res, 'http://' + self.settings.STORAGE_ACCOUNT_NAME
                         + '.blob.core.windows.net/vhds/my.vhd')

    @record
    def test_make_blob_url_with_sas(self):
        # Arrange

        # Act
        res = self.bs.make_blob_url('vhds', 'my.vhd', sas_token='sas')

        # Assert
        self.assertEqual(res, 'https://' + self.settings.STORAGE_ACCOUNT_NAME
                         + '.blob.core.windows.net/vhds/my.vhd?sas')

    @record
    def test_make_blob_url_with_snapshot(self):
        # Arrange

        # Act
        res = self.bs.make_blob_url('vhds', 'my.vhd',
                                    snapshot='2016-11-09T14:11:07.6175300Z')

        # Assert
        self.assertEqual(res, 'https://' + self.settings.STORAGE_ACCOUNT_NAME
                         + '.blob.core.windows.net/vhds/my.vhd?'
                           'snapshot=2016-11-09T14:11:07.6175300Z')

    @record
    def test_make_blob_url_with_snapshot_and_sas(self):
        # Arrange

        # Act
        res = self.bs.make_blob_url('vhds', 'my.vhd', sas_token='sas',
                                    snapshot='2016-11-09T14:11:07.6175300Z')

        # Assert
        self.assertEqual(res, 'https://' + self.settings.STORAGE_ACCOUNT_NAME
                         + '.blob.core.windows.net/vhds/my.vhd?'
                           'snapshot=2016-11-09T14:11:07.6175300Z&sas')

    @record
    def test_create_blob_with_question_mark(self):
        # Arrange
        blob_name = '?ques?tion?'
        blob_data = u'???'

        # Act
        self.bs.create_blob_from_text(self.container_name, blob_name, blob_data)

        # Assert
        blob = self.bs.get_blob_to_text(self.container_name, blob_name)
        self.assertEqual(blob.content, blob_data)

    @record
    def test_create_blob_with_special_chars(self):
        # Arrange

        # Act
        for c in '-._ /()$=\',~':
            blob_name = '{0}a{0}a{0}'.format(c)
            blob_data = c
            self.bs.create_blob_from_text(self.container_name, blob_name, blob_data)
            blob = self.bs.get_blob_to_text(self.container_name, blob_name)
            self.assertEqual(blob.content, blob_data)

        # Assert

    @record
    def test_create_blob_with_lease_id(self):
        # Arrange
        blob_name = self._create_block_blob()
        lease_id = self.bs.acquire_blob_lease(self.container_name, blob_name)

        # Act
        data = b'hello world again'
        resp = self.bs.create_blob_from_bytes (
            self.container_name, blob_name, data,
            lease_id=lease_id)

        # Assert
        self.assertIsNotNone(resp.etag)
        blob = self.bs.get_blob_to_bytes(
            self.container_name, blob_name, lease_id=lease_id)
        self.assertEqual(blob.content, b'hello world again')

    @record
    def test_create_blob_with_metadata(self):
        # Arrange
        blob_name = self._get_blob_reference()
        metadata={'hello': 'world', 'number': '42'}

        # Act
        data = b'hello world'
        resp = self.bs.create_blob_from_bytes(
            self.container_name, blob_name, data, metadata=metadata)

        # Assert
        self.assertIsNotNone(resp.etag)
        md = self.bs.get_blob_metadata(self.container_name, blob_name)
        self.assertDictEqual(md, metadata)

    @record
    def test_get_blob_with_existing_blob(self):
        # Arrange
        blob_name = self._create_block_blob()

        # Act
        blob = self.bs.get_blob_to_bytes(self.container_name, blob_name)

        # Assert
        self.assertIsInstance(blob, Blob)
        self.assertEqual(blob.content, self.byte_data)

    @record
    def test_get_blob_with_snapshot(self):
        # Arrange
        blob_name = self._create_block_blob()
        snapshot = self.bs.snapshot_blob(self.container_name, blob_name)

        # Act
        blob = self.bs.get_blob_to_bytes(self.container_name, blob_name, snapshot.snapshot)

        # Assert
        self.assertIsInstance(blob, Blob)
        self.assertEqual(blob.content, self.byte_data)

    @record
    def test_get_blob_with_snapshot_previous(self):
        # Arrange
        blob_name = self._create_block_blob()
        snapshot = self.bs.snapshot_blob(self.container_name, blob_name)
        self.bs.create_blob_from_bytes (self.container_name, blob_name,
                         b'hello world again', )

        # Act
        blob_previous = self.bs.get_blob_to_bytes(
            self.container_name, blob_name, snapshot.snapshot)
        blob_latest = self.bs.get_blob_to_bytes(self.container_name, blob_name)

        # Assert
        self.assertIsInstance(blob_previous, Blob)
        self.assertIsInstance(blob_latest, Blob)
        self.assertEqual(blob_previous.content, self.byte_data)
        self.assertEqual(blob_latest.content, b'hello world again')

    @record
    def test_get_blob_with_range(self):
        # Arrange
        blob_name = self._create_block_blob()

        # Act
        blob = self.bs.get_blob_to_bytes(self.container_name, blob_name, start_range=0, end_range=5)

        # Assert
        self.assertIsInstance(blob, Blob)
        self.assertEqual(blob.content, self.byte_data[:6])

    @record
    def test_get_blob_with_lease(self):
        # Arrange
        blob_name = self._create_block_blob()
        lease_id = self.bs.acquire_blob_lease(self.container_name, blob_name)

        # Act
        blob = self.bs.get_blob_to_bytes(self.container_name, blob_name, lease_id=lease_id)
        self.bs.release_blob_lease(self.container_name, blob_name, lease_id)

        # Assert
        self.assertIsInstance(blob, Blob)
        self.assertEqual(blob.content, self.byte_data)

    @record
    def test_get_blob_with_non_existing_blob(self):
        # Arrange
        blob_name = self._get_blob_reference()

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.get_blob_to_bytes(self.container_name, blob_name)

        # Assert

    @record
    def test_set_blob_properties_with_existing_blob(self):
        # Arrange
        blob_name = self._create_block_blob()

        # Act
        self.bs.set_blob_properties(
            self.container_name,
            blob_name,
            content_settings=ContentSettings(
                content_language='spanish',
                content_disposition='inline'),
        )

        # Assert
        blob = self.bs.get_blob_properties(self.container_name, blob_name)
        self.assertEqual(blob.properties.content_settings.content_language, 'spanish')
        self.assertEqual(blob.properties.content_settings.content_disposition, 'inline')

    @record
    def test_set_blob_properties_with_blob_settings_param(self):
        # Arrange
        blob_name = self._create_block_blob()
        blob = self.bs.get_blob_properties(self.container_name, blob_name)

        # Act
        blob.properties.content_settings.content_language = 'spanish'
        blob.properties.content_settings.content_disposition = 'inline'
        self.bs.set_blob_properties(
            self.container_name,
            blob_name,
            content_settings=blob.properties.content_settings,
        )

        # Assert
        blob = self.bs.get_blob_properties(self.container_name, blob_name)
        self.assertEqual(blob.properties.content_settings.content_language, 'spanish')
        self.assertEqual(blob.properties.content_settings.content_disposition, 'inline')

    @record
    def test_get_blob_properties(self):
        # Arrange
        blob_name = self._create_block_blob()

        # Act
        blob = self.bs.get_blob_properties(self.container_name, blob_name)

        # Assert
        self.assertIsInstance(blob, Blob)
        self.assertEqual(blob.properties.blob_type, self.bs.blob_type)
        self.assertEqual(blob.properties.content_length, len(self.byte_data))
        self.assertEqual(blob.properties.lease.status, 'unlocked')

    @record
    def test_get_blob_server_encryption(self):
        # Arrange
        blob_name = self._create_block_blob()

        # Act
        blob = self.bs.get_blob_to_bytes(self.container_name, blob_name)

        # Assert
        self.assertTrue(blob.properties.server_encrypted)

    @record
    def test_get_blob_properties_server_encryption(self):
        # Arrange
        blob_name = self._create_block_blob()

        # Act
        blob = self.bs.get_blob_properties(self.container_name, blob_name)

        # Assert
        self.assertTrue(blob.properties.server_encrypted)

    @record
    def test_list_blobs_server_encryption(self):
        #Arrange
        self._create_block_blob()
        self._create_block_blob()
        blob_list = self.bs.list_blobs(self.container_name)

        #Act

        #Assert
        for blob in blob_list:
            self.assertTrue(blob.properties.server_encrypted)

    @record
    def test_no_server_encryption(self):
        # Arrange
        blob_name = self._create_block_blob()

        #Act
        def callback(response):
            response.headers['x-ms-server-encrypted'] = 'false'

        self.bs.response_callback = callback
        blob = self.bs.get_blob_properties(self.container_name, blob_name)

        #Assert
        self.assertFalse(blob.properties.server_encrypted)

    @record
    def test_get_blob_properties_with_snapshot(self):
        # Arrange
        blob_name = self._create_block_blob()
        res = self.bs.snapshot_blob(self.container_name, blob_name)
        blobs = list(self.bs.list_blobs(self.container_name, include='snapshots'))
        self.assertEqual(len(blobs), 2)

        # Act
        blob = self.bs.get_blob_properties(self.container_name, blob_name, snapshot=res.snapshot)

        # Assert
        self.assertIsNotNone(blob)
        self.assertEqual(blob.properties.blob_type, self.bs.blob_type)
        self.assertEqual(blob.properties.content_length, len(self.byte_data))

    @record
    def test_get_blob_properties_with_leased_blob(self):
        # Arrange
        blob_name = self._create_block_blob()
        lease = self.bs.acquire_blob_lease(self.container_name, blob_name)

        # Act
        blob = self.bs.get_blob_properties(self.container_name, blob_name)

        # Assert
        self.assertIsInstance(blob, Blob)
        self.assertEqual(blob.properties.blob_type, self.bs.blob_type)
        self.assertEqual(blob.properties.content_length, len(self.byte_data))
        self.assertEqual(blob.properties.lease.status, 'locked')
        self.assertEqual(blob.properties.lease.state, 'leased')
        self.assertEqual(blob.properties.lease.duration, 'infinite')

    @record
    def test_get_blob_metadata(self):
        # Arrange
        blob_name = self._create_block_blob()

        # Act
        md = self.bs.get_blob_metadata(self.container_name, blob_name)

        # Assert
        self.assertIsNotNone(md)

    @record
    def test_set_blob_metadata_with_upper_case(self):
        # Arrange
        metadata = {'hello': 'world', 'number': '42', 'UP': 'UPval'}
        blob_name = self._create_block_blob()

        # Act
        self.bs.set_blob_metadata(self.container_name, blob_name, metadata)

        # Assert
        md = self.bs.get_blob_metadata(self.container_name, blob_name)
        self.assertEqual(3, len(md))
        self.assertEqual(md['hello'], 'world')
        self.assertEqual(md['number'], '42')
        self.assertEqual(md['up'], 'UPval')

    @record
    def test_delete_blob_with_existing_blob(self):
        # Arrange
        blob_name = self._create_block_blob()

        # Act
        resp = self.bs.delete_blob(self.container_name, blob_name)

        # Assert
        self.assertIsNone(resp)

    @record
    def test_delete_blob_with_non_existing_blob(self):
        # Arrange
        blob_name = self._get_blob_reference()

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.delete_blob (self.container_name, blob_name)

        # Assert

    @record
    def test_delete_blob_snapshot(self):
        # Arrange
        blob_name = self._create_block_blob()
        res = self.bs.snapshot_blob(self.container_name, blob_name)

        # Act
        self.bs.delete_blob(self.container_name, blob_name, snapshot=res.snapshot)

        # Assert
        blobs = list(self.bs.list_blobs(self.container_name, include='snapshots'))
        self.assertEqual(len(blobs), 1)
        self.assertEqual(blobs[0].name, blob_name)
        self.assertIsNone(blobs[0].snapshot)

    @record
    def test_delete_blob_snapshots(self):
        # Arrange
        blob_name = self._create_block_blob()
        self.bs.snapshot_blob(self.container_name, blob_name)

        # Act
        self.bs.delete_blob(self.container_name, blob_name,
                            delete_snapshots=DeleteSnapshot.Only)

        # Assert
        blobs = list(self.bs.list_blobs(self.container_name, include='snapshots'))
        self.assertEqual(len(blobs), 1)
        self.assertIsNone(blobs[0].snapshot)

    @record
    def test_delete_blob_with_snapshots(self):
        # Arrange
        blob_name = self._create_block_blob()
        self.bs.snapshot_blob(self.container_name, blob_name)

        # Act
        self.bs.delete_blob(self.container_name, blob_name,
                            delete_snapshots=DeleteSnapshot.Include)

        # Assert
        blobs = list(self.bs.list_blobs(self.container_name, include='snapshots'))
        self.assertEqual(len(blobs), 0)

    @record
    def test_copy_blob_with_existing_blob(self):
        # Arrange
        blob_name = self._create_block_blob()

        # Act
        sourceblob = '/{0}/{1}/{2}'.format(self.settings.STORAGE_ACCOUNT_NAME,
                                           self.container_name,
                                           blob_name)
        copy = self.bs.copy_blob(self.container_name, 'blob1copy', sourceblob)

        # Assert
        self.assertIsNotNone(copy)
        self.assertEqual(copy.status, 'success')
        self.assertIsNotNone(copy.id)
        copy_blob = self.bs.get_blob_to_bytes(self.container_name, 'blob1copy')
        self.assertEqual(copy_blob.content, self.byte_data)

    @record
    def test_copy_blob_async_private_blob(self):
        # Arrange
        self._create_remote_container()
        source_blob_name = self._create_remote_block_blob()
        source_blob_url = self.bs2.make_blob_url(self.remote_container_name, source_blob_name)

        # Act
        target_blob_name = 'targetblob'
        with self.assertRaises(AzureMissingResourceHttpError):
            self.bs.copy_blob(self.container_name, target_blob_name, source_blob_url)

        # Assert

    @record
    def test_copy_blob_async_private_blob_with_sas(self):
        # Arrange
        data = b'12345678' * 1024 * 1024
        self._create_remote_container()
        source_blob_name = self._create_remote_block_blob(blob_data=data)

        sas_token = self.bs2.generate_blob_shared_access_signature(
            self.remote_container_name,
            source_blob_name,
            permission=BlobPermissions.READ,
            expiry=datetime.utcnow() + timedelta(hours=1),
        )

        source_blob_url = self.bs2.make_blob_url(
            self.remote_container_name,
            source_blob_name,
            sas_token=sas_token,
        )

        # Act
        target_blob_name = 'targetblob'
        copy_resp = self.bs.copy_blob(
            self.container_name, target_blob_name, source_blob_url)

        # Assert
        self.assertEqual(copy_resp.status, 'pending')
        self._wait_for_async_copy(self.container_name, target_blob_name)
        actual_data = self.bs.get_blob_to_bytes(self.container_name, target_blob_name)
        self.assertEqual(actual_data.content, data)

    @record
    def test_abort_copy_blob(self):
        # Arrange
        data = b'12345678' * 1024 * 1024
        self._create_remote_container()
        source_blob_name = self._create_remote_block_blob(blob_data=data)

        sas_token = self.bs2.generate_blob_shared_access_signature(
            self.remote_container_name,
            source_blob_name,
            permission=BlobPermissions.READ,
            expiry=datetime.utcnow() + timedelta(hours=1),          
        )

        source_blob_url = self.bs2.make_blob_url(
            self.remote_container_name,
            source_blob_name,
            sas_token=sas_token,
        )

        # Act
        target_blob_name = 'targetblob'
        copy_resp = self.bs.copy_blob(
            self.container_name, target_blob_name, source_blob_url)
        self.assertEqual(copy_resp.status, 'pending')
        self.bs.abort_copy_blob(
            self.container_name, 'targetblob', copy_resp.id)

        # Assert
        target_blob = self.bs.get_blob_to_bytes(self.container_name, target_blob_name)
        self.assertEqual(target_blob.content, b'')
        self.assertEqual(target_blob.properties.copy.status, 'aborted')

    @record
    def test_abort_copy_blob_with_synchronous_copy_fails(self):
        # Arrange
        source_blob_name = self._create_block_blob()
        source_blob_url = self.bs.make_blob_url(
            self.container_name, source_blob_name)

        # Act
        target_blob_name = 'targetblob'
        copy_resp = self.bs.copy_blob(
            self.container_name, target_blob_name, source_blob_url)
        with self.assertRaises(AzureHttpError):
            self.bs.abort_copy_blob(
                self.container_name,
                target_blob_name,
                copy_resp.id)

        # Assert
        self.assertEqual(copy_resp.status, 'success')

    @record
    def test_snapshot_blob(self):
        # Arrange
        blob_name = self._create_block_blob()

        # Act
        resp = self.bs.snapshot_blob(self.container_name, blob_name)

        # Assert
        self.assertIsNotNone(resp)
        self.assertIsNotNone(resp.snapshot)

    @record
    def test_lease_blob_acquire_and_release(self):
        # Arrange
        blob_name = self._create_block_blob()

        # Act
        lease_id = self.bs.acquire_blob_lease(self.container_name, blob_name)
        self.bs.release_blob_lease(self.container_name, blob_name, lease_id)
        lease_id2 = self.bs.acquire_blob_lease(self.container_name, blob_name)

        # Assert
        self.assertIsNotNone(lease_id)
        self.assertIsNotNone(lease_id2)

    @record
    def test_lease_blob_with_duration(self):
        # Arrange
        blob_name = self._create_block_blob()

        # Act
        lease_id = self.bs.acquire_blob_lease(
            self.container_name, blob_name, lease_duration=15)
        resp2 = self.bs.create_blob_from_bytes (self.container_name, blob_name, b'hello 2',
                                 lease_id=lease_id)
        self.sleep(15)

        # Assert
        with self.assertRaises(AzureHttpError):
            self.bs.create_blob_from_bytes (self.container_name, blob_name, b'hello 3',
                             lease_id=lease_id)

    @record
    def test_lease_blob_with_proposed_lease_id(self):
        # Arrange
        blob_name = self._create_block_blob()

        # Act
        lease_id = 'a0e6c241-96ea-45a3-a44b-6ae868bc14d0'
        lease_id1 = self.bs.acquire_blob_lease(
            self.container_name, blob_name,
            proposed_lease_id=lease_id)

        # Assert
        self.assertEqual(lease_id1, lease_id)

    @record
    def test_lease_blob_change_lease_id(self):
        # Arrange
        blob_name = self._create_block_blob()

        # Act
        lease_id = 'a0e6c241-96ea-45a3-a44b-6ae868bc14d0'
        cur_lease_id = self.bs.acquire_blob_lease(self.container_name, blob_name)
        self.bs.change_blob_lease(self.container_name, blob_name, cur_lease_id, lease_id)
        next_lease_id = self.bs.renew_blob_lease(self.container_name, blob_name, lease_id)

        # Assert
        self.assertNotEqual(cur_lease_id, next_lease_id)
        self.assertEqual(next_lease_id, lease_id)

    @record
    def test_lease_blob_break_period(self):
        # Arrange
        blob_name = self._create_block_blob()

        # Act
        lease_id = self.bs.acquire_blob_lease(self.container_name, blob_name,
                                   lease_duration=15)
        lease_time = self.bs.break_blob_lease(self.container_name, blob_name,
                                   lease_break_period=5)
        blob = self.bs.create_blob_from_bytes (self.container_name, blob_name, b'hello 2', lease_id=lease_id)
        self.sleep(5)

        with self.assertRaises(AzureHttpError):
            self.bs.create_blob_from_bytes (self.container_name, blob_name, b'hello 3', lease_id=lease_id)

        # Assert
        self.assertIsNotNone(lease_id)
        self.assertIsNotNone(lease_time)
        self.assertIsNotNone(blob.etag)

    @record
    def test_lease_blob_acquire_and_renew(self):
        # Arrange
        blob_name = self._create_block_blob()

        # Act
        lease_id1 = self.bs.acquire_blob_lease(self.container_name, blob_name)
        lease_id2 = self.bs.renew_blob_lease(self.container_name, blob_name, lease_id1)

        # Assert
        self.assertEqual(lease_id1, lease_id2)

    @record
    def test_lease_blob_acquire_twice_fails(self):
        # Arrange
        blob_name = self._create_block_blob()
        lease_id1 = self.bs.acquire_blob_lease(self.container_name, blob_name)

        # Act
        with self.assertRaises(AzureHttpError):
            self.bs.acquire_blob_lease(self.container_name, blob_name)
        self.bs.release_blob_lease(self.container_name, blob_name, lease_id1)

        # Assert
        self.assertIsNotNone(lease_id1)

    @record
    def test_unicode_get_blob_unicode_name(self):
        # Arrange
        blob_name = '啊齄丂狛狜'
        self.bs.create_blob_from_bytes(self.container_name, blob_name, b'hello world')

        # Act
        blob = self.bs.get_blob_to_bytes(self.container_name, blob_name)

        # Assert
        self.assertIsInstance(blob, Blob)
        self.assertEqual(blob.content, b'hello world')

    @record
    def test_create_blob_blob_unicode_data(self):
        # Arrange
        blob_name = self._get_blob_reference()

        # Act
        data = u'hello world啊齄丂狛狜'.encode('utf-8')
        resp = self.bs.create_blob_from_bytes (
            self.container_name, blob_name, data, )

        # Assert
        self.assertIsNotNone(resp.etag)

    @record
    def test_no_sas_private_blob(self):
        # Arrange
        blob_name = self._create_block_blob()

        # Act
        url = self.bs.make_blob_url(self.container_name, blob_name)
        response = requests.get(url)

        # Assert
        self.assertFalse(response.ok)
        self.assertNotEqual(-1, response.text.find('ResourceNotFound'))

    @record
    def test_no_sas_public_blob(self):
        # Arrange
        data = b'a public blob can be read without a shared access signature'
        blob_name = 'blob1.txt'
        container_name = self._get_container_reference()
        self.bs.create_container(container_name, None, 'blob')
        self.bs.create_blob_from_bytes (container_name, blob_name, data, )

        # Act
        url = self.bs.make_blob_url(container_name, blob_name)
        response = requests.get(url)

        # Assert
        self.assertTrue(response.ok)
        self.assertEqual(data, response.content)

    @record
    def test_public_access_blob(self):
        # Arrange
        data = b'public access blob'
        blob_name = 'blob1.txt'
        container_name = self._get_container_reference()
        self.bs.create_container(container_name, None, 'blob')
        self.bs.create_blob_from_bytes (container_name, blob_name, data, )

        # Act
        service = BlockBlobService(
            self.settings.STORAGE_ACCOUNT_NAME,
            request_session=requests.Session(),
        )
        self._set_test_proxy(service, self.settings)
        result = service.get_blob_to_bytes(container_name, blob_name)

        # Assert
        self.assertEqual(data, result.content)

    @record
    def test_sas_access_blob(self):
        # SAS URL is calculated from storage key, so this test runs live only
        if TestMode.need_recording_file(self.test_mode):
            return

        # Arrange
        blob_name = self._create_block_blob()
        
        token = self.bs.generate_blob_shared_access_signature(
            self.container_name,
            blob_name,
            permission=BlobPermissions.READ,
            expiry=datetime.utcnow() + timedelta(hours=1),
        )

        # Act
        service = BlockBlobService(
            self.settings.STORAGE_ACCOUNT_NAME,
            sas_token=token,
            request_session=requests.Session(),
        )
        self._set_test_proxy(service, self.settings)
        result = service.get_blob_to_bytes(self.container_name, blob_name)

        # Assert
        self.assertEqual(self.byte_data, result.content)

    @record
    def test_sas_signed_identifier(self):
        # SAS URL is calculated from storage key, so this test runs live only
        if TestMode.need_recording_file(self.test_mode):
            return

        # Arrange
        blob_name = self._create_block_blob()

        access_policy = AccessPolicy()
        access_policy.start = '2011-10-11'
        access_policy.expiry = '2018-10-12'
        access_policy.permission = BlobPermissions.READ
        identifiers = {'testid': access_policy}

        resp = self.bs.set_container_acl(self.container_name, identifiers)

        token = self.bs.generate_blob_shared_access_signature(
            self.container_name,
            blob_name,
            id='testid'
            )

        # Act
        service = BlockBlobService(
            self.settings.STORAGE_ACCOUNT_NAME,
            sas_token=token,
            request_session=requests.Session(),
        )
        self._set_test_proxy(service, self.settings)
        result = service.get_blob_to_bytes(self.container_name, blob_name)

        # Assert
        self.assertEqual(self.byte_data, result.content)

    @record
    def test_account_sas(self):
        # SAS URL is calculated from storage key, so this test runs live only
        if TestMode.need_recording_file(self.test_mode):
            return

        # Arrange
        blob_name = self._create_block_blob()

        token = self.bs.generate_account_shared_access_signature(
            ResourceTypes.OBJECT,
            AccountPermissions.READ,
            datetime.utcnow() + timedelta(hours=1),
        )

        # Act
        url = self.bs.make_blob_url(
            self.container_name,
            blob_name,
            sas_token=token,
        )
        response = requests.get(url)

        # Assert
        self.assertTrue(response.ok)
        self.assertEqual(self.byte_data, response.content)

    @record
    def test_shared_read_access_blob(self):
        # SAS URL is calculated from storage key, so this test runs live only
        if TestMode.need_recording_file(self.test_mode):
            return

        # Arrange
        blob_name = self._create_block_blob()

        token = self.bs.generate_blob_shared_access_signature(
            self.container_name,
            blob_name,
            permission=BlobPermissions.READ,
            expiry=datetime.utcnow() + timedelta(hours=1),
        )

        # Act
        url = self.bs.make_blob_url(
            self.container_name,
            blob_name,
            sas_token=token,
        )
        response = requests.get(url)

        # Assert
        self.assertTrue(response.ok)
        self.assertEqual(self.byte_data, response.content)

    @record
    def test_shared_read_access_blob_with_content_query_params(self):
        # SAS URL is calculated from storage key, so this test runs live only
        if TestMode.need_recording_file(self.test_mode):
            return

        # Arrange
        blob_name = self._create_block_blob()

        token = self.bs.generate_blob_shared_access_signature(
            self.container_name,
            blob_name,
            permission=BlobPermissions.READ,
            expiry=datetime.utcnow() + timedelta(hours=1),
            cache_control='no-cache',
            content_disposition='inline',
            content_encoding='utf-8',
            content_language='fr',
            content_type='text',
        )
        url = self.bs.make_blob_url(
            self.container_name,
            blob_name,
            sas_token=token,
        )

        # Act
        response = requests.get(url)

        # Assert
        self.assertEqual(self.byte_data, response.content)
        self.assertEqual(response.headers['cache-control'], 'no-cache')
        self.assertEqual(response.headers['content-disposition'], 'inline')
        self.assertEqual(response.headers['content-encoding'], 'utf-8')
        self.assertEqual(response.headers['content-language'], 'fr')
        self.assertEqual(response.headers['content-type'], 'text')

    @record
    def test_shared_write_access_blob(self):
        # SAS URL is calculated from storage key, so this test runs live only
        if TestMode.need_recording_file(self.test_mode):
            return

        # Arrange
        updated_data = b'updated blob data'
        blob_name = self._create_block_blob()

        token = self.bs.generate_blob_shared_access_signature(
            self.container_name,
            blob_name,
            permission=BlobPermissions.WRITE,
            expiry=datetime.utcnow() + timedelta(hours=1),
        )
        url = self.bs.make_blob_url(
            self.container_name,
            blob_name,
            sas_token=token,
        )

        # Act
        headers = {'x-ms-blob-type': self.bs.blob_type}
        response = requests.put(url, headers=headers, data=updated_data)

        # Assert
        self.assertTrue(response.ok)
        blob = self.bs.get_blob_to_bytes(self.container_name, blob_name)
        self.assertEqual(updated_data, blob.content)

    @record
    def test_shared_delete_access_blob(self):
        # SAS URL is calculated from storage key, so this test runs live only
        if TestMode.need_recording_file(self.test_mode):
            return

        # Arrange
        blob_name = self._create_block_blob()

        token = self.bs.generate_blob_shared_access_signature(
            self.container_name,
            blob_name,
            permission=BlobPermissions.DELETE,
            expiry=datetime.utcnow() + timedelta(hours=1),
        )
        url = self.bs.make_blob_url(
            self.container_name,
            blob_name,
            sas_token=token,
        )

        # Act
        response = requests.delete(url)

        # Assert
        self.assertTrue(response.ok)
        with self.assertRaises(AzureMissingResourceHttpError):
            blob = self.bs.get_blob_to_bytes(self.container_name, blob_name)

#------------------------------------------------------------------------------
if __name__ == '__main__':
    unittest.main()
