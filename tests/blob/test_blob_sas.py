# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from datetime import datetime, timedelta

import requests
from azure.common import (
    AzureMissingResourceHttpError,
    AzureException,
)

from azure.storage.blob import (
    BlockBlobService,
    BlobPermissions,
    ContainerPermissions,
)
from azure.storage.common import (
    AccessPolicy,
    ResourceTypes,
    AccountPermissions,
    TokenCredential,
)
from tests.testcase import (
    StorageTestCase,
    TestMode,
    record,
)


class StorageBlobSASTest(StorageTestCase):

    def setUp(self):
        super(StorageBlobSASTest, self).setUp()

        self.bs = self._create_storage_service(BlockBlobService, self.settings)
        self.container_name = self.get_resource_name('utcontainer')

        if not self.is_playback():
            self.bs.create_container(self.container_name)

        self.byte_data = self.get_random_bytes(1024)

    def tearDown(self):
        if not self.is_playback():
            self.bs.delete_container(self.container_name)

        return super(StorageBlobSASTest, self).tearDown()

    def _get_container_reference(self):
        return self.get_resource_name("sastestcontainer")

    def _get_blob_reference(self):
        return self.get_resource_name("sastestblob")

    def _create_block_blob(self):
        blob_name = self._get_blob_reference()
        self.bs.create_blob_from_bytes(self.container_name, blob_name, self.byte_data)
        return blob_name

    def _get_user_delegation_key(self, key_start_time, key_expiry_time):
        token_credential = TokenCredential(self.generate_oauth_token())
        service = BlockBlobService(self.settings.STORAGE_ACCOUNT_NAME, token_credential=token_credential)
        return service.get_user_delegation_key(key_start_time, key_expiry_time)

    @record
    def test_get_user_delegation_key(self):
        # Act
        start = datetime.utcnow()
        expiry = datetime.utcnow() + timedelta(hours=1)
        user_delegation_key_1 = self._get_user_delegation_key(key_start_time=start, key_expiry_time=expiry)
        user_delegation_key_2 = self._get_user_delegation_key(key_start_time=start, key_expiry_time=expiry)

        # Assert key1 is valid
        self.assertIsNotNone(user_delegation_key_1.signed_oid)
        self.assertIsNotNone(user_delegation_key_1.signed_tid)
        self.assertIsNotNone(user_delegation_key_1.signed_start)
        self.assertIsNotNone(user_delegation_key_1.signed_expiry)
        self.assertIsNotNone(user_delegation_key_1.signed_version)
        self.assertIsNotNone(user_delegation_key_1.signed_service)
        self.assertIsNotNone(user_delegation_key_1.value)

        # Assert key1 and key2 are equal, since they have the exact same start and end times
        self.assertEqual(user_delegation_key_1.signed_oid, user_delegation_key_2.signed_oid)
        self.assertEqual(user_delegation_key_1.signed_tid, user_delegation_key_2.signed_tid)
        self.assertEqual(user_delegation_key_1.signed_start, user_delegation_key_2.signed_start)
        self.assertEqual(user_delegation_key_1.signed_expiry, user_delegation_key_2.signed_expiry)
        self.assertEqual(user_delegation_key_1.signed_version, user_delegation_key_2.signed_version)
        self.assertEqual(user_delegation_key_1.signed_service, user_delegation_key_2.signed_service)
        self.assertEqual(user_delegation_key_1.value, user_delegation_key_2.value)

    def test_user_delegation_sas_for_blob(self):
        # SAS URL is calculated from storage key, so this test runs live only
        if TestMode.need_recording_file(self.test_mode):
            return

        # Arrange
        blob_name = self._create_block_blob()
        user_delegation_key = self._get_user_delegation_key(datetime.utcnow(), datetime.utcnow() + timedelta(hours=1))

        # create a new service object without any key, to make sure the sas is truly generated from the delegation key
        service = BlockBlobService(self.settings.STORAGE_ACCOUNT_NAME)
        token = service.generate_blob_shared_access_signature(
            self.container_name,
            blob_name,
            permission=BlobPermissions.READ,
            expiry=datetime.utcnow() + timedelta(hours=1),
            user_delegation_key=user_delegation_key,
        )

        # Act
        # Use the generated identity sas
        service = BlockBlobService(
            self.settings.STORAGE_ACCOUNT_NAME,
            sas_token=token,
            request_session=requests.Session(),
        )
        result = service.get_blob_to_bytes(self.container_name, blob_name)

        # Assert
        self.assertEqual(self.byte_data, result.content)

    @record
    def test_user_delegation_sas_for_container(self):
        # SAS URL is calculated from storage key, so this test runs live only
        if TestMode.need_recording_file(self.test_mode):
            return

        # Arrange
        blob_name = self._create_block_blob()
        user_delegation_key = self._get_user_delegation_key(datetime.utcnow(), datetime.utcnow() + timedelta(hours=1))

        # create a new service object without any key, to make sure the sas is truly generated from the delegation key
        service = BlockBlobService(self.settings.STORAGE_ACCOUNT_NAME)
        token = service.generate_container_shared_access_signature(
            self.container_name,
            expiry=datetime.utcnow() + timedelta(hours=1),
            permission=ContainerPermissions.READ,
            user_delegation_key=user_delegation_key,
        )

        # Act
        service = BlockBlobService(
            self.settings.STORAGE_ACCOUNT_NAME,
            sas_token=token,
            request_session=requests.Session(),
        )
        result = service.get_blob_to_bytes(self.container_name, blob_name)

        # Assert
        self.assertEqual(self.byte_data, result.content)

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
    def test_sas_access_blob_snapshot(self):
        # SAS URL is calculated from storage key, so this test runs live only
        if TestMode.need_recording_file(self.test_mode):
            return

        # Arrange
        blob_name = self._create_block_blob()
        blob_snapshot = self.bs.snapshot_blob(self.container_name, blob_name)

        token = self.bs.generate_blob_shared_access_signature(
            self.container_name,
            blob_name,
            permission=BlobPermissions.READ + BlobPermissions.DELETE,
            expiry=datetime.utcnow() + timedelta(hours=1),
            snapshot=blob_snapshot.snapshot
        )
        service = BlockBlobService(
            self.settings.STORAGE_ACCOUNT_NAME,
            sas_token=token,
            request_session=requests.Session(),
        )

        # Read from the snapshot
        result = service.get_blob_to_bytes(self.container_name, blob_name, snapshot=blob_snapshot.snapshot)

        # Assert
        self.assertEqual(self.byte_data, result.content)

        # Delete the snapshot
        service.delete_blob(self.container_name, blob_name, snapshot=blob_snapshot.snapshot)

        # Assert
        self.assertFalse(service.exists(self.container_name, blob_name, snapshot=blob_snapshot.snapshot))

        # Accessing the blob with a snapshot sas should fail
        with self.assertRaises(AzureException):
            service.get_blob_to_bytes(self.container_name, blob_name)

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
            ResourceTypes.OBJECT + ResourceTypes.CONTAINER,
            AccountPermissions.READ,
            datetime.utcnow() + timedelta(hours=1),
        )

        # Act
        blob_url = self.bs.make_blob_url(
            self.container_name,
            blob_name,
            sas_token=token,
        )
        container_url = self.bs.make_container_url(
            self.container_name,
            sas_token=token,
        )

        blob_response = requests.get(blob_url)
        container_response = requests.get(container_url)

        # Assert
        self.assertTrue(blob_response.ok)
        self.assertEqual(self.byte_data, blob_response.content)
        self.assertTrue(container_response.ok)

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

    @record
    def test_shared_access_container(self):
        # SAS URL is calculated from storage key, so this test runs live only
        if TestMode.need_recording_file(self.test_mode):
            return

        # Arrange
        blob_name = self._create_block_blob()
        token = self.bs.generate_container_shared_access_signature(
            self.container_name,
            expiry=datetime.utcnow() + timedelta(hours=1),
            permission=ContainerPermissions.READ,
        )
        url = self.bs.make_blob_url(
            self.container_name,
            blob_name,
            sas_token=token,
        )

        # Act
        response = requests.get(url)

        # Assert
        self.assertTrue(response.ok)
        self.assertEqual(self.byte_data, response.content)
