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
import unittest
from datetime import datetime, timedelta
import requests
from azure.storage import (
    CloudStorageAccount,
    ResourceTypes,
    AccountPermissions,
    Services,
)
from azure.storage.blob import BlockBlobService
from azure.storage.queue import QueueService
from azure.storage.table import TableService
from tests.testcase import StorageTestCase

#------------------------------------------------------------------------------


class StorageAccountTest(StorageTestCase):

    def setUp(self):
        super(StorageAccountTest, self).setUp()
        self.account_name = self.settings.STORAGE_ACCOUNT_NAME
        self.account_key = self.settings.STORAGE_ACCOUNT_KEY
        self.account = CloudStorageAccount(self.account_name, self.account_key)

    #--Test cases --------------------------------------------------------
    def test_create_blob_service(self):
        # Arrange

        # Act
        service = self.account.create_block_blob_service()

        # Assert
        self.assertIsNotNone(service)
        self.assertIsInstance(service, BlockBlobService)
        self.assertEqual(service.account_name, self.account_name)
        self.assertEqual(service.account_key, self.account_key)

    def test_create_blob_service_empty_credentials(self):
        # Arrange

        # Act
        bad_account = CloudStorageAccount('', '')
        with self.assertRaises(ValueError):
            service = bad_account.create_block_blob_service()

        # Assert

    def test_create_table_service(self):
        # Arrange

        # Act
        service = self.account.create_table_service()

        # Assert
        self.assertIsNotNone(service)
        self.assertIsInstance(service, TableService)
        self.assertEqual(service.account_name, self.account_name)
        self.assertEqual(service.account_key, self.account_key)

    def test_create_queue_service(self):
        # Arrange

        # Act
        service = self.account.create_queue_service()

        # Assert
        self.assertIsNotNone(service)
        self.assertIsInstance(service, QueueService)
        self.assertEqual(service.account_name, self.account_name)
        self.assertEqual(service.account_key, self.account_key)

    def test_generate_account_sas(self):
        # Arrange
        token = self.account.generate_shared_access_signature(
            Services.BLOB,
            ResourceTypes.OBJECT,
            AccountPermissions.READ,
            datetime.utcnow() + timedelta(hours=1),
        )

        service = self.account.create_block_blob_service()
        data = b'shared access signature with read permission on blob'
        container_name='container1'
        blob_name = 'blob1.txt'

        try:
            service.create_container(container_name)
            service.create_blob_from_bytes(container_name, blob_name, data)

            # Act
            url = service.make_blob_url(
                container_name,
                blob_name,
                sas_token=token,
            )
            response = requests.get(url)

            # Assert
            self.assertTrue(response.ok)
            self.assertEqual(data, response.content)
        finally:
            service.delete_container(container_name)

#------------------------------------------------------------------------------
if __name__ == '__main__':
    unittest.main()
