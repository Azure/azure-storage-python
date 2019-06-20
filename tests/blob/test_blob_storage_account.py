# coding: utf-8

# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import unittest

from azure.storage.blob import (
    BlockBlobService,
)
from azure.storage.blob.models import StandardBlobTier, BatchSetBlobTierSubRequest, RehydratePriority
from tests.testcase import (
    StorageTestCase,
    record,
)

# ------------------------------------------------------------------------------
TEST_BLOB_PREFIX = 'blob'
# ------------------------------------------------------------------------------


class BlobStorageAccountTest(StorageTestCase):
    def setUp(self):
        super(BlobStorageAccountTest, self).setUp()

        self.bs = self._create_storage_service_for_blob_storage_account(BlockBlobService, self.settings)

        self.container_name = self.get_resource_name('utcontainer')

        if not self.is_playback():
            self.bs.create_container(self.container_name)

    def tearDown(self):
        if not self.is_playback():
            try:
                self.bs.delete_container(self.container_name)
            except:
                pass

        return super(BlobStorageAccountTest, self).tearDown()

    # --Helpers-----------------------------------------------------------------
    def _get_blob_reference(self):
        return self.get_resource_name(TEST_BLOB_PREFIX)

    def _create_blob(self):
        blob_name = self._get_blob_reference()
        self.bs.create_blob_from_bytes(self.container_name, blob_name, b'')
        return blob_name

    def assertBlobEqual(self, container_name, blob_name, expected_data):
        actual_data = self.bs.get_blob_to_bytes(container_name, blob_name)
        self.assertEqual(actual_data.content, expected_data)

    # --Tests specific to Blob Storage Accounts (not general purpose)------------

    @record
    def test_standard_blob_tier_set_tier_api(self):
        self.bs.create_container(self.container_name)
        tiers = [StandardBlobTier.Archive, StandardBlobTier.Cool, StandardBlobTier.Hot]
        
        for tier in tiers:
            blob_name = self._get_blob_reference()
            data = b'hello world'
            self.bs.create_blob_from_bytes(self.container_name, blob_name, data)

            blob_ref = self.bs.get_blob_properties(self.container_name, blob_name)
            self.assertIsNotNone(blob_ref.properties.blob_tier)
            self.assertTrue(blob_ref.properties.blob_tier_inferred)
            self.assertIsNone(blob_ref.properties.blob_tier_change_time)

            blobs = list(self.bs.list_blobs(self.container_name))

            # Assert
            self.assertIsNotNone(blobs)
            self.assertGreaterEqual(len(blobs), 1)
            self.assertIsNotNone(blobs[0])
            self.assertNamedItemInContainer(blobs, blob_name)
            self.assertIsNotNone(blobs[0].properties.blob_tier)
            self.assertTrue(blobs[0].properties.blob_tier_inferred)
            self.assertIsNone(blobs[0].properties.blob_tier_change_time)

            self.bs.set_standard_blob_tier(self.container_name, blob_name, tier)

            blob_ref2 = self.bs.get_blob_properties(self.container_name, blob_name)
            self.assertEqual(tier, blob_ref2.properties.blob_tier)
            self.assertFalse(blob_ref2.properties.blob_tier_inferred)
            self.assertIsNotNone(blob_ref2.properties.blob_tier_change_time)

            blobs = list(self.bs.list_blobs(self.container_name))

            # Assert
            self.assertIsNotNone(blobs)
            self.assertGreaterEqual(len(blobs), 1)
            self.assertIsNotNone(blobs[0])
            self.assertNamedItemInContainer(blobs, blob_name)
            self.assertEqual(blobs[0].properties.blob_tier, tier)
            self.assertFalse(blobs[0].properties.blob_tier_inferred)
            self.assertIsNotNone(blobs[0].properties.blob_tier_change_time)

            self.bs.delete_blob(self.container_name, blob_name)

    def test_empty_batch_set_standard_blob_tier(self):
        # Arrange
        batch_set_standard_blob_tier_requests = list()

        with self.assertRaises(ValueError):
            self.bs.batch_set_standard_blob_tier(batch_set_standard_blob_tier_requests)

    def test_batch_set_257_standard_blob_tier_for_blobs(self):
        # Arrange
        batch_set_standard_blob_tier_requests = list()

        for i in range(0, 257):
            batch_set_standard_blob_tier_requests.append(
                BatchSetBlobTierSubRequest(self.container_name, i, StandardBlobTier.Archive))

        with self.assertRaises(ValueError):
            self.bs.batch_set_standard_blob_tier(batch_set_standard_blob_tier_requests)

    @record
    def test_set_standard_blob_tier_with_rehydrate_priority(self):
        # Arrange
        self.bs.create_container(self.container_name)
        blob_name = self._create_blob()
        blob_tier = StandardBlobTier.Archive
        rehydrate_tier = StandardBlobTier.Cool
        rehydrate_priority = RehydratePriority.Standard

        # Act
        self.bs.set_standard_blob_tier(self.container_name, blob_name, blob_tier,
                                       rehydrate_priority=rehydrate_priority)
        self.bs.set_standard_blob_tier(self.container_name, blob_name, rehydrate_tier)
        blob_ref = self.bs.get_blob_properties(self.container_name, blob_name)

        # Assert
        self.assertEquals('rehydrate-pending-to-cool', blob_ref.properties.rehydration_status)

    @record
    def test_batch_set_standard_blob_tier_for_one_blob(self):
        # Arrange
        batch_set_blob_tier_request = []

        self.bs.create_container(self.container_name)
        blob_name = self._get_blob_reference()
        data = b'hello world'
        blob_tier = StandardBlobTier.Cool
        self.bs.create_blob_from_bytes(self.container_name, blob_name, data)

        sub_request = BatchSetBlobTierSubRequest(self.container_name, blob_name, blob_tier)
        batch_set_blob_tier_request.append(sub_request)

        # Act
        resp = self.bs.batch_set_standard_blob_tier(batch_set_blob_tier_request)
        blob_ref = self.bs.get_blob_properties(self.container_name, blob_name)

        # Assert
        self.assertIsNotNone(resp)
        self.assertEquals(len(batch_set_blob_tier_request), len(resp))
        self.assertEquals(blob_tier, blob_ref.properties.blob_tier)
        for sub_response in resp:
            self.assertTrue(sub_response.is_successful)

    @record
    def test_batch_set_three_blob_tier(self):
        # Arrange
        self.bs.create_container(self.container_name)
        tiers = [StandardBlobTier.Archive, StandardBlobTier.Cool, StandardBlobTier.Hot]
        rehydrate_priority = [RehydratePriority.High, RehydratePriority.Standard, RehydratePriority.High]
        blob_names = list()

        batch_set_blob_tier_request = []
        for i in range(0, len(tiers)):
            blob_name = str(i)
            data = b'hello world'
            self.bs.create_blob_from_bytes(self.container_name, blob_name, data)

            sub_request = BatchSetBlobTierSubRequest(self.container_name, blob_name, tiers[i], rehydrate_priority[i])
            batch_set_blob_tier_request.append(sub_request)
            blob_names.append(blob_name)

        # Act
        resp = self.bs.batch_set_standard_blob_tier(batch_set_blob_tier_request)
        blob_refs = list()
        for blob_name in blob_names:
            blob_refs.append(self.bs.get_blob_properties(self.container_name, blob_name))

        # Assert
        self.assertIsNotNone(resp)
        self.assertEquals(len(batch_set_blob_tier_request), len(resp))
        for i in range(0, len(resp)):
            self.assertTrue(resp[i].is_successful)
            # make sure the tier for each blob is correct
            self.assertEquals(tiers[i], blob_refs[i].properties.blob_tier)

    @record
    def test_batch_set_nine_standard_blob_tier(self):
        # To make sure BatchSubResponse is bounded to a correct sub-request

        # Arrange
        self.bs.create_container(self.container_name)
        tiers = [StandardBlobTier.Archive, StandardBlobTier.Cool, StandardBlobTier.Hot,
                 StandardBlobTier.Archive, StandardBlobTier.Cool, StandardBlobTier.Hot,
                 StandardBlobTier.Archive, StandardBlobTier.Cool, StandardBlobTier.Hot]

        batch_set_blob_tier_request = []
        # For even index, create batch delete sub-request for existing blob and their snapshot
        # For odd index, create batch delete sub-request for non-existing blob
        for i in range(0, len(tiers)):
            blob_name = str(i)
            if i % 2 is 0:
                data = b'hello world'
                self.bs.create_blob_from_bytes(self.container_name, blob_name, data)

            sub_request = BatchSetBlobTierSubRequest(self.container_name, blob_name, tiers[i])
            batch_set_blob_tier_request.append(sub_request)

        # Act
        resp = self.bs.batch_set_standard_blob_tier(batch_set_blob_tier_request)

        # Assert
        self.assertIsNotNone(resp)
        self.assertEquals(len(batch_set_blob_tier_request), len(resp))
        for i in range(0, len(tiers)):
            is_successful = resp[i].is_successful
            # for every even indexed sub-request, the blob should be deleted successfully
            if i % 2 is 0:
                self.assertEquals(is_successful, True, "sub-request" + str(i) + "should be true")
            # For every odd indexed sub-request, there should be a 404 http status code because the blob is non-existing
            else:
                self.assertEquals(is_successful, False, "sub-request" + str(i) + "should be false")
                self.assertEquals(404, resp[i].http_response.status)

    @record
    def test_batch_set_standard_blob_tier_api_with_non_askii_blob_name(self):
        # Arrange
        self.bs.create_container(self.container_name)
        tiers = [StandardBlobTier.Archive, StandardBlobTier.Cool, StandardBlobTier.Hot]

        batch_set_blob_tier_request = []
        for tier in tiers:
            blob_name = "ööööööööö"
            data = b'hello world'
            self.bs.create_blob_from_bytes(self.container_name, blob_name, data)

            sub_request = BatchSetBlobTierSubRequest(self.container_name, blob_name, tier)
            batch_set_blob_tier_request.append(sub_request)

        # Act
        resp = self.bs.batch_set_standard_blob_tier(batch_set_blob_tier_request)

        # Assert
        self.assertIsNotNone(resp)
        self.assertEquals(len(batch_set_blob_tier_request), len(resp))
        for sub_response in resp:
            self.assertTrue(sub_response.is_successful)

    @record
    def test_batch_set_non_existing_blob_tier(self):
        # Arrange
        self.bs.create_container(self.container_name)
        tiers = [StandardBlobTier.Archive, StandardBlobTier.Cool, StandardBlobTier.Hot]

        batch_set_blob_tier_request = []
        for tier in tiers:
            blob_name = self._get_blob_reference()
            sub_request = BatchSetBlobTierSubRequest(self.container_name, blob_name, tier)
            batch_set_blob_tier_request.append(sub_request)

        # Act
        resp = self.bs.batch_set_standard_blob_tier(batch_set_blob_tier_request)

        # Assert
        self.assertIsNotNone(resp)
        self.assertEquals(len(batch_set_blob_tier_request), len(resp))
        for sub_response in resp:
            self.assertFalse(sub_response.is_successful)

    @record
    def test_rehydration_status(self):
        blob_name = 'rehydration_test_blob_1'
        blob_name2 = 'rehydration_test_blob_2'

        data = b'hello world'
        self.bs.create_blob_from_bytes(self.container_name, blob_name, data)
        self.bs.set_standard_blob_tier(self.container_name, blob_name, StandardBlobTier.Archive)
        self.bs.set_standard_blob_tier(self.container_name, blob_name, StandardBlobTier.Cool)

        blob_ref = self.bs.get_blob_properties(self.container_name, blob_name)
        self.assertEqual(StandardBlobTier.Archive, blob_ref.properties.blob_tier)
        self.assertEqual("rehydrate-pending-to-cool", blob_ref.properties.rehydration_status)
        self.assertFalse(blob_ref.properties.blob_tier_inferred)

        blobs = list(self.bs.list_blobs(self.container_name))
        self.bs.delete_blob(self.container_name, blob_name)

        # Assert
        self.assertIsNotNone(blobs)
        self.assertGreaterEqual(len(blobs), 1)
        self.assertIsNotNone(blobs[0])
        self.assertNamedItemInContainer(blobs, blob_name)
        self.assertEqual(StandardBlobTier.Archive, blobs[0].properties.blob_tier)
        self.assertEqual("rehydrate-pending-to-cool", blobs[0].properties.rehydration_status)
        self.assertFalse(blobs[0].properties.blob_tier_inferred)

        self.bs.create_blob_from_bytes(self.container_name, blob_name2, data)
        self.bs.set_standard_blob_tier(self.container_name, blob_name2, StandardBlobTier.Archive)
        self.bs.set_standard_blob_tier(self.container_name, blob_name2, StandardBlobTier.Hot)

        blob_ref2 = self.bs.get_blob_properties(self.container_name, blob_name2)
        self.assertEqual(StandardBlobTier.Archive, blob_ref2.properties.blob_tier)
        self.assertEqual("rehydrate-pending-to-hot", blob_ref2.properties.rehydration_status)
        self.assertFalse(blob_ref2.properties.blob_tier_inferred)

        blobs = list(self.bs.list_blobs(self.container_name))

        # Assert
        self.assertIsNotNone(blobs)
        self.assertGreaterEqual(len(blobs), 1)
        self.assertIsNotNone(blobs[0])
        self.assertNamedItemInContainer(blobs, blob_name2)
        self.assertEqual(StandardBlobTier.Archive, blobs[0].properties.blob_tier)
        self.assertEqual("rehydrate-pending-to-hot", blobs[0].properties.rehydration_status)
        self.assertFalse(blobs[0].properties.blob_tier_inferred)


# ------------------------------------------------------------------------------
if __name__ == '__main__':
    unittest.main()
