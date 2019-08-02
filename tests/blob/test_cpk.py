from io import BytesIO

from azure.common import AzureHttpError
from azure.storage.blob import (
    Blob,
    BlockBlobService,
    PageBlobService,
    AppendBlobService,
    CustomerProvidedEncryptionKey,
    BlobBlock,
    BlobPermissions,
    ContentSettings)
from tests.testcase import (
    StorageTestCase,
    record,
)
from datetime import (
    datetime,
    timedelta,
)
from tests.testcase import TestMode

# ------------------------------------------------------------------------------
TEST_ENCRYPTION_KEY = CustomerProvidedEncryptionKey(key_value="MDEyMzQ1NjcwMTIzNDU2NzAxMjM0NTY3MDEyMzQ1Njc=",
                                                    key_hash="3QFFFpRA5+XANHqwwbT4yXDmrT/2JaLt/FKHjzhOdoE=")


# ------------------------------------------------------------------------------

class StorageCPKTest(StorageTestCase):
    def setUp(self):
        super(StorageCPKTest, self).setUp()

        self.bbs = self._create_storage_service(BlockBlobService, self.settings)
        self.pbs = self._create_storage_service(PageBlobService, self.settings)
        self.abs = self._create_storage_service(AppendBlobService, self.settings)
        self.container_name = self.get_resource_name('utcontainer')

        # prep some test data so that they can be used in upload tests
        self.byte_data = self.get_random_bytes(64 * 1024)

        # create source blob to be copied from
        self.source_blob_name = self.get_resource_name('srcblob')

        if not self.is_playback():
            self.bbs.create_container(self.container_name)
            self.bbs.create_blob_from_bytes(self.container_name, self.source_blob_name, self.byte_data)

        # generate a SAS so that it is accessible with a URL
        sas_token = self.bbs.generate_blob_shared_access_signature(
            self.container_name,
            self.source_blob_name,
            permission=BlobPermissions.READ,
            expiry=datetime.utcnow() + timedelta(hours=1),
        )
        self.source_blob_url = self.bbs.make_blob_url(self.container_name, self.source_blob_name, sas_token=sas_token)

        # configure the block blob service so that we can test create_blob_from* APIs with more than 1 chunk
        self.bbs.MAX_BLOCK_SIZE = 1024
        self.bbs.MAX_SINGLE_PUT_SIZE = 1024
        self.bbs.MIN_LARGE_BLOCK_UPLOAD_THRESHOLD = 1024
        self.abs.MAX_BLOCK_SIZE = 1024
        self.pbs.MAX_PAGE_SIZE = 1024

    def tearDown(self):
        if not self.is_playback():
            try:
                self.bbs.delete_container(self.container_name)
            except:
                pass

        return super(StorageCPKTest, self).tearDown()

    # --Helpers-----------------------------------------------------------------

    def _get_blob_reference(self):
        return self.get_resource_name("cpk")

    # -- Test cases for APIs supporting CPK ----------------------------------------------

    @record
    def test_put_block_and_put_block_list(self):
        # Arrange
        blob_name = self._get_blob_reference()
        self.bbs.put_block(self.container_name, blob_name, b'AAA', '1', cpk=TEST_ENCRYPTION_KEY)
        self.bbs.put_block(self.container_name, blob_name, b'BBB', '2', cpk=TEST_ENCRYPTION_KEY)
        self.bbs.put_block(self.container_name, blob_name, b'CCC', '3', cpk=TEST_ENCRYPTION_KEY)

        # Act
        block_list = [BlobBlock(id='1'), BlobBlock(id='2'), BlobBlock(id='3')]
        put_block_list_resp = self.bbs.put_block_list(self.container_name, blob_name, block_list,
                                                      cpk=TEST_ENCRYPTION_KEY)

        # Assert
        self.assertIsNotNone(put_block_list_resp.etag)
        self.assertIsNotNone(put_block_list_resp.last_modified)
        self.assertTrue(put_block_list_resp.request_server_encrypted)
        self.assertEqual(put_block_list_resp.encryption_key_sha256, TEST_ENCRYPTION_KEY.key_hash)

        # Act get the blob content without cpk should fail
        with self.assertRaises(AzureHttpError):
            self.bbs.get_blob_to_bytes(self.container_name, blob_name)

        # Act get the blob content
        blob = self.bbs.get_blob_to_bytes(self.container_name, blob_name, cpk=TEST_ENCRYPTION_KEY)

        # Assert content was retrieved with the cpk
        self.assertEqual(blob.content, b'AAABBBCCC')
        self.assertEqual(blob.properties.etag, put_block_list_resp.etag)
        self.assertEqual(blob.properties.last_modified, put_block_list_resp.last_modified)
        self.assertEqual(blob.properties.encryption_key_sha256, TEST_ENCRYPTION_KEY.key_hash)

    @record
    def test_create_block_blob_with_chunks(self):
        # parallel operation
        if TestMode.need_recording_file(self.test_mode):
            return

        # Arrange
        blob_name = self._get_blob_reference()

        # Act
        # create_blob_from_bytes forces the in-memory chunks to be used
        put_block_list_resp = self.bbs.create_blob_from_bytes(self.container_name, blob_name, self.byte_data,
                                                              cpk=TEST_ENCRYPTION_KEY)

        # Assert
        self.assertIsNotNone(put_block_list_resp.etag)
        self.assertIsNotNone(put_block_list_resp.last_modified)
        self.assertTrue(put_block_list_resp.request_server_encrypted)
        self.assertEqual(put_block_list_resp.encryption_key_sha256, TEST_ENCRYPTION_KEY.key_hash)

        # Act get the blob content without cpk should fail
        with self.assertRaises(AzureHttpError):
            self.bbs.get_blob_to_bytes(self.container_name, blob_name)

        # Act get the blob content
        blob = self.bbs.get_blob_to_bytes(self.container_name, blob_name, cpk=TEST_ENCRYPTION_KEY)

        # Assert content was retrieved with the cpk
        self.assertEqual(blob.content, self.byte_data)
        self.assertEqual(blob.properties.etag, put_block_list_resp.etag)
        self.assertEqual(blob.properties.last_modified, put_block_list_resp.last_modified)
        self.assertEqual(blob.properties.encryption_key_sha256, TEST_ENCRYPTION_KEY.key_hash)

    @record
    def test_create_block_blob_with_sub_streams(self):
        # problem with the recording framework can only run live
        if TestMode.need_recording_file(self.test_mode):
            return

        # Arrange
        blob_name = self._get_blob_reference()

        # Act
        stream = BytesIO(self.byte_data)
        put_block_list_resp = self.bbs.create_blob_from_stream(self.container_name, blob_name, stream,
                                                               cpk=TEST_ENCRYPTION_KEY)

        # Assert
        self.assertIsNotNone(put_block_list_resp.etag)
        self.assertIsNotNone(put_block_list_resp.last_modified)
        self.assertTrue(put_block_list_resp.request_server_encrypted)
        self.assertEqual(put_block_list_resp.encryption_key_sha256, TEST_ENCRYPTION_KEY.key_hash)

        # Act get the blob content without cpk should fail
        with self.assertRaises(AzureHttpError):
            self.bbs.get_blob_to_bytes(self.container_name, blob_name)

        # Act get the blob content
        blob = self.bbs.get_blob_to_bytes(self.container_name, blob_name, cpk=TEST_ENCRYPTION_KEY)

        # Assert content was retrieved with the cpk
        self.assertEqual(blob.content, self.byte_data)
        self.assertEqual(blob.properties.etag, put_block_list_resp.etag)
        self.assertEqual(blob.properties.last_modified, put_block_list_resp.last_modified)
        self.assertEqual(blob.properties.encryption_key_sha256, TEST_ENCRYPTION_KEY.key_hash)

    @record
    def test_create_block_blob_with_single_chunk(self):
        # Arrange
        blob_name = self._get_blob_reference()

        # Act
        put_block_list_resp = self.bbs.create_blob_from_bytes(self.container_name, blob_name, b'AAABBBCCC',
                                                              cpk=TEST_ENCRYPTION_KEY)

        # Assert
        self.assertIsNotNone(put_block_list_resp.etag)
        self.assertIsNotNone(put_block_list_resp.last_modified)
        self.assertTrue(put_block_list_resp.request_server_encrypted)
        self.assertEqual(put_block_list_resp.encryption_key_sha256, TEST_ENCRYPTION_KEY.key_hash)

        # Act get the blob content without cpk should fail
        with self.assertRaises(AzureHttpError):
            self.bbs.get_blob_to_bytes(self.container_name, blob_name)

        # Act get the blob content
        blob = self.bbs.get_blob_to_bytes(self.container_name, blob_name, cpk=TEST_ENCRYPTION_KEY)

        # Assert content was retrieved with the cpk
        self.assertEqual(blob.content, b'AAABBBCCC')
        self.assertEqual(blob.properties.etag, put_block_list_resp.etag)
        self.assertEqual(blob.properties.last_modified, put_block_list_resp.last_modified)
        self.assertEqual(blob.properties.encryption_key_sha256, TEST_ENCRYPTION_KEY.key_hash)

    @record
    def test_put_block_from_url_and_commit(self):
        # Arrange
        dest_blob_name = self._get_blob_reference()

        # Act part 1: make put block from url calls
        self.bbs.put_block_from_url(self.container_name, dest_blob_name, self.source_blob_url,
                                    source_range_start=0, source_range_end=4 * 1024 - 1, block_id=1,
                                    cpk=TEST_ENCRYPTION_KEY)
        self.bbs.put_block_from_url(self.container_name, dest_blob_name, self.source_blob_url,
                                    source_range_start=4 * 1024, source_range_end=8 * 1024, block_id=2,
                                    cpk=TEST_ENCRYPTION_KEY)

        # Assert blocks
        block_list = self.bbs.get_block_list(self.container_name, dest_blob_name, None, 'all')
        self.assertEqual(len(block_list.uncommitted_blocks), 2)
        self.assertEqual(len(block_list.committed_blocks), 0)

        # commit the blocks without cpk should fail
        block_list = [BlobBlock(id='1'), BlobBlock(id='2')]
        with self.assertRaises(AzureHttpError):
            self.bbs.put_block_list(self.container_name, dest_blob_name, block_list)

        # Act commit the blocks with cpk should succeed
        put_block_list_resp = self.bbs.put_block_list(self.container_name, dest_blob_name, block_list,
                                                      cpk=TEST_ENCRYPTION_KEY)

        # Assert
        self.assertIsNotNone(put_block_list_resp.etag)
        self.assertIsNotNone(put_block_list_resp.last_modified)
        self.assertTrue(put_block_list_resp.request_server_encrypted)
        self.assertEqual(put_block_list_resp.encryption_key_sha256, TEST_ENCRYPTION_KEY.key_hash)

        # Assert destination blob has right content
        blob = self.bbs.get_blob_to_bytes(self.container_name, dest_blob_name, cpk=TEST_ENCRYPTION_KEY)
        self.assertEqual(blob.content, self.byte_data[0: 8 * 1024 + 1])
        self.assertEqual(blob.properties.etag, put_block_list_resp.etag)
        self.assertEqual(blob.properties.last_modified, put_block_list_resp.last_modified)
        self.assertEqual(blob.properties.encryption_key_sha256, TEST_ENCRYPTION_KEY.key_hash)

    @record
    def test_append_block(self):
        # Arrange
        blob_name = self._get_blob_reference()
        self.abs.create_blob(self.container_name, blob_name, cpk=TEST_ENCRYPTION_KEY)

        # Act
        for content in [b'AAA', b'BBB', b'CCC']:
            append_blob_prop = self.abs.append_block(self.container_name, blob_name, content, cpk=TEST_ENCRYPTION_KEY)

            # Assert
            self.assertIsNotNone(append_blob_prop.etag)
            self.assertIsNotNone(append_blob_prop.last_modified)
            self.assertTrue(append_blob_prop.request_server_encrypted)
            self.assertEqual(append_blob_prop.encryption_key_sha256, TEST_ENCRYPTION_KEY.key_hash)

        # Act get the blob content without cpk should fail
        with self.assertRaises(AzureHttpError):
            self.abs.get_blob_to_bytes(self.container_name, blob_name)

        # Act get the blob content
        blob = self.abs.get_blob_to_bytes(self.container_name, blob_name, cpk=TEST_ENCRYPTION_KEY)

        # Assert content was retrieved with the cpk
        self.assertEqual(blob.content, b'AAABBBCCC')
        self.assertEqual(blob.properties.encryption_key_sha256, TEST_ENCRYPTION_KEY.key_hash)

    @record
    def test_append_block_from_url(self):
        # Arrange
        dest_blob_name = self._get_blob_reference()
        self.abs.create_blob(self.container_name, dest_blob_name, cpk=TEST_ENCRYPTION_KEY)

        # Act
        append_blob_prop = self.abs.append_block_from_url(self.container_name, dest_blob_name, self.source_blob_url,
                                                          source_range_start=0, source_range_end=4 * 1024 - 1,
                                                          cpk=TEST_ENCRYPTION_KEY)

        # Assert
        self.assertIsNotNone(append_blob_prop.etag)
        self.assertIsNotNone(append_blob_prop.last_modified)
        self.assertTrue(append_blob_prop.request_server_encrypted)
        self.assertEqual(append_blob_prop.encryption_key_sha256, TEST_ENCRYPTION_KEY.key_hash)

        # Act get the blob content without cpk should fail
        with self.assertRaises(AzureHttpError):
            self.abs.get_blob_to_bytes(self.container_name, dest_blob_name)

        # Act get the blob content
        blob = self.abs.get_blob_to_bytes(self.container_name, dest_blob_name, cpk=TEST_ENCRYPTION_KEY)

        # Assert content was retrieved with the cpk
        self.assertEqual(blob.content, self.byte_data[0: 4 * 1024])
        self.assertEqual(blob.properties.encryption_key_sha256, TEST_ENCRYPTION_KEY.key_hash)

    @record
    def test_create_append_blob_with_chunks(self):
        # Arrange
        blob_name = self._get_blob_reference()
        self.abs.create_blob(self.container_name, blob_name, cpk=TEST_ENCRYPTION_KEY)

        # Act
        append_blob_prop = self.abs.append_blob_from_bytes(self.container_name, blob_name, self.byte_data,
                                                           cpk=TEST_ENCRYPTION_KEY)

        # Assert
        self.assertIsNotNone(append_blob_prop.etag)
        self.assertIsNotNone(append_blob_prop.last_modified)
        self.assertTrue(append_blob_prop.request_server_encrypted)
        self.assertEqual(append_blob_prop.encryption_key_sha256, TEST_ENCRYPTION_KEY.key_hash)

        # Act get the blob content without cpk should fail
        with self.assertRaises(AzureHttpError):
            self.abs.get_blob_to_bytes(self.container_name, blob_name)

        # Act get the blob content
        blob = self.abs.get_blob_to_bytes(self.container_name, blob_name, cpk=TEST_ENCRYPTION_KEY)

        # Assert content was retrieved with the cpk
        self.assertEqual(blob.content, self.byte_data)
        self.assertEqual(blob.properties.encryption_key_sha256, TEST_ENCRYPTION_KEY.key_hash)

    @record
    def test_update_page(self):
        # Arrange
        blob_name = self._get_blob_reference()
        self.pbs.create_blob(self.container_name, blob_name, content_length=1024 * 1024, cpk=TEST_ENCRYPTION_KEY)

        # Act
        page_blob_prop = self.pbs.update_page(self.container_name, blob_name, self.byte_data,
                                              start_range=0,
                                              end_range=len(self.byte_data) - 1,
                                              cpk=TEST_ENCRYPTION_KEY)

        # Assert
        self.assertIsNotNone(page_blob_prop.etag)
        self.assertIsNotNone(page_blob_prop.last_modified)
        self.assertTrue(page_blob_prop.request_server_encrypted)
        self.assertEqual(page_blob_prop.encryption_key_sha256, TEST_ENCRYPTION_KEY.key_hash)

        # Act get the blob content without cpk should fail
        with self.assertRaises(AzureHttpError):
            self.pbs.get_blob_to_bytes(self.container_name, blob_name, start_range=0,
                                       end_range=len(self.byte_data) - 1)

        # Act get the blob content
        blob = self.pbs.get_blob_to_bytes(self.container_name, blob_name, cpk=TEST_ENCRYPTION_KEY, start_range=0,
                                          end_range=len(self.byte_data) - 1)

        # Assert content was retrieved with the cpk
        self.assertEqual(blob.content, self.byte_data)
        self.assertEqual(blob.properties.encryption_key_sha256, TEST_ENCRYPTION_KEY.key_hash)

    @record
    def test_update_page_from_url(self):
        # Arrange
        blob_name = self._get_blob_reference()
        self.pbs.create_blob(self.container_name, blob_name, content_length=1024 * 1024, cpk=TEST_ENCRYPTION_KEY)

        # Act
        page_blob_prop = self.pbs.update_page_from_url(self.container_name, blob_name,
                                                       start_range=0,
                                                       end_range=len(self.byte_data) - 1,
                                                       copy_source_url=self.source_blob_url,
                                                       source_range_start=0,
                                                       cpk=TEST_ENCRYPTION_KEY)

        # Assert
        self.assertIsNotNone(page_blob_prop.etag)
        self.assertIsNotNone(page_blob_prop.last_modified)
        self.assertTrue(page_blob_prop.request_server_encrypted)
        self.assertEqual(page_blob_prop.encryption_key_sha256, TEST_ENCRYPTION_KEY.key_hash)

        # Act get the blob content without cpk should fail
        with self.assertRaises(AzureHttpError):
            self.pbs.get_blob_to_bytes(self.container_name, blob_name, start_range=0,
                                       end_range=len(self.byte_data) - 1)

        # Act get the blob content
        blob = self.pbs.get_blob_to_bytes(self.container_name, blob_name, cpk=TEST_ENCRYPTION_KEY, start_range=0,
                                          end_range=len(self.byte_data) - 1)

        # Assert content was retrieved with the cpk
        self.assertEqual(blob.content, self.byte_data)
        self.assertEqual(blob.properties.encryption_key_sha256, TEST_ENCRYPTION_KEY.key_hash)

    @record
    def test_create_page_blob_with_chunks(self):
        if TestMode.need_recording_file(self.test_mode):
            return

        # Arrange
        blob_name = self._get_blob_reference()
        # Act
        page_blob_prop = self.pbs.create_blob_from_bytes(self.container_name, blob_name, self.byte_data,
                                                         cpk=TEST_ENCRYPTION_KEY)

        # Assert
        self.assertIsNotNone(page_blob_prop.etag)
        self.assertIsNotNone(page_blob_prop.last_modified)
        self.assertTrue(page_blob_prop.request_server_encrypted)
        self.assertEqual(page_blob_prop.encryption_key_sha256, TEST_ENCRYPTION_KEY.key_hash)

        # Act get the blob content without cpk should fail
        with self.assertRaises(AzureHttpError):
            self.pbs.get_blob_to_bytes(self.container_name, blob_name)

        # Act get the blob content
        blob = self.pbs.get_blob_to_bytes(self.container_name, blob_name, cpk=TEST_ENCRYPTION_KEY)

        # Assert content was retrieved with the cpk
        self.assertEqual(blob.content, self.byte_data)
        self.assertEqual(blob.properties.encryption_key_sha256, TEST_ENCRYPTION_KEY.key_hash)

    @record
    def test_get_set_blob_properties(self):
        # Arrange
        blob_name = self._get_blob_reference()
        self.bbs.create_blob_from_bytes(self.container_name, blob_name, b'AAABBBCCC', cpk=TEST_ENCRYPTION_KEY)

        # Act without the encryption key should fail
        with self.assertRaises(AzureHttpError):
            self.bbs.get_blob_properties(self.container_name, blob_name)

        # Act
        blob = self.bbs.get_blob_properties(self.container_name, blob_name, cpk=TEST_ENCRYPTION_KEY)

        # Assert
        self.assertTrue(blob.properties.server_encrypted)
        self.assertEqual(blob.properties.encryption_key_sha256, TEST_ENCRYPTION_KEY.key_hash)

        # Act set blob properties
        self.bbs.set_blob_properties(
            self.container_name,
            blob_name,
            content_settings=ContentSettings(
                content_language='spanish',
                content_disposition='inline'),
            cpk=TEST_ENCRYPTION_KEY,
        )

        # Assert
        blob = self.bbs.get_blob_properties(self.container_name, blob_name, cpk=TEST_ENCRYPTION_KEY)
        self.assertEqual(blob.properties.content_settings.content_language, 'spanish')
        self.assertEqual(blob.properties.content_settings.content_disposition, 'inline')

    @record
    def test_get_set_blob_metadata(self):
        # Arrange
        blob_name = self._get_blob_reference()
        self.bbs.create_blob_from_bytes(self.container_name, blob_name, b'AAABBBCCC', cpk=TEST_ENCRYPTION_KEY)
        metadata = {'hello': 'world', 'number': '42', 'UP': 'UPval'}

        # Act without cpk should fail
        with self.assertRaises(AzureHttpError):
            self.bbs.set_blob_metadata(self.container_name, blob_name, metadata)

        # Act with cpk should work
        self.bbs.set_blob_metadata(self.container_name, blob_name, metadata, cpk=TEST_ENCRYPTION_KEY)

        # Assert
        md = self.bbs.get_blob_metadata(self.container_name, blob_name, cpk=TEST_ENCRYPTION_KEY)
        self.assertEqual(3, len(md))
        self.assertEqual(md['hello'], 'world')
        self.assertEqual(md['number'], '42')
        self.assertEqual(md['UP'], 'UPval')
        self.assertFalse('up' in md)

        # Act get metadata without cpk should fail
        with self.assertRaises(AzureHttpError):
            self.bbs.get_blob_metadata(self.container_name, blob_name)

    @record
    def test_snapshot_blob(self):
        # Arrange
        blob_name = self._get_blob_reference()
        self.bbs.create_blob_from_bytes(self.container_name, blob_name, b'AAABBBCCC', cpk=TEST_ENCRYPTION_KEY)

        # Act without cpk should not work
        with self.assertRaises(AzureHttpError):
            self.bbs.snapshot_blob(self.container_name, blob_name)

        # Act with cpk should work
        blob_snapshot = self.bbs.snapshot_blob(self.container_name, blob_name, cpk=TEST_ENCRYPTION_KEY)

        # Assert
        self.assertIsNotNone(blob_snapshot)
