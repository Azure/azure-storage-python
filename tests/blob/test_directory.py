# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import datetime

from azure.common import (
    AzureHttpError,
    AzureMissingResourceHttpError,
    AzureConflictHttpError,
)

from azure.storage.blob import (
    BlockBlobService,
)
from azure.storage.blob.baseblobservice import (
    BaseBlobService,
)
from azure.storage.common import (
    LocationMode,
    ExponentialRetry,
)
from tests.testcase import (
    StorageTestCase,
    TestMode,
    record,
)


class StorageDirectoryTest(StorageTestCase):
    def setUp(self):
        super(StorageDirectoryTest, self).setUp()
        self.bs = self._create_storage_service(BlockBlobService, self.settings)
        self.bs_namespace = self._create_storage_service_with_hierarchical_namespace(BlockBlobService, self.settings)

        # shorten retries for faster failures
        self.bs.retry = ExponentialRetry(initial_backoff=1, increment_base=2, max_attempts=3).retry
        self.bs_namespace.retry = ExponentialRetry(initial_backoff=1, increment_base=2, max_attempts=3).retry

        self.container_name = self.get_resource_name('utcontainer')
        if not self.is_playback():
            self.bs.create_container(self.container_name)
            self.bs_namespace._create_file_system(self.container_name)

    def tearDown(self):
        if not self.is_playback():
            self.bs.delete_container(self.container_name)
            self.bs_namespace._delete_file_system(self.container_name)
        return super(StorageDirectoryTest, self).tearDown()

    def _get_directory_reference(self, suffix=""):
        return self.get_resource_name("directorytest" + suffix)

    def _get_blob_reference(self, directory_path):
        return "{}/{}".format(directory_path, self.get_resource_name("blob"))

    def _create_sub_dirs(self, blob_service, directory_name, num_of_sub_dir):
        import concurrent.futures
        import itertools
        # Use a thread pool because it is too slow otherwise
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            def create_sub_dir():
                blob_service.create_directory(self.container_name,
                                              "{}/{}".format(directory_name, self._get_directory_reference()))

            futures = {executor.submit(create_sub_dir) for _ in itertools.repeat(None, num_of_sub_dir)}
            concurrent.futures.wait(futures)

    def test_host_swapping(self):
        # Arrange
        example_blob_hosts = {LocationMode.PRIMARY: "account.blob.core.windows.net",
                              LocationMode.SECONDARY: "account-secondary.blob.core.windows.net"}

        # Act
        swapped_hosts = BaseBlobService._swap_blob_endpoints(example_blob_hosts)

        # Assert
        # DFS only supports the primary endpoint
        self.assertEqual(len(swapped_hosts), 1)
        self.assertEqual(swapped_hosts[LocationMode.PRIMARY], "account.dfs.core.windows.net")

    @record
    def test_create_delete_directory_without_hierarchical_namespace(self):
        self.create_delete_directory_simple_test_implementation(self.bs)
        self.create_directory_with_permission_implementation(self.bs, hierarchical_namespace_enabled=False)
        self.delete_directory_marker_test_implementation(self.bs)
        self.delete_directory_recursive_test_implementation(self.bs)
        self.delete_directory_access_conditions_test_implementation(self.bs)

    @record
    def test_create_delete_directory_with_hierarchical_namespace(self):
        self.create_delete_directory_simple_test_implementation(self.bs_namespace)
        self.create_directory_with_permission_implementation(self.bs_namespace, hierarchical_namespace_enabled=True)
        self.delete_directory_recursive_test_implementation(self.bs_namespace)

        # TODO encountering error 500 for if_match and if_none_match condition, uncomment after service fix
        # self.delete_directory_access_conditions_test_implementation(self.bs_namespace)

    def create_delete_directory_simple_test_implementation(self, blob_service):
        # Arrange
        directory_name = self._get_directory_reference()
        metadata = {"foo": "bar", "mama": "mia"}

        # Act
        props = blob_service.create_directory(self.container_name, directory_name, metadata=metadata)

        # Assert
        self.assertIsNotNone(props)
        self.assertIsNotNone(props.etag)
        self.assertIsNotNone(props.last_modified)

        # Act
        # Creating the same directory again
        self.sleep(1)
        props2 = blob_service.create_directory(self.container_name, directory_name)

        # Assert
        self.assertIsNotNone(props2)
        self.assertNotEqual(props.etag, props2.etag)
        self.assertNotEqual(props.last_modified, props2.last_modified)

        # Act
        deleted, marker = blob_service.delete_directory(self.container_name, directory_name)

        # Assert
        self.assertTrue(deleted)

        # Act
        # Delete an already non-existing directory
        deleted, marker = blob_service.delete_directory(self.container_name, directory_name)

        # Assert
        self.assertFalse(deleted)

        # Act
        # Delete an already non-existing directory, but with let it throw an error
        with self.assertRaises(AzureMissingResourceHttpError):
            blob_service.delete_directory(self.container_name, directory_name, fail_not_exist=True)

    def create_directory_with_permission_implementation(self, blob_service, hierarchical_namespace_enabled):
        # Arrange
        directory_name = self._get_directory_reference()

        if not hierarchical_namespace_enabled:
            # Act
            # Create with permission and umask is expected to fail due to the lack of namespace service
            with self.assertRaises(AzureHttpError):
                blob_service.create_directory(self.container_name, directory_name,
                                              posix_permissions='rwxrw-rw-', posix_umask='0022')
        else:
            # Act
            # Create with permission and umask
            props = blob_service.create_directory(self.container_name, directory_name,
                                                  posix_permissions='rwxrw-rw-', posix_umask='0022')

            # Assert
            self.assertIsNotNone(props)
            self.assertIsNotNone(props.etag)
            self.assertIsNotNone(props.last_modified)

            # Cleanup
            blob_service.delete_directory(self.container_name, directory_name)

    def delete_directory_marker_test_implementation(self, blob_service):
        # this test is too costly(too many requests to the service) and should only run in live mode
        if TestMode.need_recording_file(self.test_mode):
            return

        # Arrange
        directory_name = self._get_directory_reference()
        blob_service.create_directory(self.container_name, directory_name)

        # Create enough sub-directories to trigger the service to return a marker
        self._create_sub_dirs(blob_service, directory_name, num_of_sub_dir=1000)

        # Act
        deleted, marker = blob_service.delete_directory(self.container_name, directory_name)

        # Assert
        self.assertTrue(deleted)
        self.assertIsNotNone(marker)

        # Act
        # Continue the delete
        count = 0
        while marker is not None:
            deleted, new_marker = blob_service.delete_directory(self.container_name, directory_name, marker=marker)

            # Assert
            self.assertTrue(deleted)
            self.assertNotEqual(marker, new_marker)
            marker = new_marker
            count += 1

        self.logger.info("Took {} calls to finish deleting.".format(count))

    def delete_directory_recursive_test_implementation(self, blob_service):
        # Arrange
        directory_name = self._get_directory_reference()
        sub_directory_name = self._get_directory_reference()
        blob_service.create_directory(self.container_name, directory_name)
        blob_service.create_directory(self.container_name, "{}/{}".format(directory_name, sub_directory_name))

        # Act
        with self.assertRaises(AzureConflictHttpError):
            blob_service.delete_directory(self.container_name, directory_name, recursive=False)

        deleted, marker = blob_service.delete_directory(self.container_name, directory_name, recursive=True)
        self.assertTrue(deleted)
        self.assertIsNone(marker)

    def delete_directory_access_conditions_test_implementation(self, blob_service):
        # Arrange
        directory_name = self._get_directory_reference()
        props = blob_service.create_directory(self.container_name, directory_name)

        # if_match fails
        with self.assertRaises(AzureHttpError):
            blob_service.delete_directory(self.container_name, directory_name, if_match='0x111111111111111')

        # if_match succeeds
        deleted, marker = blob_service.delete_directory(self.container_name, directory_name,
                                                        if_match=props.etag)
        self.assertTrue(deleted)
        self.assertIsNone(marker)

        # Arrange
        props = blob_service.create_directory(self.container_name, directory_name)

        # if_none_match fails
        with self.assertRaises(AzureHttpError):
            blob_service.delete_directory(self.container_name, directory_name, if_none_match=props.etag)

        # if_none_match succeeds
        deleted, marker = blob_service.delete_directory(self.container_name, directory_name,
                                                        if_none_match='0x111111111111111')
        self.assertTrue(deleted)
        self.assertIsNone(marker)

        # Arrange
        props = blob_service.create_directory(self.container_name, directory_name)

        # if_modified_since fails
        with self.assertRaises(AzureHttpError):
            blob_service.delete_directory(self.container_name, directory_name, if_modified_since=props.last_modified)

        # if_modified_since succeeds
        deleted, marker = blob_service.delete_directory(self.container_name, directory_name,
                                                        if_modified_since=props.last_modified - datetime.timedelta(
                                                            minutes=1))
        self.assertTrue(deleted)
        self.assertIsNone(marker)

        # Arrange
        props = blob_service.create_directory(self.container_name, directory_name)

        # if_unmodified_since fails
        with self.assertRaises(AzureHttpError):
            blob_service.delete_directory(self.container_name, directory_name,
                                          if_unmodified_since=props.last_modified - datetime.timedelta(
                                              minutes=1))

        # if_unmodified_since succeeds
        deleted, marker = blob_service.delete_directory(self.container_name, directory_name,
                                                        if_unmodified_since=props.last_modified)
        self.assertTrue(deleted)
        self.assertIsNone(marker)

    @record
    def test_rename_directory_without_hierarchical_namespace(self):
        self.rename_directory_simple_test_implementation(self.bs)
        self.rename_directory_marker_test_implementation(self.bs)

        # TODO error 500s get thrown for precondition fail, uncomment after service fix
        # self.rename_directory_access_conditions_test_implementation(self.bs)

    @record
    def test_rename_directory_with_hierarchical_namespace(self):
        self.rename_directory_simple_test_implementation(self.bs_namespace)

        # TODO access conditions get ignored, uncomment after service fix
        # self.rename_directory_access_conditions_test_implementation(self.bs_namespace)

    def rename_directory_simple_test_implementation(self, blob_service):
        # Arrange
        directory_name = self._get_directory_reference()

        # Act
        props = blob_service.create_directory(self.container_name, directory_name)

        # Assert
        self.assertIsNotNone(props)
        self.assertIsNotNone(props.etag)
        self.assertIsNotNone(props.last_modified)

        # Arrange
        new_directory_name = self._get_directory_reference("new")

        # Act
        marker = blob_service.rename_directory(self.container_name, new_directory_name, directory_name)

        # Assert
        self.assertIsNone(marker)

    def rename_directory_marker_test_implementation(self, blob_service):
        # this test is too costly(too many requests to the service) and should only run in live mode
        if TestMode.need_recording_file(self.test_mode):
            return

        # Arrange
        directory_name = self._get_directory_reference()
        new_directory_name = self._get_directory_reference("new")
        blob_service.create_directory(self.container_name, directory_name)

        # Create enough sub-directories to trigger the service to return a marker
        self._create_sub_dirs(blob_service, directory_name, num_of_sub_dir=1000)

        # Act
        marker = blob_service.rename_directory(self.container_name, new_directory_name, directory_name)

        # Assert
        self.assertIsNotNone(marker)

        # Act
        # Continue the rename
        count = 0
        while marker is not None:
            new_marker = blob_service.rename_directory(self.container_name, new_directory_name, directory_name,
                                                       marker=marker)
            self.assertNotEqual(marker, new_marker)
            marker = new_marker
            count += 1

        self.logger.info("Took {} calls to finish renaming.".format(count))

    def rename_directory_access_conditions_test_implementation(self, blob_service):
        # Arrange
        directory_name = self._get_directory_reference()
        new_directory_name = self._get_directory_reference("new")
        props = blob_service.create_directory(self.container_name, directory_name)

        # if_match fails
        with self.assertRaises(AzureHttpError):
            blob_service.rename_directory(self.container_name, new_directory_name, directory_name,
                                          source_if_match='0x111111111111111')

        # if_match succeeds
        marker = blob_service.rename_directory(self.container_name, new_directory_name, directory_name,
                                               source_if_match=props.etag)
        self.assertIsNone(marker)

        # Arrange
        props = blob_service.create_directory(self.container_name, directory_name)

        # if_none_match fails
        with self.assertRaises(AzureHttpError):
            blob_service.rename_directory(self.container_name, new_directory_name, directory_name,
                                          source_if_none_match=props.etag)

        # if_none_match succeeds
        marker = blob_service.rename_directory(self.container_name, new_directory_name, directory_name,
                                               source_if_none_match='0x111111111111111')
        self.assertIsNone(marker)

        # Arrange
        props = blob_service.create_directory(self.container_name, directory_name)

        # if_modified_since fails
        with self.assertRaises(AzureHttpError):
            blob_service.rename_directory(self.container_name, new_directory_name, directory_name,
                                          source_if_modified_since=props.last_modified)

        # if_modified_since succeeds
        marker = blob_service.rename_directory(self.container_name, new_directory_name, directory_name,
                                               source_if_modified_since=props.last_modified - datetime.timedelta(
                                                   minutes=1))
        self.assertIsNone(marker)

        # Arrange
        props = blob_service.create_directory(self.container_name, directory_name)

        # if_unmodified_since fails
        with self.assertRaises(AzureHttpError):
            blob_service.rename_directory(self.container_name, new_directory_name, directory_name,
                                          source_if_unmodified_since=props.last_modified - datetime.timedelta(
                                              minutes=1))

        # if_unmodified_since succeeds
        marker = blob_service.rename_directory(self.container_name, new_directory_name, directory_name,
                                               source_if_unmodified_since=props.last_modified)
        self.assertIsNone(marker)
