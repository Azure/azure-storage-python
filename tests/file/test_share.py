# coding: utf-8

import unittest
from datetime import datetime, timedelta

# -------------------------------------------------------------------------
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
# --------------------------------------------------------------------------
import requests
from azure.common import (
    AzureHttpError,
    AzureConflictHttpError,
    AzureMissingResourceHttpError,
    AzureException,
)

from azure.storage.common import (
    AccessPolicy,
)
from azure.storage.file import (
    FileService,
    SharePermissions,
)
from tests.testcase import (
    StorageTestCase,
    TestMode,
    record,
)

# ------------------------------------------------------------------------------
TEST_SHARE_PREFIX = 'share'


# ------------------------------------------------------------------------------

class StorageShareTest(StorageTestCase):
    def setUp(self):
        super(StorageShareTest, self).setUp()

        self.fs = self._create_storage_service(FileService, self.settings)
        self.test_shares = []

    def tearDown(self):
        if not self.is_playback():
            for share_name in self.test_shares:
                try:
                    self.fs.delete_share(share_name)
                except:
                    pass
        return super(StorageShareTest, self).tearDown()

    # --Helpers-----------------------------------------------------------------
    def _get_share_reference(self, prefix=TEST_SHARE_PREFIX):
        share_name = self.get_resource_name(prefix)
        self.test_shares.append(share_name)
        return share_name

    def _create_share(self, prefix=TEST_SHARE_PREFIX):
        share_name = self._get_share_reference(prefix)
        self.fs.create_share(share_name)
        return share_name

    # --Test cases for shares -----------------------------------------
    @record
    def test_create_share(self):
        # Arrange
        share_name = self._get_share_reference()

        # Act
        created = self.fs.create_share(share_name)

        # Assert
        self.assertTrue(created)

    @record
    def test_create_share_fail_on_exist(self):
        # Arrange
        share_name = self._get_share_reference()

        # Act
        created = self.fs.create_share(share_name, None, None, True)

        # Assert
        self.assertTrue(created)

    @record
    def test_create_share_with_already_existing_share(self):
        # Arrange
        share_name = self._get_share_reference()

        # Act
        created1 = self.fs.create_share(share_name)
        created2 = self.fs.create_share(share_name)

        # Assert
        self.assertTrue(created1)
        self.assertFalse(created2)

    @record
    def test_create_share_with_already_existing_share_fail_on_exist(self):
        # Arrange
        share_name = self._get_share_reference()

        # Act
        created = self.fs.create_share(share_name)
        with self.assertRaises(AzureConflictHttpError):
            self.fs.create_share(share_name, None, None, True)

        # Assert
        self.assertTrue(created)

    @record
    def test_create_share_with_metadata(self):
        # Arrange
        share_name = self._get_share_reference()
        metadata = {'hello': 'world', 'number': '42'}

        # Act
        created = self.fs.create_share(share_name, metadata)

        # Assert
        self.assertTrue(created)
        md = self.fs.get_share_metadata(share_name)
        self.assertDictEqual(md, metadata)

    @record
    def test_create_share_with_quota(self):
        # Arrange
        share_name = self._get_share_reference()

        # Act
        self.fs.create_share(share_name, quota=1)

        # Assert
        share = self.fs.get_share_properties(share_name)
        self.assertIsNotNone(share)
        self.assertEqual(share.properties.quota, 1)

    @record
    def test_share_exists(self):
        # Arrange
        share_name = self._create_share()

        # Act
        exists = self.fs.exists(share_name)

        # Assert
        self.assertTrue(exists)

    @record
    def test_share_not_exists(self):
        # Arrange
        share_name = self._get_share_reference()

        # Act
        exists = self.fs.exists(share_name)

        # Assert
        self.assertFalse(exists)

    @record
    def test_unicode_create_share_unicode_name(self):
        # Arrange
        share_name = u'啊齄丂狛狜'

        # Act
        with self.assertRaises(AzureHttpError):
            # not supported - share name must be alphanumeric, lowercase
            self.fs.create_share(share_name)

            # Assert

    @record
    def test_list_shares_no_options(self):
        # Arrange
        share_name = self._create_share()

        # Act
        shares = list(self.fs.list_shares())

        # Assert
        self.assertIsNotNone(shares)
        self.assertGreaterEqual(len(shares), 1)
        self.assertIsNotNone(shares[0])
        self.assertNamedItemInContainer(shares, share_name)

    @record
    def test_list_shares_with_prefix(self):
        # Arrange
        share_name = self._create_share()

        # Act
        shares = list(self.fs.list_shares(prefix=share_name))

        # Assert
        self.assertIsNotNone(shares)
        self.assertEqual(len(shares), 1)
        self.assertIsNotNone(shares[0])
        self.assertEqual(shares[0].name, share_name)
        self.assertIsNone(shares[0].metadata)

    @record
    def test_list_shares_with_include_metadata(self):
        # Arrange
        share_name = self._create_share()
        metadata = {'hello': 'world', 'number': '42'}
        resp = self.fs.set_share_metadata(share_name, metadata)

        # Act
        shares = list(self.fs.list_shares(share_name, include_metadata=True))

        # Assert
        self.assertIsNotNone(shares)
        self.assertGreaterEqual(len(shares), 1)
        self.assertIsNotNone(shares[0])
        self.assertNamedItemInContainer(shares, share_name)
        self.assertDictEqual(shares[0].metadata, metadata)

    @record
    def test_list_shares_with_num_results_and_marker(self):
        # Arrange
        prefix = 'listshare'
        share_names = []
        for i in range(0, 4):
            share_names.append(self._create_share(prefix + str(i)))

        share_names.sort()

        # Act
        generator1 = self.fs.list_shares(prefix, num_results=2)
        generator2 = self.fs.list_shares(prefix,
                                         marker=generator1.next_marker,
                                         num_results=2)

        shares1 = generator1.items
        shares2 = generator2.items

        # Assert
        self.assertIsNotNone(shares1)
        self.assertEqual(len(shares1), 2)
        self.assertNamedItemInContainer(shares1, share_names[0])
        self.assertNamedItemInContainer(shares1, share_names[1])
        self.assertIsNotNone(shares2)
        self.assertEqual(len(shares2), 2)
        self.assertNamedItemInContainer(shares2, share_names[2])
        self.assertNamedItemInContainer(shares2, share_names[3])

    @record
    def test_set_share_metadata(self):
        # Arrange
        metadata = {'hello': 'world', 'number': '43'}
        share_name = self._create_share()

        # Act
        resp = self.fs.set_share_metadata(share_name, metadata)

        # Assert
        self.assertIsNone(resp)
        md = self.fs.get_share_metadata(share_name)
        self.assertDictEqual(md, metadata)

    @record
    def test_get_share_metadata(self):
        # Arrange
        metadata = {'hello': 'world', 'number': '42'}
        share_name = self._create_share()
        self.fs.set_share_metadata(share_name, metadata)

        # Act
        md = self.fs.get_share_metadata(share_name)

        # Assert
        self.assertDictEqual(md, metadata)

    @record
    def test_get_share_properties(self):
        # Arrange
        metadata = {'hello': 'world', 'number': '42'}
        share_name = self._create_share()
        self.fs.set_share_metadata(share_name, metadata)

        # Act
        props = self.fs.get_share_properties(share_name)

        # Assert
        self.assertIsNotNone(props)
        self.assertDictEqual(props.metadata, metadata)

    @record
    def test_set_share_properties(self):
        # Arrange
        share_name = self._create_share()
        self.fs.set_share_properties(share_name, 1)

        # Act
        props = self.fs.get_share_properties(share_name)

        # Assert
        self.assertIsNotNone(props)
        self.assertEqual(props.properties.quota, 1)

    @record
    def test_delete_share_with_existing_share(self):
        # Arrange
        share_name = self._create_share()

        # Act
        deleted = self.fs.delete_share(share_name)

        # Assert
        self.assertTrue(deleted)
        exists = self.fs.exists(share_name)
        self.assertFalse(exists)

    @record
    def test_delete_share_with_existing_share_fail_not_exist(self):
        # Arrange
        share_name = self._create_share()

        # Act
        deleted = self.fs.delete_share(share_name, True)

        # Assert
        self.assertTrue(deleted)
        exists = self.fs.exists(share_name)
        self.assertFalse(exists)

    @record
    def test_delete_share_with_non_existing_share(self):
        # Arrange
        share_name = self._get_share_reference()

        # Act
        deleted = self.fs.delete_share(share_name)

        # Assert
        self.assertFalse(deleted)

    @record
    def test_delete_share_with_non_existing_share_fail_not_exist(self):
        # Arrange
        share_name = self._get_share_reference()

        # Act
        with self.assertRaises(AzureMissingResourceHttpError):
            self.fs.delete_share(share_name, True)

            # Assert

    @record
    def test_get_share_stats(self):
        # Arrange
        share_name = self._create_share()
        self.fs.create_file_from_text(share_name, None, 'file1', b'hello world')

        # Act
        share_usage = self.fs.get_share_stats(share_name)

        # Assert
        self.assertEqual(share_usage, 1)

    @record
    def test_set_share_acl(self):
        # Arrange
        share_name = self._create_share()

        # Act
        resp = self.fs.set_share_acl(share_name)

        # Assert
        self.assertIsNone(resp)
        acl = self.fs.get_share_acl(share_name)
        self.assertIsNotNone(acl)

    @record
    def test_set_share_acl_with_empty_signed_identifiers(self):
        # Arrange
        share_name = self._create_share()

        # Act
        self.fs.set_share_acl(share_name, dict())

        # Assert
        acl = self.fs.get_share_acl(share_name)
        self.assertIsNotNone(acl)
        self.assertEqual(len(acl), 0)

    @record
    def test_set_share_acl_with_empty_signed_identifier(self):
        # Arrange
        share_name = self._create_share()

        # Act
        self.fs.set_share_acl(share_name, {'empty': AccessPolicy()})

        # Assert
        acl = self.fs.get_share_acl(share_name)
        self.assertIsNotNone(acl)
        self.assertEqual(len(acl), 1)
        self.assertIsNotNone(acl['empty'])
        self.assertIsNone(acl['empty'].permission)
        self.assertIsNone(acl['empty'].expiry)
        self.assertIsNone(acl['empty'].start)

    @record
    def test_set_share_acl_with_signed_identifiers(self):
        # Arrange
        share_name = self._create_share()

        # Act
        identifiers = dict()
        identifiers['testid'] = AccessPolicy(
            permission=SharePermissions.READ,
            expiry=datetime.utcnow() + timedelta(hours=1),
            start=datetime.utcnow() - timedelta(minutes=1),
        )

        resp = self.fs.set_share_acl(share_name, identifiers)

        # Assert
        self.assertIsNone(resp)
        acl = self.fs.get_share_acl(share_name)
        self.assertIsNotNone(acl)
        self.assertEqual(len(acl), 1)
        self.assertTrue('testid' in acl)

    @record
    def test_set_share_acl_too_many_ids(self):
        # Arrange
        share_name = self._create_share()

        # Act
        identifiers = dict()
        for i in range(0, 6):
            identifiers['id{}'.format(i)] = AccessPolicy()

        # Assert
        with self.assertRaisesRegexp(AzureException,
                                     'Too many access policies provided. The server does not support setting more than 5 access policies on a single resource.'):
            self.fs.set_share_acl(share_name, identifiers)

    @record
    def test_list_directories_and_files(self):
        # Arrange
        share_name = self._create_share()
        self.fs.create_directory(share_name, 'dir1')
        self.fs.create_directory(share_name, 'dir2')
        self.fs.create_file(share_name, None, 'file1', 1024)
        self.fs.create_file(share_name, 'dir1', 'file2', 1025)

        # Act
        resp = list(self.fs.list_directories_and_files(share_name))

        # Assert
        self.assertIsNotNone(resp)
        self.assertEqual(len(resp), 3)
        self.assertIsNotNone(resp[0])
        self.assertNamedItemInContainer(resp, 'dir1')
        self.assertNamedItemInContainer(resp, 'dir2')
        self.assertNamedItemInContainer(resp, 'file1')

    @record
    def test_list_directories_and_files_with_num_results(self):
        # Arrange
        share_name = self._create_share()
        self.fs.create_directory(share_name, 'dir1')
        self.fs.create_file(share_name, None, 'filea1', 1024)
        self.fs.create_file(share_name, None, 'filea2', 1024)
        self.fs.create_file(share_name, None, 'filea3', 1024)
        self.fs.create_file(share_name, None, 'fileb1', 1024)

        # Act
        result = list(self.fs.list_directories_and_files(share_name, num_results=2))

        # Assert
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 2)
        self.assertNamedItemInContainer(result, 'dir1')
        self.assertNamedItemInContainer(result, 'filea1')

    @record
    def test_list_directories_and_files_with_num_results_and_marker(self):
        # Arrange
        share_name = self._create_share()
        self.fs.create_directory(share_name, 'dir1')
        self.fs.create_file(share_name, 'dir1', 'filea1', 1024)
        self.fs.create_file(share_name, 'dir1', 'filea2', 1024)
        self.fs.create_file(share_name, 'dir1', 'filea3', 1024)
        self.fs.create_file(share_name, 'dir1', 'fileb1', 1024)

        # Act
        generator1 = self.fs.list_directories_and_files(share_name, 'dir1', num_results=2)
        generator2 = self.fs.list_directories_and_files(share_name, 'dir1',
                                                        marker=generator1.next_marker,
                                                        num_results=2)

        result1 = generator1.items
        result2 = generator2.items

        # Assert
        self.assertEqual(len(result1), 2)
        self.assertEqual(len(result2), 2)
        self.assertNamedItemInContainer(result1, 'filea1')
        self.assertNamedItemInContainer(result1, 'filea2')
        self.assertNamedItemInContainer(result2, 'filea3')
        self.assertNamedItemInContainer(result2, 'fileb1')
        self.assertEqual(result2.next_marker, None)

    @record
    def test_list_directories_and_files_with_prefix(self):
        # Arrange
        share_name = self._create_share()
        self.fs.create_directory(share_name, 'dir1')
        self.fs.create_directory(share_name, 'dir2')
        self.fs.create_file(share_name, None, 'file1', 1024)
        self.fs.create_directory(share_name, 'dir1/pref_dir3')
        self.fs.create_file(share_name, 'dir1', 'pref_file2', 1025)
        self.fs.create_file(share_name, 'dir1', 'file3', 1025)

        # Act
        resp = list(self.fs.list_directories_and_files(share_name, 'dir1', prefix='pref'))

        # Assert
        self.assertIsNotNone(resp)
        self.assertEqual(len(resp), 2)
        self.assertIsNotNone(resp[0])
        self.assertNamedItemInContainer(resp, 'pref_file2')
        self.assertNamedItemInContainer(resp, 'pref_dir3')

    @record
    def test_shared_access_share(self):
        # SAS URL is calculated from storage key, so this test runs live only
        if TestMode.need_recording_file(self.test_mode):
            return

        # Arrange
        file_name = 'file1'
        dir_name = 'dir1'
        data = b'hello world'

        share_name = self._create_share()
        self.fs.create_directory(share_name, dir_name)
        self.fs.create_file_from_bytes(share_name, dir_name, file_name, data)

        token = self.fs.generate_share_shared_access_signature(
            share_name,
            expiry=datetime.utcnow() + timedelta(hours=1),
            permission=SharePermissions.READ,
        )
        url = self.fs.make_file_url(
            share_name,
            dir_name,
            file_name,
            sas_token=token,
        )

        # Act
        response = requests.get(url)

        # Assert
        self.assertTrue(response.ok)
        self.assertEqual(data, response.content)


# ------------------------------------------------------------------------------
if __name__ == '__main__':
    unittest.main()
