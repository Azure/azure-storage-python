# coding: utf-8

import locale
import os
import sys
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
import unittest
from datetime import datetime, timedelta

from azure.common import (
    AzureHttpError,
    AzureConflictHttpError,
    AzureMissingResourceHttpError,
    AzureException,
)

from azure.storage.common import (
    AccessPolicy,
    ResourceTypes,
    AccountPermissions,
)
from azure.storage.table import (
    TableService,
    TablePermissions,
)
from tests.testcase import (
    StorageTestCase,
    TestMode,
    record,
)

# ------------------------------------------------------------------------------
TEST_TABLE_PREFIX = 'table'


# ------------------------------------------------------------------------------


class StorageTableTest(StorageTestCase):
    def setUp(self):
        super(StorageTableTest, self).setUp()

        self.ts = self._create_storage_service(TableService, self.settings)
        self.test_tables = []

    def tearDown(self):
        if not self.is_playback():
            for table_name in self.test_tables:
                try:
                    self.ts.delete_table(table_name)
                except:
                    pass
        return super(StorageTableTest, self).tearDown()

    # --Helpers-----------------------------------------------------------------
    def _get_table_reference(self, prefix=TEST_TABLE_PREFIX):
        table_name = self.get_resource_name(prefix)
        self.test_tables.append(table_name)
        return table_name

    def _create_table(self, prefix=TEST_TABLE_PREFIX):
        table_name = self._get_table_reference(prefix)
        self.ts.create_table(table_name)
        return table_name

    # --Test cases for tables --------------------------------------------------
    @record
    def test_create_table(self):
        # Arrange
        table_name = self._get_table_reference()

        # Act
        created = self.ts.create_table(table_name)

        # Assert
        self.assertTrue(created)
        self.assertTrue(self.ts.exists(table_name))

    @record
    def test_create_table_fail_on_exist(self):
        # Arrange
        table_name = self._get_table_reference()

        # Act
        created = self.ts.create_table(table_name, True)

        # Assert
        self.assertTrue(created)
        self.assertTrue(self.ts.exists(table_name))

    @record
    def test_create_table_with_already_existing_table(self):
        # Arrange
        table_name = self._get_table_reference()

        # Act
        created1 = self.ts.create_table(table_name)
        created2 = self.ts.create_table(table_name)

        # Assert
        self.assertTrue(created1)
        self.assertFalse(created2)
        self.assertTrue(self.ts.exists(table_name))

    @record
    def test_create_table_with_already_existing_table_fail_on_exist(self):
        # Arrange
        table_name = self._create_table()

        # Act
        with self.assertRaises(AzureConflictHttpError):
            self.ts.create_table(table_name, True)

            # Assert

    @record
    def test_table_exists(self):
        # Arrange
        table_name = self._create_table()

        # Act
        exists = self.ts.exists(table_name)

        # Assert
        self.assertTrue(exists)

    @record
    def test_table_not_exists(self):
        # Arrange
        table_name = self._get_table_reference()

        # Act
        exists = self.ts.exists(table_name)

        # Assert
        self.assertFalse(exists)

    @record
    def test_list_tables(self):
        # Arrange
        table_name = self._create_table()

        # Act
        tables = list(self.ts.list_tables())

        # Assert
        self.assertIsNotNone(tables)
        self.assertGreaterEqual(len(tables), 1)
        self.assertIsNotNone(tables[0])
        self.assertNamedItemInContainer(tables, table_name)

    @record
    def test_list_tables_with_num_results(self):
        # Arrange
        for i in range(0, 4):
            self._create_table()

        # Act
        tables = list(self.ts.list_tables(num_results=3))

        # Assert
        self.assertEqual(len(tables), 3)

    @record
    def test_list_tables_with_marker(self):
        # Arrange
        prefix = 'listtable'
        table_names = []
        for i in range(0, 4):
            table_names.append(self._create_table(prefix))

        table_names.sort()

        # Act
        generator1 = self.ts.list_tables(num_results=2)
        generator2 = self.ts.list_tables(num_results=2, marker=generator1.next_marker)

        tables1 = generator1.items
        tables2 = generator2.items

        # Assert
        self.assertEqual(len(tables1), 2)
        self.assertEqual(len(tables2), 2)

    @record
    def test_delete_table_with_existing_table(self):
        # Arrange
        table_name = self._create_table()

        # Act
        deleted = self.ts.delete_table(table_name)

        # Assert
        self.assertTrue(deleted)
        self.assertFalse(self.ts.exists(table_name))

    @record
    def test_delete_table_with_existing_table_fail_not_exist(self):
        # Arrange
        table_name = self._create_table()

        # Act
        deleted = self.ts.delete_table(table_name, True)

        # Assert
        self.assertTrue(deleted)
        self.assertFalse(self.ts.exists(table_name))

    @record
    def test_delete_table_with_non_existing_table(self):
        # Arrange
        table_name = self._get_table_reference()

        # Act
        deleted = self.ts.delete_table(table_name)

        # Assert
        self.assertFalse(deleted)

    @record
    def test_delete_table_with_non_existing_table_fail_not_exist(self):
        # Arrange
        table_name = self._get_table_reference()

        # Act
        with self.assertRaises(AzureMissingResourceHttpError):
            self.ts.delete_table(table_name, True)

            # Assert

    @record
    def test_unicode_create_table_unicode_name(self):
        # Arrange
        table_name = u'啊齄丂狛狜'

        # Act
        with self.assertRaises(AzureHttpError):
            # not supported - table name must be alphanumeric, lowercase
            self.ts.create_table(table_name)

            # Assert

    @record
    def test_get_table_acl(self):
        # Arrange
        table_name = self._create_table()

        # Act
        acl = self.ts.get_table_acl(table_name)

        # Assert
        self.assertIsNotNone(acl)
        self.assertEqual(len(acl), 0)

    @record
    def test_set_table_acl(self):
        # Arrange
        table_name = self._create_table()

        # Act
        self.ts.set_table_acl(table_name)

        # Assert
        acl = self.ts.get_table_acl(table_name)
        self.assertIsNotNone(acl)
        self.assertEqual(len(acl), 0)

    @record
    def test_set_table_acl_with_empty_signed_identifiers(self):
        # Arrange
        table_name = self._create_table()

        # Act
        self.ts.set_table_acl(table_name, dict())

        # Assert
        acl = self.ts.get_table_acl(table_name)
        self.assertIsNotNone(acl)
        self.assertEqual(len(acl), 0)

    @record
    def test_set_table_acl_with_empty_signed_identifier(self):
        # Arrange
        table_name = self._create_table()

        # Act
        self.ts.set_table_acl(table_name, {'empty': AccessPolicy()})

        # Assert
        acl = self.ts.get_table_acl(table_name)
        self.assertIsNotNone(acl)
        self.assertEqual(len(acl), 1)
        self.assertIsNotNone(acl['empty'])
        self.assertIsNone(acl['empty'].permission)
        self.assertIsNone(acl['empty'].expiry)
        self.assertIsNone(acl['empty'].start)

    @record
    def test_set_table_acl_with_signed_identifiers(self):
        # Arrange
        table_name = self._create_table()

        # Act
        identifiers = dict()
        identifiers['testid'] = AccessPolicy(start='2011-10-11',
                                             expiry='2011-10-12',
                                             permission=TablePermissions.QUERY)

        self.ts.set_table_acl(table_name, identifiers)

        # Assert
        acl = self.ts.get_table_acl(table_name)
        self.assertIsNotNone(acl)
        self.assertEqual(len(acl), 1)
        self.assertTrue('testid' in acl)

    @record
    def test_set_table_acl_too_many_ids(self):
        # Arrange
        table_name = self._create_table()

        # Act
        identifiers = dict()
        for i in range(0, 6):
            identifiers['id{}'.format(i)] = AccessPolicy()

        # Assert
        with self.assertRaisesRegexp(AzureException,
                                     'Too many access policies provided. The server does not support setting more than 5 access policies on a single resource.'):
            self.ts.set_table_acl(table_name, identifiers)

    @record
    def test_account_sas(self):
        # SAS URL is calculated from storage key, so this test runs live only
        if TestMode.need_recording_file(self.test_mode):
            return

        # Arrange
        table_name = self._create_table()
        entity = {
            'PartitionKey': 'test',
            'RowKey': 'test1',
            'text': 'hello',
        }
        self.ts.insert_entity(table_name, entity)

        entity['RowKey'] = 'test2'
        self.ts.insert_entity(table_name, entity)

        token = self.ts.generate_account_shared_access_signature(
            ResourceTypes.OBJECT,
            AccountPermissions.READ,
            datetime.utcnow() + timedelta(hours=1),
            datetime.utcnow() - timedelta(minutes=1),
        )

        # Act
        service = TableService(
            account_name=self.settings.STORAGE_ACCOUNT_NAME,
            sas_token=token,
        )
        self._set_test_proxy(service, self.settings)
        entities = list(service.query_entities(table_name))

        # Assert
        self.assertEqual(len(entities), 2)
        self.assertEqual(entities[0].text, 'hello')
        self.assertEqual(entities[1].text, 'hello')

    @record
    def test_locale(self):
        # Arrange
        if os.name is "nt":
            culture = "Spanish_Spain"
        elif os.name is 'posix':
            culture = 'es_ES.UTF-8'
        else:
            culture = 'es_ES.utf8'

        locale.setlocale(locale.LC_ALL, culture)
        e = None

        # Act
        try:
            resp = self.ts.list_tables()
        except:
            e = sys.exc_info()[0]

        # Assert
        self.assertIsNone(e)


# ------------------------------------------------------------------------------
if __name__ == '__main__':
    unittest.main()
