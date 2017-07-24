# coding: utf-8

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
from math import isnan

from azure.common import (
    AzureHttpError,
    AzureConflictHttpError,
    AzureMissingResourceHttpError,
    AzureException,
)
from dateutil.tz import tzutc, tzoffset

from azure.storage.common import (
    AccessPolicy,
)
from azure.storage.common._common_conversion import (
    _encode_base64,
)
from azure.storage.table import (
    Entity,
    EntityProperty,
    TableService,
    TablePermissions,
    EdmType,
    TableBatch,
)
from tests.testcase import (
    StorageTestCase,
    TestMode,
    record,
)


# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------

class StorageTableEntityTest(StorageTestCase):
    def setUp(self):
        super(StorageTableEntityTest, self).setUp()

        self.ts = self._create_storage_service(TableService, self.settings)

        self.table_name = self.get_resource_name('uttable')

        if not self.is_playback():
            self.ts.create_table(self.table_name)

        self.query_tables = []

    def tearDown(self):
        if not self.is_playback():
            try:
                self.ts.delete_table(self.table_name)
            except:
                pass

            for table_name in self.query_tables:
                try:
                    self.ts.delete_table(table_name)
                except:
                    pass

        return super(StorageTableEntityTest, self).tearDown()

    # --Helpers-----------------------------------------------------------------

    def _create_query_table(self, entity_count):
        '''
        Creates a table with the specified name and adds entities with the
        default set of values. PartitionKey is set to 'MyPartition' and RowKey
        is set to a unique counter value starting at 1 (as a string).
        '''
        table_name = self.get_resource_name('querytable')
        self.ts.create_table(table_name, True)
        self.query_tables.append(table_name)

        entity = self._create_random_entity_dict()
        with self.ts.batch(table_name) as batch:
            for i in range(1, entity_count + 1):
                entity['RowKey'] = entity['RowKey'] + str(i)
                batch.insert_entity(entity)
        return table_name

    def _create_random_base_entity_class(self):
        '''
        Creates a class-based entity with only pk and rk.
        '''
        partition = self.get_resource_name('pk')
        row = self.get_resource_name('rk')
        entity = Entity()
        entity.PartitionKey = partition
        entity.RowKey = row
        return entity

    def _create_random_base_entity_dict(self):
        '''
        Creates a dict-based entity with only pk and rk.
        '''
        partition = self.get_resource_name('pk')
        row = self.get_resource_name('rk')
        return {'PartitionKey': partition,
                'RowKey': row,
                }

    def _create_random_entity_class(self, pk=None, rk=None):
        '''
        Creates a class-based entity with fixed values, using all
        of the supported data types.
        '''
        partition = pk if pk is not None else self.get_resource_name('pk')
        row = rk if rk is not None else self.get_resource_name('rk')
        entity = Entity()
        entity.PartitionKey = partition
        entity.RowKey = row
        entity.age = 39
        entity.sex = 'male'
        entity.married = True
        entity.deceased = False
        entity.optional = None
        entity.evenratio = 3.0
        entity.ratio = 3.1
        entity.large = 933311100
        entity.Birthday = datetime(1973, 10, 4)
        entity.birthday = datetime(1970, 10, 4)
        entity.binary = EntityProperty(EdmType.BINARY, b'binary')
        entity.other = EntityProperty(EdmType.INT32, 20)
        entity.clsid = EntityProperty(
            EdmType.GUID, 'c9da6455-213d-42c9-9a79-3e9149a57833')
        return entity

    def _create_random_entity_dict(self, pk=None, rk=None):
        '''
        Creates a dictionary-based entity with fixed values, using all
        of the supported data types.
        '''
        partition = pk if pk is not None else self.get_resource_name('pk')
        row = rk if rk is not None else self.get_resource_name('rk')
        return {'PartitionKey': partition,
                'RowKey': row,
                'age': 39,
                'sex': 'male',
                'married': True,
                'deceased': False,
                'optional': None,
                'ratio': 3.1,
                'evenratio': 3.0,
                'large': 933311100,
                'Birthday': datetime(1973, 10, 4),
                'birthday': datetime(1970, 10, 4),
                'binary': EntityProperty(EdmType.BINARY, b'binary'),
                'other': EntityProperty(EdmType.INT32, 20),
                'clsid': EntityProperty(
                    EdmType.GUID,
                    'c9da6455-213d-42c9-9a79-3e9149a57833')}

    def _insert_random_entity(self, pk=None, rk=None):
        entity = self._create_random_entity_class(pk, rk)
        etag = self.ts.insert_entity(self.table_name, entity)
        entity.etag = etag
        return entity

    def _create_updated_entity_dict(self, partition, row):
        '''
        Creates a dictionary-based entity with fixed values, with a
        different set of values than the default entity. It
        adds fields, changes field values, changes field types,
        and removes fields when compared to the default entity.
        '''
        return {'PartitionKey': partition,
                'RowKey': row,
                'age': 'abc',
                'sex': 'female',
                'sign': 'aquarius',
                'birthday': datetime(1991, 10, 4)}

    def _assert_default_entity(self, entity):
        '''
        Asserts that the entity passed in matches the default entity.
        '''
        self.assertEqual(entity.age, 39)
        self.assertEqual(entity.sex, 'male')
        self.assertEqual(entity.married, True)
        self.assertEqual(entity.deceased, False)
        self.assertFalse(hasattr(entity, "optional"))
        self.assertFalse(hasattr(entity, "aquarius"))
        self.assertEqual(entity.ratio, 3.1)
        self.assertEqual(entity.evenratio, 3.0)
        self.assertEqual(entity.large, 933311100)
        self.assertEqual(entity.Birthday, datetime(1973, 10, 4, tzinfo=tzutc()))
        self.assertEqual(entity.birthday, datetime(1970, 10, 4, tzinfo=tzutc()))
        self.assertIsInstance(entity.binary, EntityProperty)
        self.assertEqual(entity.binary.type, EdmType.BINARY)
        self.assertEqual(entity.binary.value, b'binary')
        self.assertIsInstance(entity.other, EntityProperty)
        self.assertEqual(entity.other.type, EdmType.INT32)
        self.assertEqual(entity.other.value, 20)
        self.assertIsInstance(entity.clsid, EntityProperty)
        self.assertEqual(entity.clsid.type, EdmType.GUID)
        self.assertEqual(entity.clsid.value,
                         'c9da6455-213d-42c9-9a79-3e9149a57833')
        self.assertTrue(hasattr(entity, "Timestamp"))
        self.assertIsInstance(entity.Timestamp, datetime)
        self.assertIsNotNone(entity.etag)

    def _assert_default_entity_json_no_metadata(self, entity):
        '''
        Asserts that the entity passed in matches the default entity.
        '''
        self.assertEqual(entity.age, '39')
        self.assertEqual(entity.sex, 'male')
        self.assertEqual(entity.married, True)
        self.assertEqual(entity.deceased, False)
        self.assertFalse(hasattr(entity, "optional"))
        self.assertFalse(hasattr(entity, "aquarius"))
        self.assertEqual(entity.ratio, 3.1)
        self.assertEqual(entity.evenratio, 3.0)
        self.assertEqual(entity.large, '933311100')
        self.assertEqual(entity.Birthday, '1973-10-04T00:00:00Z')
        self.assertEqual(entity.birthday, '1970-10-04T00:00:00Z')
        self.assertEqual(entity.binary, _encode_base64(b'binary'))
        self.assertIsInstance(entity.other, EntityProperty)
        self.assertEqual(entity.other.type, EdmType.INT32)
        self.assertEqual(entity.other.value, 20)
        self.assertEqual(entity.clsid, 'c9da6455-213d-42c9-9a79-3e9149a57833')
        self.assertTrue(hasattr(entity, "Timestamp"))
        self.assertIsInstance(entity.Timestamp, datetime)
        self.assertIsNotNone(entity.etag)

    def _assert_updated_entity(self, entity):
        '''
        Asserts that the entity passed in matches the updated entity.
        '''
        self.assertEqual(entity.age, 'abc')
        self.assertEqual(entity.sex, 'female')
        self.assertFalse(hasattr(entity, "married"))
        self.assertFalse(hasattr(entity, "deceased"))
        self.assertEqual(entity.sign, 'aquarius')
        self.assertFalse(hasattr(entity, "optional"))
        self.assertFalse(hasattr(entity, "ratio"))
        self.assertFalse(hasattr(entity, "evenratio"))
        self.assertFalse(hasattr(entity, "large"))
        self.assertFalse(hasattr(entity, "Birthday"))
        self.assertEqual(entity.birthday, datetime(1991, 10, 4, tzinfo=tzutc()))
        self.assertFalse(hasattr(entity, "other"))
        self.assertFalse(hasattr(entity, "clsid"))
        self.assertTrue(hasattr(entity, "Timestamp"))
        self.assertIsNotNone(entity.etag)

    def _assert_merged_entity(self, entity):
        '''
        Asserts that the entity passed in matches the default entity
        merged with the updated entity.
        '''
        self.assertEqual(entity.age, 'abc')
        self.assertEqual(entity.sex, 'female')
        self.assertEqual(entity.sign, 'aquarius')
        self.assertEqual(entity.married, True)
        self.assertEqual(entity.deceased, False)
        self.assertEqual(entity.sign, 'aquarius')
        self.assertEqual(entity.ratio, 3.1)
        self.assertEqual(entity.evenratio, 3.0)
        self.assertEqual(entity.large, 933311100)
        self.assertEqual(entity.Birthday, datetime(1973, 10, 4, tzinfo=tzutc()))
        self.assertEqual(entity.birthday, datetime(1991, 10, 4, tzinfo=tzutc()))
        self.assertIsInstance(entity.other, EntityProperty)
        self.assertEqual(entity.other.type, EdmType.INT32)
        self.assertEqual(entity.other.value, 20)
        self.assertIsInstance(entity.clsid, EntityProperty)
        self.assertEqual(entity.clsid.type, EdmType.GUID)
        self.assertEqual(entity.clsid.value,
                         'c9da6455-213d-42c9-9a79-3e9149a57833')
        self.assertTrue(hasattr(entity, "Timestamp"))
        self.assertIsNotNone(entity.etag)

    def _resolver_with_assert(self, pk, rk, name, value, type):
        self.assertIsNotNone(pk)
        self.assertIsNotNone(rk)
        self.assertIsNotNone(name)
        self.assertIsNotNone(value)
        self.assertIsNone(type)
        if name == 'large' or name == 'age':
            return EdmType.INT64
        if name == 'Birthday' or name == 'birthday':
            return EdmType.DATETIME
        if name == 'clsid':
            return EdmType.GUID
        if name == 'binary':
            return EdmType.BINARY

    # --Test cases for entities ------------------------------------------
    @record
    def test_insert_entity_dictionary(self):
        # Arrange
        dict = self._create_random_entity_dict()

        # Act      
        resp = self.ts.insert_entity(self.table_name, dict)

        # Assert
        self.assertIsNotNone(resp)

    @record
    def test_insert_entity_class_instance(self):
        # Arrange
        entity = self._create_random_entity_class()

        # Act       
        resp = self.ts.insert_entity(self.table_name, entity)

        # Assert
        self.assertIsNotNone(resp)

    @record
    def test_insert_entity_conflict(self):
        # Arrange
        entity = self._insert_random_entity()

        # Act
        with self.assertRaises(AzureConflictHttpError):
            self.ts.insert_entity(self.table_name, entity)

            # Assert

    @record
    def test_insert_entity_with_large_int32_value_throws(self):
        # Arrange

        # Act
        dict32 = self._create_random_base_entity_dict()
        dict32['large'] = EntityProperty(EdmType.INT32, 2 ** 31)

        # Assert
        with self.assertRaisesRegexp(TypeError,
                                     '{0} is too large to be cast to type Edm.Int32.'.format(2 ** 31)):
            self.ts.insert_entity(self.table_name, dict32)

        dict32['large'] = EntityProperty(EdmType.INT32, -(2 ** 31 + 1))
        with self.assertRaisesRegexp(TypeError,
                                     '{0} is too large to be cast to type Edm.Int32.'.format(-(2 ** 31 + 1))):
            self.ts.insert_entity(self.table_name, dict32)

    @record
    def test_insert_entity_with_large_int64_value_throws(self):
        # Arrange

        # Act
        dict64 = self._create_random_base_entity_dict()
        dict64['large'] = EntityProperty(EdmType.INT64, 2 ** 63)

        # Assert
        with self.assertRaisesRegexp(TypeError,
                                     '{0} is too large to be cast to type Edm.Int64.'.format(2 ** 63)):
            self.ts.insert_entity(self.table_name, dict64)

        dict64['large'] = EntityProperty(EdmType.INT64, -(2 ** 63 + 1))
        with self.assertRaisesRegexp(TypeError,
                                     '{0} is too large to be cast to type Edm.Int64.'.format(-(2 ** 63 + 1))):
            self.ts.insert_entity(self.table_name, dict64)

    def test_insert_entity_missing_pk(self):
        # Arrange
        entity = {'RowKey': 'rk'}

        # Act
        with self.assertRaises(ValueError):
            resp = self.ts.insert_entity(self.table_name, entity)

            # Assert

    @record
    def test_insert_entity_missing_rk(self):
        # Arrange
        entity = {'PartitionKey': 'pk'}

        # Act
        with self.assertRaises(ValueError):
            resp = self.ts.insert_entity(self.table_name, entity)

            # Assert

    @record
    def test_insert_entity_too_many_properties(self):
        # Arrange
        entity = self._create_random_base_entity_dict()
        for i in range(255):
            entity['key{0}'.format(i)] = 'value{0}'.format(i)

        # Act
        with self.assertRaises(ValueError):
            resp = self.ts.insert_entity(self.table_name, entity)

            # Assert

    @record
    def test_insert_entity_property_name_too_long(self):
        # Arrange
        entity = self._create_random_base_entity_dict()
        str = 'a' * 256
        entity[str] = 'badval'

        # Act
        with self.assertRaises(ValueError):
            resp = self.ts.insert_entity(self.table_name, entity)

            # Assert

    @record
    def test_get_entity(self):
        # Arrange
        entity = self._insert_random_entity()

        # Act
        resp = self.ts.get_entity(self.table_name, entity.PartitionKey, entity.RowKey)

        # Assert
        self.assertEqual(resp.PartitionKey, entity.PartitionKey)
        self.assertEqual(resp.RowKey, entity.RowKey)
        self._assert_default_entity(resp)

    @record
    def test_get_entity_if_match(self):
        # Arrange
        entity = self._insert_random_entity()

        # Act
        # Do a get and confirm the etag is parsed correctly by using it
        # as a condition to delete.
        resp = self.ts.get_entity(self.table_name, entity.PartitionKey, entity.RowKey)
        self.ts.delete_entity(self.table_name, resp.PartitionKey, resp.RowKey, if_match=resp.etag)

        # Assert

    @record
    def test_get_entity_full_metadata(self):
        # Arrange
        entity = self._insert_random_entity()

        # Act
        resp = self.ts.get_entity(self.table_name, entity.PartitionKey, entity.RowKey,
                                  accept='application/json;odata=fullmetadata')

        # Assert
        self.assertEqual(resp.PartitionKey, entity.PartitionKey)
        self.assertEqual(resp.RowKey, entity.RowKey)
        self._assert_default_entity(resp)

    @record
    def test_get_entity_no_metadata(self):
        # Arrange
        entity = self._insert_random_entity()

        # Act
        resp = self.ts.get_entity(self.table_name, entity.PartitionKey, entity.RowKey,
                                  accept='application/json;odata=nometadata')

        # Assert
        self.assertEqual(resp.PartitionKey, entity.PartitionKey)
        self.assertEqual(resp.RowKey, entity.RowKey)
        self._assert_default_entity_json_no_metadata(resp)

    @record
    def test_get_entity_with_property_resolver(self):
        # Arrange
        entity = self._insert_random_entity()

        # Act
        resp = self.ts.get_entity(self.table_name, entity.PartitionKey, entity.RowKey,
                                  accept='application/json;odata=nometadata',
                                  property_resolver=self._resolver_with_assert)

        # Assert
        self.assertEqual(resp.PartitionKey, entity.PartitionKey)
        self.assertEqual(resp.RowKey, entity.RowKey)
        self._assert_default_entity(resp)

    @record
    def test_get_entity_with_property_resolver_not_supported(self):
        # Arrange
        entity = self._insert_random_entity()

        # Act
        with self.assertRaisesRegexp(AzureException,
                                     'Type not supported when sending data to the service:'):
            self.ts.get_entity(self.table_name, entity.PartitionKey, entity.RowKey,
                               property_resolver=lambda pk, rk, name, val, type: 'badType')

            # Assert

    @record
    def test_get_entity_with_property_resolver_invalid(self):
        # Arrange
        entity = self._insert_random_entity()

        # Act
        with self.assertRaisesRegexp(AzureException,
                                     'The specified property resolver returned an invalid type.'):
            self.ts.get_entity(self.table_name, entity.PartitionKey, entity.RowKey,
                               property_resolver=lambda pk, rk, name, val, type: EdmType.INT64)

            # Assert

    @record
    def test_get_entity_not_existing(self):
        # Arrange
        entity = self._create_random_entity_class()

        # Act
        with self.assertRaises(AzureMissingResourceHttpError):
            self.ts.get_entity(self.table_name, entity.PartitionKey, entity.RowKey, )

            # Assert

    @record
    def test_get_entity_with_select(self):
        # Arrange
        entity = self._insert_random_entity()

        # Act
        resp = self.ts.get_entity(self.table_name, entity.PartitionKey, entity.RowKey, 'age,sex,xyz')

        # Assert
        self.assertEqual(resp.age, 39)
        self.assertEqual(resp.sex, 'male')
        self.assertEqual(resp.xyz, None)
        self.assertFalse(hasattr(resp, "birthday"))
        self.assertFalse(hasattr(resp, "married"))
        self.assertFalse(hasattr(resp, "deceased"))

    @record
    def test_get_entity_with_special_doubles(self):
        # Arrange
        entity = self._create_random_base_entity_dict()
        entity.update({
            'inf': float('inf'),
            'negativeinf': float('-inf'),
            'nan': float('nan')
        })
        self.ts.insert_entity(self.table_name, entity)

        # Act
        resp = self.ts.get_entity(self.table_name, entity['PartitionKey'], entity['RowKey'])

        # Assert
        self.assertEqual(resp.inf, float('inf'))
        self.assertEqual(resp.negativeinf, float('-inf'))
        self.assertTrue(isnan(resp.nan))

    @record
    def test_update_entity(self):
        # Arrange
        entity = self._insert_random_entity()

        # Act
        sent_entity = self._create_updated_entity_dict(entity.PartitionKey, entity.RowKey)
        resp = self.ts.update_entity(self.table_name, sent_entity)

        # Assert
        self.assertIsNotNone(resp)
        received_entity = self.ts.get_entity(self.table_name, entity.PartitionKey, entity.RowKey)
        self._assert_updated_entity(received_entity)

    @record
    def test_update_entity_with_if_matches(self):
        # Arrange
        entity = self._insert_random_entity()

        # Act
        sent_entity = self._create_updated_entity_dict(entity.PartitionKey, entity.RowKey)
        resp = self.ts.update_entity(self.table_name, sent_entity, if_match=entity.etag)

        # Assert
        self.assertIsNotNone(resp)
        received_entity = self.ts.get_entity(self.table_name, entity.PartitionKey, entity.RowKey)
        self._assert_updated_entity(received_entity)

    @record
    def test_update_entity_with_if_doesnt_match(self):
        # Arrange
        entity = self._insert_random_entity()

        # Act
        sent_entity = self._create_updated_entity_dict(entity.PartitionKey, entity.RowKey)
        with self.assertRaises(AzureHttpError):
            self.ts.update_entity(self.table_name, sent_entity,
                                  if_match=u'W/"datetime\'2012-06-15T22%3A51%3A44.9662825Z\'"')

            # Assert

    @record
    def test_insert_or_merge_entity_with_existing_entity(self):
        # Arrange
        entity = self._insert_random_entity()

        # Act
        sent_entity = self._create_updated_entity_dict(entity.PartitionKey, entity.RowKey)
        resp = self.ts.insert_or_merge_entity(self.table_name, sent_entity)

        # Assert
        self.assertIsNotNone(resp)
        received_entity = self.ts.get_entity(self.table_name, entity.PartitionKey, entity.RowKey)
        self._assert_merged_entity(received_entity)

    @record
    def test_insert_or_merge_entity_with_non_existing_entity(self):
        # Arrange
        entity = self._create_random_base_entity_class()

        # Act
        sent_entity = self._create_updated_entity_dict(entity.PartitionKey, entity.RowKey)
        resp = self.ts.insert_or_merge_entity(self.table_name, sent_entity)

        # Assert
        self.assertIsNotNone(resp)
        received_entity = self.ts.get_entity(self.table_name, entity.PartitionKey, entity.RowKey)
        self._assert_updated_entity(received_entity)

    @record
    def test_insert_or_replace_entity_with_existing_entity(self):
        # Arrange
        entity = self._insert_random_entity()

        # Act
        sent_entity = self._create_updated_entity_dict(entity.PartitionKey, entity.RowKey)
        resp = self.ts.insert_or_replace_entity(self.table_name, sent_entity)

        # Assert
        self.assertIsNotNone(resp)
        received_entity = self.ts.get_entity(self.table_name, entity.PartitionKey, entity.RowKey)
        self._assert_updated_entity(received_entity)

    @record
    def test_insert_or_replace_entity_with_non_existing_entity(self):
        # Arrange
        entity = self._create_random_base_entity_class()

        # Act
        sent_entity = self._create_updated_entity_dict(entity.PartitionKey, entity.RowKey)
        resp = self.ts.insert_or_replace_entity(self.table_name, sent_entity)

        # Assert
        self.assertIsNotNone(resp)
        received_entity = self.ts.get_entity(self.table_name, entity.PartitionKey, entity.RowKey)
        self._assert_updated_entity(received_entity)

    @record
    def test_merge_entity(self):
        # Arrange
        entity = self._insert_random_entity()

        # Act
        sent_entity = self._create_updated_entity_dict(entity.PartitionKey, entity.RowKey)
        resp = self.ts.merge_entity(self.table_name, sent_entity)

        # Assert
        self.assertIsNotNone(resp)
        received_entity = self.ts.get_entity(self.table_name, entity.PartitionKey, entity.RowKey)
        self._assert_merged_entity(received_entity)

    @record
    def test_merge_entity_not_existing(self):
        # Arrange
        entity = self._create_random_base_entity_class()

        # Act
        sent_entity = self._create_updated_entity_dict(entity.PartitionKey, entity.RowKey)
        with self.assertRaises(AzureHttpError):
            self.ts.merge_entity(self.table_name, sent_entity)

            # Assert

    @record
    def test_merge_entity_with_if_matches(self):
        # Arrange
        entity = self._insert_random_entity()

        # Act
        sent_entity = self._create_updated_entity_dict(entity.PartitionKey, entity.RowKey)
        resp = self.ts.merge_entity(self.table_name, sent_entity, entity.etag)

        # Assert
        self.assertIsNotNone(resp)
        received_entity = self.ts.get_entity(self.table_name, entity.PartitionKey, entity.RowKey)
        self._assert_merged_entity(received_entity)

    @record
    def test_merge_entity_with_if_doesnt_match(self):
        # Arrange
        entity = self._insert_random_entity()

        # Act
        sent_entity = self._create_updated_entity_dict(entity.PartitionKey, entity.RowKey)
        with self.assertRaises(AzureHttpError):
            self.ts.merge_entity(self.table_name, sent_entity,
                                 if_match=u'W/"datetime\'2012-06-15T22%3A51%3A44.9662825Z\'"')

            # Assert

    @record
    def test_delete_entity(self):
        # Arrange
        entity = self._insert_random_entity()

        # Act
        resp = self.ts.delete_entity(self.table_name, entity.PartitionKey, entity.RowKey)

        # Assert
        self.assertIsNone(resp)
        with self.assertRaises(AzureHttpError):
            self.ts.get_entity(self.table_name, entity.PartitionKey, entity.RowKey)

    @record
    def test_delete_entity_not_existing(self):
        # Arrange
        entity = self._create_random_base_entity_class()

        # Act
        with self.assertRaises(AzureHttpError):
            self.ts.delete_entity(self.table_name, entity.PartitionKey, entity.RowKey)

            # Assert

    @record
    def test_delete_entity_with_if_matches(self):
        # Arrange
        entity = self._insert_random_entity()

        # Act
        resp = self.ts.delete_entity(self.table_name, entity.PartitionKey, entity.RowKey, if_match=entity.etag)

        # Assert
        self.assertIsNone(resp)
        with self.assertRaises(AzureHttpError):
            self.ts.get_entity(self.table_name, entity.PartitionKey, entity.RowKey)

    @record
    def test_delete_entity_with_if_doesnt_match(self):
        # Arrange
        entity = self._insert_random_entity()

        # Act
        with self.assertRaises(AzureHttpError):
            self.ts.delete_entity(self.table_name, entity.PartitionKey, entity.RowKey,
                                  if_match=u'W/"datetime\'2012-06-15T22%3A51%3A44.9662825Z\'"')

            # Assert

    @record
    def test_unicode_property_value(self):
        ''' regression test for github issue #57'''
        # Arrange
        entity = self._create_random_base_entity_dict()
        entity1 = entity.copy()
        entity1.update({'Description': u'ꀕ'})
        entity2 = entity.copy()
        entity2.update({'RowKey': 'test2', 'Description': 'ꀕ'})

        # Act
        self.ts.insert_entity(self.table_name, entity1)
        self.ts.insert_entity(self.table_name, entity2)
        entities = list(self.ts.query_entities(self.table_name, "PartitionKey eq '{}'".format(entity['PartitionKey'])))

        # Assert
        self.assertEqual(len(entities), 2)
        self.assertEqual(entities[0].Description, u'ꀕ')
        self.assertEqual(entities[1].Description, u'ꀕ')

    @record
    def test_unicode_property_name(self):
        # Arrange
        entity = self._create_random_base_entity_dict()
        entity1 = entity.copy()
        entity1.update({u'啊齄丂狛狜': u'ꀕ'})
        entity2 = entity.copy()
        entity2.update({'RowKey': 'test2', u'啊齄丂狛狜': 'hello'})

        # Act  
        self.ts.insert_entity(self.table_name, entity1)
        self.ts.insert_entity(self.table_name, entity2)
        entities = list(self.ts.query_entities(self.table_name, "PartitionKey eq '{}'".format(entity['PartitionKey'])))

        # Assert
        self.assertEqual(len(entities), 2)
        self.assertEqual(entities[0][u'啊齄丂狛狜'], u'ꀕ')
        self.assertEqual(entities[1][u'啊齄丂狛狜'], u'hello')

    @record
    def test_empty_and_spaces_property_value(self):
        # Arrange
        entity = self._create_random_base_entity_dict()
        entity.update({
            'EmptyByte': '',
            'EmptyUnicode': u'',
            'SpacesOnlyByte': '   ',
            'SpacesOnlyUnicode': u'   ',
            'SpacesBeforeByte': '   Text',
            'SpacesBeforeUnicode': u'   Text',
            'SpacesAfterByte': 'Text   ',
            'SpacesAfterUnicode': u'Text   ',
            'SpacesBeforeAndAfterByte': '   Text   ',
            'SpacesBeforeAndAfterUnicode': u'   Text   ',
        })

        # Act
        self.ts.insert_entity(self.table_name, entity)
        resp = self.ts.get_entity(self.table_name, entity['PartitionKey'], entity['RowKey'])

        # Assert
        self.assertIsNotNone(resp)
        self.assertEqual(resp.EmptyByte, '')
        self.assertEqual(resp.EmptyUnicode, u'')
        self.assertEqual(resp.SpacesOnlyByte, '   ')
        self.assertEqual(resp.SpacesOnlyUnicode, u'   ')
        self.assertEqual(resp.SpacesBeforeByte, '   Text')
        self.assertEqual(resp.SpacesBeforeUnicode, u'   Text')
        self.assertEqual(resp.SpacesAfterByte, 'Text   ')
        self.assertEqual(resp.SpacesAfterUnicode, u'Text   ')
        self.assertEqual(resp.SpacesBeforeAndAfterByte, '   Text   ')
        self.assertEqual(resp.SpacesBeforeAndAfterUnicode, u'   Text   ')

    @record
    def test_none_property_value(self):
        # Arrange
        entity = self._create_random_base_entity_dict()
        entity.update({'NoneValue': None})

        # Act       
        self.ts.insert_entity(self.table_name, entity)
        resp = self.ts.get_entity(self.table_name, entity['PartitionKey'], entity['RowKey'])

        # Assert
        self.assertIsNotNone(resp)
        self.assertFalse(hasattr(resp, 'NoneValue'))

    @record
    def test_binary_property_value(self):
        # Arrange
        binary_data = b'\x01\x02\x03\x04\x05\x06\x07\x08\t\n'
        entity = self._create_random_base_entity_dict()
        entity.update({'binary': EntityProperty(EdmType.BINARY, binary_data)})

        # Act  
        self.ts.insert_entity(self.table_name, entity)
        resp = self.ts.get_entity(self.table_name, entity['PartitionKey'], entity['RowKey'])

        # Assert
        self.assertIsNotNone(resp)
        self.assertEqual(resp.binary.type, EdmType.BINARY)
        self.assertEqual(resp.binary.value, binary_data)

    @record
    def test_timezone(self):
        # Arrange
        local_tz = tzoffset('BRST', -10800)
        local_date = datetime(2003, 9, 27, 9, 52, 43, tzinfo=local_tz)
        entity = self._create_random_base_entity_dict()
        entity.update({'date': local_date})

        # Act
        self.ts.insert_entity(self.table_name, entity)
        resp = self.ts.get_entity(self.table_name, entity['PartitionKey'], entity['RowKey'])

        # Assert
        self.assertIsNotNone(resp)
        self.assertEqual(resp.date, local_date.astimezone(tzutc()))
        self.assertEqual(resp.date.astimezone(local_tz), local_date)

    @record
    def test_query_entities(self):
        # Arrange
        table_name = self._create_query_table(2)

        # Act
        entities = list(self.ts.query_entities(table_name))

        # Assert
        self.assertEqual(len(entities), 2)
        for entity in entities:
            self._assert_default_entity(entity)

    @record
    def test_query_zero_entities(self):
        # Arrange
        table_name = self._create_query_table(0)

        # Act
        entities = list(self.ts.query_entities(table_name))

        # Assert
        self.assertEqual(len(entities), 0)

    @record
    def test_query_entities_full_metadata(self):
        # Arrange
        table_name = self._create_query_table(2)

        # Act
        entities = list(self.ts.query_entities(table_name,
                                               accept='application/json;odata=fullmetadata'))

        # Assert
        self.assertEqual(len(entities), 2)
        for entity in entities:
            self._assert_default_entity(entity)

    @record
    def test_query_entities_no_metadata(self):
        # Arrange
        table_name = self._create_query_table(2)

        # Act
        entities = list(self.ts.query_entities(table_name,
                                               accept='application/json;odata=nometadata'))

        # Assert
        self.assertEqual(len(entities), 2)
        for entity in entities:
            self._assert_default_entity_json_no_metadata(entity)

    @record
    def test_query_entities_with_property_resolver(self):
        # Arrange
        table_name = self._create_query_table(2)

        # Act
        entities = list(self.ts.query_entities(table_name,
                                               accept='application/json;odata=nometadata',
                                               property_resolver=self._resolver_with_assert))

        # Assert
        self.assertEqual(len(entities), 2)
        for entity in entities:
            self._assert_default_entity(entity)

    @record
    def test_query_entities_large(self):
        # Arrange
        table_name = self._create_query_table(0)
        total_entities_count = 1000
        entities_per_batch = 50

        for j in range(total_entities_count // entities_per_batch):
            batch = TableBatch()
            for i in range(entities_per_batch):
                entity = Entity()
                entity.PartitionKey = 'large'
                entity.RowKey = 'batch{0}-item{1}'.format(j, i)
                entity.test = EntityProperty(EdmType.BOOLEAN, 'true')
                entity.test2 = 'hello world;' * 100
                entity.test3 = 3
                entity.test4 = EntityProperty(EdmType.INT64, '1234567890')
                entity.test5 = datetime(2016, 12, 31, 11, 59, 59, 0)
                batch.insert_entity(entity)
            self.ts.commit_batch(table_name, batch)

        # Act
        start_time = datetime.now()
        entities = list(self.ts.query_entities(table_name))
        elapsed_time = datetime.now() - start_time

        # Assert
        print('query_entities took {0} secs.'.format(elapsed_time.total_seconds()))
        # azure allocates 5 seconds to execute a query
        # if it runs slowly, it will return fewer results and make the test fail
        self.assertEqual(len(entities), total_entities_count)

    @record
    def test_query_entities_with_filter(self):
        # Arrange
        entity = self._insert_random_entity()

        # Act
        entities = list(
            self.ts.query_entities(self.table_name, filter="PartitionKey eq '{}'".format(entity.PartitionKey)))

        # Assert
        self.assertEqual(len(entities), 1)
        self.assertEqual(entity.PartitionKey, entities[0].PartitionKey)
        self._assert_default_entity(entities[0])

    @record
    def test_query_entities_with_select(self):
        # Arrange
        table_name = self._create_query_table(2)

        # Act
        entities = list(self.ts.query_entities(table_name, select='age,sex'))

        # Assert
        self.assertEqual(len(entities), 2)
        self.assertEqual(entities[0].age, 39)
        self.assertEqual(entities[0].sex, 'male')
        self.assertFalse(hasattr(entities[0], "birthday"))
        self.assertFalse(hasattr(entities[0], "married"))
        self.assertFalse(hasattr(entities[0], "deceased"))

    @record
    def test_query_entities_with_top(self):
        # Arrange
        table_name = self._create_query_table(3)

        # Act
        entities = list(self.ts.query_entities(table_name, num_results=2))

        # Assert
        self.assertEqual(len(entities), 2)

    @record
    def test_query_entities_with_top_and_next(self):
        # Arrange
        table_name = self._create_query_table(5)

        # Act
        resp1 = self.ts.query_entities(table_name, num_results=2)
        resp2 = self.ts.query_entities(table_name, num_results=2, marker=resp1.next_marker)
        resp3 = self.ts.query_entities(table_name, num_results=2, marker=resp2.next_marker)

        entities1 = resp1.items
        entities2 = resp2.items
        entities3 = resp3.items

        # Assert
        self.assertEqual(len(entities1), 2)
        self.assertEqual(len(entities2), 2)
        self.assertEqual(len(entities3), 1)
        self._assert_default_entity(entities1[0])
        self._assert_default_entity(entities1[1])
        self._assert_default_entity(entities2[0])
        self._assert_default_entity(entities2[1])
        self._assert_default_entity(entities3[0])

    @record
    def test_sas_query(self):
        # SAS URL is calculated from storage key, so this test runs live only
        if TestMode.need_recording_file(self.test_mode):
            return

        # Arrange
        entity = self._insert_random_entity()
        token = self.ts.generate_table_shared_access_signature(
            self.table_name,
            TablePermissions.QUERY,
            datetime.utcnow() + timedelta(hours=1),
            datetime.utcnow() - timedelta(minutes=1),
        )

        # Act
        service = TableService(
            account_name=self.settings.STORAGE_ACCOUNT_NAME,
            sas_token=token,
        )
        self._set_test_proxy(service, self.settings)
        entities = list(service.query_entities(self.table_name,
                                               filter="PartitionKey eq '{}'".format(entity['PartitionKey'])))

        # Assert
        self.assertEqual(len(entities), 1)
        self._assert_default_entity(entities[0])

    @record
    def test_sas_add(self):
        # SAS URL is calculated from storage key, so this test runs live only
        if TestMode.need_recording_file(self.test_mode):
            return

        # Arrange
        token = self.ts.generate_table_shared_access_signature(
            self.table_name,
            TablePermissions.ADD,
            datetime.utcnow() + timedelta(hours=1),
            datetime.utcnow() - timedelta(minutes=1),
        )

        # Act
        service = TableService(
            account_name=self.settings.STORAGE_ACCOUNT_NAME,
            sas_token=token,
        )
        self._set_test_proxy(service, self.settings)

        entity = self._create_random_entity_dict()
        service.insert_entity(self.table_name, entity)

        # Assert
        resp = self.ts.get_entity(self.table_name, entity['PartitionKey'], entity['RowKey'])
        self._assert_default_entity(resp)

    @record
    def test_sas_add_inside_range(self):
        # SAS URL is calculated from storage key, so this test runs live only
        if TestMode.need_recording_file(self.test_mode):
            return

        # Arrange

        token = self.ts.generate_table_shared_access_signature(
            self.table_name,
            TablePermissions.ADD,
            datetime.utcnow() + timedelta(hours=1),
            start_pk='test', start_rk='test1',
            end_pk='test', end_rk='test1',
        )

        # Act
        service = TableService(
            account_name=self.settings.STORAGE_ACCOUNT_NAME,
            sas_token=token,
        )
        self._set_test_proxy(service, self.settings)
        entity = self._create_random_entity_dict('test', 'test1')
        service.insert_entity(self.table_name, entity)

        # Assert
        resp = self.ts.get_entity(self.table_name, 'test', 'test1')
        self._assert_default_entity(resp)

    @record
    def test_sas_add_outside_range(self):
        # SAS URL is calculated from storage key, so this test runs live only
        if TestMode.need_recording_file(self.test_mode):
            return

        # Arrange

        token = self.ts.generate_table_shared_access_signature(
            self.table_name,
            TablePermissions.ADD,
            datetime.utcnow() + timedelta(hours=1),
            start_pk='test', start_rk='test1',
            end_pk='test', end_rk='test1',
        )

        # Act
        service = TableService(
            account_name=self.settings.STORAGE_ACCOUNT_NAME,
            sas_token=token,
        )
        self._set_test_proxy(service, self.settings)
        with self.assertRaises(AzureHttpError):
            entity = self._create_random_entity_dict()
            service.insert_entity(self.table_name, entity)

            # Assert

    @record
    def test_sas_update(self):
        # SAS URL is calculated from storage key, so this test runs live only
        if TestMode.need_recording_file(self.test_mode):
            return

        # Arrange
        entity = self._insert_random_entity()
        token = self.ts.generate_table_shared_access_signature(
            self.table_name,
            TablePermissions.UPDATE,
            datetime.utcnow() + timedelta(hours=1),
        )

        # Act
        service = TableService(
            account_name=self.settings.STORAGE_ACCOUNT_NAME,
            sas_token=token,
        )
        self._set_test_proxy(service, self.settings)
        updated_entity = self._create_updated_entity_dict(entity.PartitionKey, entity.RowKey)
        resp = service.update_entity(self.table_name, updated_entity)

        # Assert
        received_entity = self.ts.get_entity(self.table_name, entity.PartitionKey, entity.RowKey)
        self._assert_updated_entity(received_entity)

    @record
    def test_sas_delete(self):
        # SAS URL is calculated from storage key, so this test runs live only
        if TestMode.need_recording_file(self.test_mode):
            return

        # Arrange
        entity = self._insert_random_entity()
        token = self.ts.generate_table_shared_access_signature(
            self.table_name,
            TablePermissions.DELETE,
            datetime.utcnow() + timedelta(hours=1),
        )

        # Act
        service = TableService(
            account_name=self.settings.STORAGE_ACCOUNT_NAME,
            sas_token=token,
        )
        self._set_test_proxy(service, self.settings)
        service.delete_entity(self.table_name, entity.PartitionKey, entity.RowKey)

        # Assert
        with self.assertRaises(AzureMissingResourceHttpError):
            self.ts.get_entity(self.table_name, entity.PartitionKey, entity.RowKey)

    @record
    def test_sas_upper_case_table_name(self):
        # SAS URL is calculated from storage key, so this test runs live only
        if TestMode.need_recording_file(self.test_mode):
            return

        # Arrange
        entity = self._insert_random_entity()

        # Table names are case insensitive, so simply upper case our existing table name to test
        token = self.ts.generate_table_shared_access_signature(
            self.table_name.upper(),
            TablePermissions.QUERY,
            datetime.utcnow() + timedelta(hours=1),
            datetime.utcnow() - timedelta(minutes=1),
        )

        # Act
        service = TableService(
            account_name=self.settings.STORAGE_ACCOUNT_NAME,
            sas_token=token,
        )
        self._set_test_proxy(service, self.settings)
        entities = list(service.query_entities(self.table_name,
                                               filter="PartitionKey eq '{}'".format(entity['PartitionKey'])))

        # Assert
        self.assertEqual(len(entities), 1)
        self._assert_default_entity(entities[0])

    @record
    def test_sas_signed_identifier(self):
        # SAS URL is calculated from storage key, so this test runs live only
        if TestMode.need_recording_file(self.test_mode):
            return

        # Arrange
        entity = self._insert_random_entity()

        access_policy = AccessPolicy()
        access_policy.start = '2011-10-11'
        access_policy.expiry = '2018-10-12'
        access_policy.permission = TablePermissions.QUERY
        identifiers = {'testid': access_policy}

        entities = self.ts.set_table_acl(self.table_name, identifiers)

        token = self.ts.generate_table_shared_access_signature(
            self.table_name,
            id='testid',
        )

        # Act
        service = TableService(
            account_name=self.settings.STORAGE_ACCOUNT_NAME,
            sas_token=token,
        )
        self._set_test_proxy(service, self.settings)
        entities = list(
            self.ts.query_entities(self.table_name, filter="PartitionKey eq '{}'".format(entity.PartitionKey)))

        # Assert
        self.assertEqual(len(entities), 1)
        self._assert_default_entity(entities[0])


# ------------------------------------------------------------------------------
if __name__ == '__main__':
    unittest.main()
