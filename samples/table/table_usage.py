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
import time
import uuid
from datetime import datetime

from azure.common import (
    AzureHttpError,
    AzureConflictHttpError,
    AzureMissingResourceHttpError,
)

from azure.storage.common import (
    Logging,
    Metrics,
    CorsRule,
)
from azure.storage.table import (
    Entity,
    TableBatch,
    EdmType,
    EntityProperty,
    TablePayloadFormat,
)


class TableSamples():
    def __init__(self, account):
        self.account = account

    def run_all_samples(self):
        self.service = self.account.create_table_service()

        self.create_table()
        self.delete_table()
        self.exists()
        self.query_entities()
        self.batch()

        self.create_entity_class()
        self.create_entity_dict()
        self.insert_entity()
        self.get_entity()
        self.update_entity()
        self.merge_entity()
        self.insert_or_merge_entity()
        self.insert_or_replace_entity()
        self.delete_entity()

        self.list_tables()

        # This method contains sleeps, so don't run by default
        # self.service_properties()

    def _get_table_reference(self, prefix='table'):
        table_name = '{}{}'.format(prefix, str(uuid.uuid4()).replace('-', ''))
        return table_name

    def _create_table(self, prefix='table'):
        table_name = self._get_table_reference(prefix)
        self.service.create_table(table_name)
        return table_name

    def create_table(self):
        # Basic
        table_name1 = self._get_table_reference()
        created = self.service.create_table(table_name1)  # True

        # Fail on exist
        table_name2 = self._get_table_reference()
        created = self.service.create_table(table_name2)  # True
        created = self.service.create_table(table_name2)  # False
        try:
            self.service.create_table(table_name2, fail_on_exist=True)
        except AzureConflictHttpError:
            pass

        self.service.delete_table(table_name1)
        self.service.delete_table(table_name2)

    def delete_table(self):
        # Basic
        table_name = self._create_table()
        deleted = self.service.delete_table(table_name)  # True

        # Fail not exist
        table_name = self._get_table_reference()
        deleted = self.service.delete_table(table_name)  # False
        try:
            self.service.delete_table(table_name, fail_not_exist=True)
        except AzureMissingResourceHttpError:
            pass

    def exists(self):
        table_name = self._get_table_reference()

        # Does not exist
        exists = self.service.exists(table_name)  # False

        # Exists
        self.service.create_table(table_name)
        exists = self.service.exists(table_name)  # True

        self.service.delete_table(table_name)

    def query_entities(self):
        table_name = self._create_table()

        entities = []
        for i in range(1, 5):
            entity = {'PartitionKey': 'John',
                      'RowKey': 'Doe the {}'.format(i),
                      'deceased': False,
                      'birthday': datetime(1991, 10, i)}
            self.service.insert_entity(table_name, entity)
            entities.append(entity)

        # Basic
        # Can access properties as dict or like an object
        queried_entities = list(self.service.query_entities(table_name))
        for entity in queried_entities:
            print(entity.RowKey)  # All 4 John Doe characters

        # Num results
        queried_entities = list(self.service.query_entities(table_name, num_results=2))
        for entity in queried_entities:
            print(entity.RowKey)  # Doe the 1, Doe the 2

        # Filter
        filter = "RowKey eq '{}'".format(entities[1]['RowKey'])
        queried_entities = list(self.service.query_entities(table_name, filter=filter))
        for entity in queried_entities:
            print(entity.RowKey)  # Doe the 2

        # Select
        # Get only the column(s) specified
        queried_entities = list(self.service.query_entities(table_name, select='birthday'))
        for entity in queried_entities:
            print(entity.birthday)  # All 4 John Doe character's birthdays
        queried_entities[0].get('RowKey')  # None

        # Accept
        # Default contains all necessary type info. JSON_NO_METADATA returns no type info, though we can guess some client side.
        # If type cannot be inferred, the value is simply returned as a string.
        queried_entities = list(self.service.query_entities(table_name,
                                                            accept=TablePayloadFormat.JSON_NO_METADATA))  # entities w/ all properties, missing type
        queried_entities[0].birthday  # (string)
        queried_entities[0].deceased  # (boolean)

        # Accept w/ Resolver
        # A resolver can be specified to give type info client side if JSON_NO_METADATA is used.
        def resolver(pk, rk, name, value, type):
            if name == 'birthday':
                return EdmType.DATETIME

        queried_entities = list(self.service.query_entities(table_name,
                                                            accept=TablePayloadFormat.JSON_NO_METADATA,
                                                            property_resolver=resolver))  # entityentities w/ all properties, missing type resolved client side
        queried_entities[0].birthday  # (datetime)
        queried_entities[0].deceased  # (boolean)

        self.service.delete_table(table_name)

    def batch(self):
        table_name = self._create_table()

        entity = Entity()
        entity.PartitionKey = 'batch'
        entity.test = True

        # All operations in the same batch must have the same partition key but different row keys
        # Batches can hold from 1 to 100 entities
        # Batches are atomic. All operations completed simulatenously. If one operation fails, they all fail.
        # Insert, update, merge, insert or merge, insert or replace, and delete entity operations are supported

        # Context manager style
        with self.service.batch(table_name) as batch:
            for i in range(0, 5):
                entity.RowKey = 'context_{}'.format(i)
                batch.insert_entity(entity)

        # Commit style
        batch = TableBatch()
        for i in range(0, 5):
            entity.RowKey = 'commit_{}'.format(i)
            batch.insert_entity(entity)
        self.service.commit_batch(table_name, batch)

        self.service.delete_table(table_name)

    def create_entity_class(self):
        '''
        Creates a class-based entity with fixed values, using all of the supported data types.
        '''
        entity = Entity()

        # Partition key and row key must be strings and are required
        entity.PartitionKey = 'pk{}'.format(str(uuid.uuid4()).replace('-', ''))
        entity.RowKey = 'rk{}'.format(str(uuid.uuid4()).replace('-', ''))

        # Some basic types are inferred
        entity.age = 39  # EdmType.INT64
        entity.large = 933311100  # EdmType.INT64
        entity.sex = 'male'  # EdmType.STRING
        entity.married = True  # EdmType.BOOLEAN
        entity.ratio = 3.1  # EdmType.DOUBLE
        entity.birthday = datetime(1970, 10, 4)  # EdmType.DATETIME

        # Binary, Int32 and GUID must be explicitly typed
        entity.binary = EntityProperty(EdmType.BINARY, b'xyz')
        entity.other = EntityProperty(EdmType.INT32, 20)
        entity.clsid = EntityProperty(EdmType.GUID, 'c9da6455-213d-42c9-9a79-3e9149a57833')
        return entity

    def create_entity_dict(self):
        '''
        Creates a dict-based entity with fixed values, using all of the supported data types.
        '''
        entity = {}

        # Partition key and row key must be strings and are required
        entity['PartitionKey'] = 'pk{}'.format(str(uuid.uuid4()).replace('-', ''))
        entity['RowKey'] = 'rk{}'.format(str(uuid.uuid4()).replace('-', ''))

        # Some basic types are inferred
        entity['age'] = 39  # EdmType.INT64
        entity['large'] = 933311100  # EdmType.INT64
        entity['sex'] = 'male'  # EdmType.STRING
        entity['married'] = True  # EdmType.BOOLEAN
        entity['ratio'] = 3.1  # EdmType.DOUBLE
        entity['birthday'] = datetime(1970, 10, 4)  # EdmType.DATETIME

        # Binary, Int32 and GUID must be explicitly typed
        entity['binary'] = EntityProperty(EdmType.BINARY, b'xyz')
        entity['other'] = EntityProperty(EdmType.INT32, 20)
        entity['clsid'] = EntityProperty(EdmType.GUID, 'c9da6455-213d-42c9-9a79-3e9149a57833')
        return entity

    def insert_entity(self):
        table_name = self._create_table()

        # Basic w/ dict
        entity = self.create_entity_dict()
        etag = self.service.insert_entity(table_name, entity)

        # Basic w/ class
        entity = self.create_entity_class()
        etag = self.service.insert_entity(table_name, entity)

        self.service.delete_table(table_name)

    def get_entity(self):
        table_name = self._create_table()
        insert_entity = self.create_entity_class()
        etag = self.service.insert_entity(table_name, insert_entity)

        # Basic
        # Can access properties as dict or like an object
        entity = self.service.get_entity(table_name, insert_entity.PartitionKey,
                                         insert_entity.RowKey)  # entity w/ all properties
        entity.age  # 39 (number)
        entity['age']  # 39 (number)
        entity.clsid.value  # 'c9da6455-213d-42c9-9a79-3e9149a57833' (string)
        entity.clsid.type  # Edm.Guid

        # Select
        entity = self.service.get_entity(table_name, insert_entity.PartitionKey, insert_entity.RowKey,
                                         select='age')  # entity w/ just 'age'
        entity['age']  # 39 (number)
        entity.get('clsid')  # None

        # Accept
        # Default contains all necessary type info. JSON_NO_METADATA returns no type info, though we can guess some client side.
        # If type cannot be inferred, the value is simply returned as a string.
        entity = self.service.get_entity(table_name, insert_entity.PartitionKey, insert_entity.RowKey,
                                         accept=TablePayloadFormat.JSON_NO_METADATA)  # entity w/ all properties, missing type
        entity.age  # '39' (string)
        entity.clsid  # 'c9da6455-213d-42c9-9a79-3e9149a57833' (string)
        entity.married  # True (boolean)

        # Accept w/ Resolver
        # A resolver can be specified to give type info client side if JSON_NO_METADATA is used.
        def resolver(pk, rk, name, value, type):
            if name == 'large' or name == 'age':
                return EdmType.INT64
            if name == 'birthday':
                return EdmType.DATETIME
            if name == 'clsid':
                return EdmType.GUID

        entity = self.service.get_entity(table_name, insert_entity.PartitionKey, insert_entity.RowKey,
                                         accept=TablePayloadFormat.JSON_NO_METADATA,
                                         property_resolver=resolver)  # entity w/ all properties, missing type
        entity.age  # 39 (number)
        entity.clsid.value  # 'c9da6455-213d-42c9-9a79-3e9149a57833' (string)
        entity.clsid.type  # Edm.Guid
        entity.married  # True (boolean)

        self.service.delete_table(table_name)

    def update_entity(self):
        table_name = self._create_table()
        entity = {'PartitionKey': 'John',
                  'RowKey': 'Doe',
                  'deceased': False,
                  'birthday': datetime(1991, 10, 4)}
        etag = self.service.insert_entity(table_name, entity)

        # Basic
        # Replaces entity entirely
        entity = {'PartitionKey': 'John',
                  'RowKey': 'Doe',
                  'deceased': True}
        etag = self.service.update_entity(table_name, entity)
        received_entity = self.service.get_entity(table_name, entity['PartitionKey'], entity['RowKey'])
        received_entity.get('deceased')  # True
        received_entity.get('birthday')  # None

        # If match
        # Replaces entity entirely if etag matches
        entity = {'PartitionKey': 'John',
                  'RowKey': 'Doe',
                  'id': 'abc12345'}

        self.service.update_entity(table_name, entity, if_match=etag)  # Succeeds
        try:
            self.service.update_entity(table_name, entity, if_match=etag)  # Throws as previous update changes etag
        except AzureHttpError:
            pass

        self.service.delete_table(table_name)

    def merge_entity(self):
        table_name = self._create_table()
        entity = {'PartitionKey': 'John',
                  'RowKey': 'Doe',
                  'deceased': False,
                  'birthday': datetime(1991, 10, 4)}
        etag = self.service.insert_entity(table_name, entity)

        # Basic
        # Replaces entity entirely
        entity = {'PartitionKey': 'John',
                  'RowKey': 'Doe',
                  'deceased': True}
        etag = self.service.merge_entity(table_name, entity)
        received_entity = self.service.get_entity(table_name, entity['PartitionKey'], entity['RowKey'])
        received_entity.get('deceased')  # True
        received_entity.get('birthday')  # datetime(1991, 10, 4)

        # If match
        # Merges entity if etag matches
        entity = {'PartitionKey': 'John',
                  'RowKey': 'Doe',
                  'id': 'abc12345'}

        self.service.merge_entity(table_name, entity, if_match=etag)  # Succeeds
        try:
            self.service.merge_entity(table_name, entity, if_match=etag)  # Throws as previous update changes etag
        except AzureHttpError:
            pass

        self.service.delete_table(table_name)

    def insert_or_merge_entity(self):
        table_name = self._create_table()
        entity = {'PartitionKey': 'John',
                  'RowKey': 'Doe',
                  'deceased': False,
                  'birthday': datetime(1991, 10, 4)}

        # Basic
        # Inserts if entity does not already exist
        etag = self.service.insert_or_merge_entity(table_name, entity)

        # Merges if entity already exists
        entity = {'PartitionKey': 'John',
                  'RowKey': 'Doe',
                  'id': 'abc12345'}
        etag = self.service.insert_or_merge_entity(table_name, entity)
        received_entity = self.service.get_entity(table_name, entity['PartitionKey'], entity['RowKey'])
        received_entity.get('id')  # 'abc12345'
        received_entity.get('deceased')  # False

        self.service.delete_table(table_name)

    def insert_or_replace_entity(self):
        table_name = self._create_table()
        entity = {'PartitionKey': 'John',
                  'RowKey': 'Doe',
                  'deceased': False,
                  'birthday': datetime(1991, 10, 4)}

        # Basic
        # Inserts if entity does not already exist
        etag = self.service.insert_or_replace_entity(table_name, entity)

        # Replaces if entity already exists
        entity = {'PartitionKey': 'John',
                  'RowKey': 'Doe',
                  'id': 'abc12345'}
        etag = self.service.insert_or_replace_entity(table_name, entity)
        received_entity = self.service.get_entity(table_name, entity['PartitionKey'], entity['RowKey'])
        received_entity.get('id')  # 'abc12345'
        received_entity.get('deceased')  # None

        self.service.delete_table(table_name)

    def delete_entity(self):
        table_name = self._create_table()
        entity = {'PartitionKey': 'John',
                  'RowKey': 'Doe'}
        etag = self.service.insert_entity(table_name, entity)

        # Basic
        # Deletes entity
        self.service.delete_entity(table_name, entity['PartitionKey'], entity['RowKey'])

        # If match
        # Deletes entity only if etag matches
        entity = {'PartitionKey': 'John',
                  'RowKey': 'Doe',
                  'id': 'abc12345'}
        etag = self.service.insert_entity(table_name, entity)
        self.service.update_entity(table_name, entity, if_match=etag)  # Succeeds
        try:
            self.service.delete_entity(table_name, entity['PartitionKey'], entity['RowKey'],
                                       if_match=etag)  # Throws as update changes etag
        except AzureHttpError:
            pass

        self.service.delete_table(table_name)

    def list_tables(self):
        table_name1 = self._create_table('table1')
        table_name2 = self._create_table('secondtable')

        # Basic
        # Commented out as this will list every table in your account
        # tables = list(self.service.list_tables())
        # for table in tables:
        #    print(table.name) # secondtable, table1, all other tables created in the self.service        

        # Num results
        # Will return in alphabetical order. 
        tables = list(self.service.list_tables(num_results=2))
        for table in tables:
            print(table.name)  # secondtable, table1, or whichever 2 queues are alphabetically first in your account

        self.service.delete_table(table_name1)
        self.service.delete_table(table_name2)

    def service_properties(self):
        # Basic
        self.service.set_table_service_properties(logging=Logging(delete=True),
                                                  hour_metrics=Metrics(enabled=True, include_apis=True),
                                                  minute_metrics=Metrics(enabled=True, include_apis=False),
                                                  cors=[CorsRule(allowed_origins=['*'], allowed_methods=['GET'])])

        # Wait 30 seconds for settings to propagate
        time.sleep(30)

        props = self.service.get_table_service_properties()  # props = ServiceProperties() w/ all properties specified above

        # Omitted properties will not overwrite what's already on the self.service
        # Empty properties will clear
        self.service.set_table_service_properties(cors=[])

        # Wait 30 seconds for settings to propagate
        time.sleep(30)

        props = self.service.get_table_service_properties()  # props = ServiceProperties() w/ CORS rules cleared
