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
import time
import uuid
from datetime import datetime, timedelta

from azure.common import (
    AzureHttpError,
    AzureConflictHttpError,
    AzureMissingResourceHttpError,
)
from azure.storage import (
    AccessPolicy,
    ResourceTypes,
    AccountPermissions,
    CloudStorageAccount,
    Logging,
    Metrics,
    CorsRule,
)
from azure.storage.table import (
    TableService,
    TablePermissions,
    Entity,
    TableBatch,
    EdmType,
    EntityProperty,
    TablePayloadFormat,
)

ACCOUNT_NAME = ''
ACCOUNT_KEY = ''
account = CloudStorageAccount(ACCOUNT_NAME, ACCOUNT_KEY)

class TableSamples():  

    def run_all_samples(self):
        self.create_table()
        self.delete_table()
        self.exists()
        self.list_tables()   

        self.create_entity_class()
        self.create_entity_dict()
        self.insert_entity()
        self.get_entity()
        self.update_entity()
        self.merge_entity()
        self.insert_or_merge_entity()
        self.insert_or_replace_entity()
        self.delete_entity()
        
        self.query_entities()
        self.batch()

        self.table_sas()
        self.account_sas()

        # The below run more slowly as they have sleeps
        self.table_acl()
        self.sas_with_signed_identifiers()
        self.service_properties()

    def _get_table_reference(self, prefix='table'):
        table_name = '{}{}'.format(prefix, str(uuid.uuid4()).replace('-', ''))
        return table_name

    def _create_table(self, service, prefix='table'):
        table_name = self._get_table_reference(prefix)
        service.create_table(table_name)
        return table_name

    def create_table(self):
        service = account.create_table_service()
        
        # Basic
        table_name1 = self._get_table_reference()
        created = service.create_table(table_name1) # True

        # Fail on exist
        table_name2 = self._get_table_reference()
        created = service.create_table(table_name2) # True 
        created = service.create_table(table_name2) # False
        try:
            service.create_table(table_name2, fail_on_exist=True)
        except AzureConflictHttpError:
            pass

        service.delete_table(table_name1)
        service.delete_table(table_name2)

    def delete_table(self):
        service = account.create_table_service()

        # Basic
        table_name = self._create_table(service)
        deleted = service.delete_table(table_name) # True 

        # Fail not exist
        table_name = self._get_table_reference()
        deleted = service.delete_table(table_name) # False
        try:
            service.delete_table(table_name, fail_not_exist=True)
        except AzureMissingResourceHttpError:
            pass

    def exists(self):
        service = account.create_table_service()
        table_name = self._get_table_reference()

        # Does not exist
        exists = service.exists(table_name) # False

        # Exists
        service.create_table(table_name)
        exists = service.exists(table_name) # True

        service.delete_table(table_name)

    def list_tables(self):
        service = account.create_table_service()
        table_name1 = self._create_table(service, 'table1')
        table_name2 = self._create_table(service, 'secondtable')

        # Basic
        # Commented out as this will list every table in your account
        # tables = list(service.list_tables())
        # for table in tables:
        #    print(table.name) # secondtable, table1, all other tables created in the service        

        # Max results
        # Will return in alphabetical order. 
        tables = list(service.list_tables(max_results=2))
        for table in tables:
            print(table.name) # secondtable, table1

        service.delete_table(table_name1)
        service.delete_table(table_name2)

    def create_entity_class(self):
        '''
        Creates a class-based entity with fixed values, using all of the supported data types.
        '''
        entity = Entity()

        # Partition key and row key must be strings and are required
        entity.PartitionKey= 'pk{}'.format(str(uuid.uuid4()).replace('-', ''))
        entity.RowKey = 'rk{}'.format(str(uuid.uuid4()).replace('-', '')) 

        # Some basic types are inferred
        entity.age = 39 # EdmType.INT64
        entity.large = 933311100 # EdmType.INT64
        entity.sex = 'male' # EdmType.STRING
        entity.married = True # EdmType.BOOLEAN
        entity.ratio = 3.1 # EdmType.DOUBLE
        entity.birthday = datetime(1970, 10, 4) # EdmType.DATETIME

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
        entity['age'] = 39 # EdmType.INT64
        entity['large'] = 933311100 # EdmType.INT64
        entity['sex'] = 'male' # EdmType.STRING
        entity['married'] = True # EdmType.BOOLEAN
        entity['ratio'] = 3.1 # EdmType.DOUBLE
        entity['birthday'] = datetime(1970, 10, 4) # EdmType.DATETIME

        # Binary, Int32 and GUID must be explicitly typed
        entity['binary'] = EntityProperty(EdmType.BINARY, b'xyz')
        entity['other'] = EntityProperty(EdmType.INT32, 20)
        entity['clsid'] = EntityProperty(EdmType.GUID, 'c9da6455-213d-42c9-9a79-3e9149a57833')
        return entity

    def insert_entity(self):
        service = account.create_table_service()
        table_name = self._create_table(service)

        # Basic w/ dict
        entity = self.create_entity_dict()
        etag = service.insert_entity(table_name, entity)

        # Basic w/ class
        entity = self.create_entity_class()
        etag = service.insert_entity(table_name, entity)

        service.delete_table(table_name)

    def get_entity(self):
        service = account.create_table_service()
        table_name = self._create_table(service)
        insert_entity = self.create_entity_class()
        etag = service.insert_entity(table_name, insert_entity)

        # Basic
        # Can access properties as dict or like an object
        entity = service.get_entity(table_name, insert_entity.PartitionKey, insert_entity.RowKey) # entity w/ all properties
        entity.age # 39 (number)
        entity['age'] #39 (number)
        entity.clsid.value # 'c9da6455-213d-42c9-9a79-3e9149a57833' (string)
        entity.clsid.type # Edm.Guid

        # Select
        entity = service.get_entity(table_name, insert_entity.PartitionKey, insert_entity.RowKey, 
                                    select='age') # entity w/ just 'age'
        entity['age'] # 39 (number)
        entity.get('clsid') # None

        # Accept
        # Default contains all necessary type info. JSON_NO_METADATA returns no type info, though we can guess some client side.
        # If type cannot be inferred, the value is simply returned as a string.
        entity = service.get_entity(table_name, insert_entity.PartitionKey, insert_entity.RowKey, 
                                    accept=TablePayloadFormat.JSON_NO_METADATA) # entity w/ all properties, missing type
        entity.age # '39' (string)
        entity.clsid # 'c9da6455-213d-42c9-9a79-3e9149a57833' (string)
        entity.married # True (boolean)

        # Accept w/ Resolver
        # A resolver can be specified to give type info client side if JSON_NO_METADATA is used.
        def resolver(pk, rk, name, value, type):
            if name == 'large' or name == 'age':
                return EdmType.INT64
            if name == 'birthday':
                return EdmType.DATETIME
            if name == 'clsid':
                return EdmType.GUID
        entity = service.get_entity(table_name, insert_entity.PartitionKey, insert_entity.RowKey, 
                                    accept=TablePayloadFormat.JSON_NO_METADATA, 
                                    property_resolver=resolver) # entity w/ all properties, missing type
        entity.age # 39 (number)
        entity.clsid.value # 'c9da6455-213d-42c9-9a79-3e9149a57833' (string)
        entity.clsid.type # Edm.Guid
        entity.married # True (boolean)

        service.delete_table(table_name)

    def update_entity(self):
        service = account.create_table_service()
        table_name = self._create_table(service)
        entity = {'PartitionKey': 'John',
                         'RowKey': 'Doe',
                         'deceased': False,
                         'birthday': datetime(1991, 10, 4)}
        etag = service.insert_entity(table_name, entity)

        # Basic
        # Replaces entity entirely
        entity = {'PartitionKey': 'John',
                  'RowKey': 'Doe',
                  'deceased': True}
        etag = service.update_entity(table_name, entity)
        received_entity = service.get_entity(table_name, entity['PartitionKey'], entity['RowKey'])
        received_entity.get('deceased') # True
        received_entity.get('birthday') # None

        # If match
        # Replaces entity entirely if etag matches
        entity = {'PartitionKey': 'John',
                  'RowKey': 'Doe',
                  'id': 'abc12345'}

        service.update_entity(table_name, entity, if_match=etag) # Succeeds
        try:
            service.update_entity(table_name, entity, if_match=etag) # Throws as previous update changes etag
        except AzureHttpError:
            pass

        service.delete_table(table_name)

    def merge_entity(self):
        service = account.create_table_service()
        table_name = self._create_table(service)
        entity = {'PartitionKey': 'John',
                         'RowKey': 'Doe',
                         'deceased': False,
                         'birthday': datetime(1991, 10, 4)}
        etag = service.insert_entity(table_name, entity)

        # Basic
        # Replaces entity entirely
        entity = {'PartitionKey': 'John',
                  'RowKey': 'Doe',
                  'deceased': True}
        etag = service.merge_entity(table_name, entity)
        received_entity = service.get_entity(table_name, entity['PartitionKey'], entity['RowKey'])
        received_entity.get('deceased') # True
        received_entity.get('birthday') # datetime(1991, 10, 4)

        # If match
        # Merges entity if etag matches
        entity = {'PartitionKey': 'John',
                  'RowKey': 'Doe',
                  'id': 'abc12345'}

        service.merge_entity(table_name, entity, if_match=etag) # Succeeds
        try:
            service.merge_entity(table_name, entity, if_match=etag) # Throws as previous update changes etag
        except AzureHttpError:
            pass

        service.delete_table(table_name)

    def insert_or_merge_entity(self):
        service = account.create_table_service()
        table_name = self._create_table(service)
        entity = {'PartitionKey': 'John',
                         'RowKey': 'Doe',
                         'deceased': False,
                         'birthday': datetime(1991, 10, 4)}

        # Basic
        # Inserts if entity does not already exist
        etag = service.insert_or_merge_entity(table_name, entity)

        # Merges if entity already exists
        entity = {'PartitionKey': 'John',
                  'RowKey': 'Doe',
                  'id': 'abc12345'}
        etag = service.insert_or_merge_entity(table_name, entity)
        received_entity = service.get_entity(table_name, entity['PartitionKey'], entity['RowKey'])
        received_entity.get('id') # 'abc12345'
        received_entity.get('deceased') # False

        service.delete_table(table_name)

    def insert_or_replace_entity(self):
        service = account.create_table_service()
        table_name = self._create_table(service)
        entity = {'PartitionKey': 'John',
                         'RowKey': 'Doe',
                         'deceased': False,
                         'birthday': datetime(1991, 10, 4)}

        # Basic
        # Inserts if entity does not already exist
        etag = service.insert_or_replace_entity(table_name, entity)

        # Replaces if entity already exists
        entity = {'PartitionKey': 'John',
                  'RowKey': 'Doe',
                  'id': 'abc12345'}
        etag = service.insert_or_replace_entity(table_name, entity)
        received_entity = service.get_entity(table_name, entity['PartitionKey'], entity['RowKey'])
        received_entity.get('id') # 'abc12345'
        received_entity.get('deceased') # None

        service.delete_table(table_name)

    def delete_entity(self):
        service = account.create_table_service()
        table_name = self._create_table(service)
        entity = {'PartitionKey': 'John',
                  'RowKey': 'Doe'}
        etag = service.insert_entity(table_name, entity)

        # Basic
        # Deletes entity
        service.delete_entity(table_name, entity['PartitionKey'], entity['RowKey'])

        # If match
        # Deletes entity only if etag matches
        entity = {'PartitionKey': 'John',
                  'RowKey': 'Doe',
                  'id': 'abc12345'}
        etag = service.insert_entity(table_name, entity)
        service.update_entity(table_name, entity, if_match=etag) # Succeeds
        try:
            service.delete_entity(table_name, entity['PartitionKey'], entity['RowKey'], if_match=etag) # Throws as update changes etag
        except AzureHttpError:
            pass

        service.delete_table(table_name)

    def query_entities(self):
        service = account.create_table_service()
        table_name = self._create_table(service)

        entities = []
        for i in range(1, 5):
            entity = {'PartitionKey': 'John',
                      'RowKey': 'Doe the {}'.format(i),
                      'deceased': False,
                      'birthday': datetime(1991, 10, i)}
            service.insert_entity(table_name, entity)
            entities.append(entity)

        # Basic
        # Can access properties as dict or like an object
        queried_entities = list(service.query_entities(table_name))
        for entity in queried_entities:
            print(entity.RowKey) # All 4 John Doe characters

        # Top
        queried_entities = list(service.query_entities(table_name, top=2))
        for entity in queried_entities:
            print(entity.RowKey) # Doe the 1, Doe the 2

        # Filter
        filter = "RowKey eq '{}'".format(entities[1]['RowKey'])
        queried_entities = list(service.query_entities(table_name, filter=filter))
        for entity in queried_entities:
            print(entity.RowKey) # Doe the 2

        # Select
        # Get only the column(s) specified
        queried_entities = list(service.query_entities(table_name, select='birthday'))
        for entity in queried_entities:
            print(entity.birthday) # All 4 John Doe character's birthdays
        queried_entities[0].get('RowKey') # None

        # Accept
        # Default contains all necessary type info. JSON_NO_METADATA returns no type info, though we can guess some client side.
        # If type cannot be inferred, the value is simply returned as a string.
        queried_entities = list(service.query_entities(table_name, 
                                                       accept=TablePayloadFormat.JSON_NO_METADATA)) # entities w/ all properties, missing type
        queried_entities[0].birthday # (string)
        queried_entities[0].deceased # (boolean)

        # Accept w/ Resolver
        # A resolver can be specified to give type info client side if JSON_NO_METADATA is used.
        def resolver(pk, rk, name, value, type):
            if name == 'birthday':
                return EdmType.DATETIME
        entity = list(service.query_entities(table_name, 
                                             accept=TablePayloadFormat.JSON_NO_METADATA, 
                                             property_resolver=resolver)) # entityentities w/ all properties, missing type resolved client side
        queried_entities[0].birthday # (datetime)
        queried_entities[0].deceased # (boolean)

        service.delete_table(table_name)

    def batch(self):
        service = account.create_table_service()
        table_name = self._create_table(service)

        entity = Entity()
        entity.PartitionKey = 'batch'
        entity.test = True

        # All operations in the same batch must have the same partition key but different row keys
        # Batches can hold from 1 to 100 entities
        # Batches are atomic. All operations completed simulatenously. If one operation fails, they all fail.
        # Insert, update, merge, insert or merge, insert or replace, and delete entity operations are supported

        # Context manager style
        with service.batch(table_name) as batch:
            for i in range(0, 5):
                entity.RowKey = 'context_{}'.format(i)
                batch.insert_entity(entity)

        # Commit style
        batch = TableBatch()
        for i in range(0, 5):
            entity.RowKey = 'commit_{}'.format(i)
            batch.insert_entity(entity)
        service.commit_batch(table_name, batch)

        service.delete_table(table_name)

    def table_sas(self):
        service = account.create_table_service()
        table_name = self._create_table(service)
        entity = {
            'PartitionKey': 'test',
            'RowKey': 'test1',
            'text': 'hello world',
            }
        service.insert_entity(table_name, entity)

        # Access only to the entities in the given table
        # Query permissions to access entities
        # Expires in an hour
        token = service.generate_table_shared_access_signature(
            table_name,
            TablePermissions.QUERY,
            datetime.utcnow() + timedelta(hours=1),
        )

        # Create a service and use the SAS
        sas_service = TableService(
            account_name=ACCOUNT_NAME,
            sas_token=token,
        )

        entities = sas_service.query_entities(table_name)
        for entity in entities:
            print(entity.text) # hello world

        service.delete_table(table_name)

    def account_sas(self):
        service = account.create_table_service()
        table_name = self._create_table(service)
        entity = {
            'PartitionKey': 'test',
            'RowKey': 'test1',
            'text': 'hello world',
            }
        service.insert_entity(table_name, entity)

        # Access to all entities in all the tables
        # Expires in an hour
        token = service.generate_account_shared_access_signature(
            ResourceTypes.OBJECT,
            AccountPermissions.READ,
            datetime.utcnow() + timedelta(hours=1),
        )

        # Create a service and use the SAS
        sas_service = TableService(
            account_name=ACCOUNT_NAME,
            sas_token=token,
        )

        entities = list(sas_service.query_entities(table_name))
        for entity in entities:
            print(entity.text) # hello world

        service.delete_table(table_name)

    def table_acl(self):
        service = account.create_table_service()
        table_name = self._create_table(service)

        # Basic
        access_policy = AccessPolicy(permission=TablePermissions.QUERY,
                                     expiry=datetime.utcnow() + timedelta(hours=1))
        identifiers = {'id': access_policy}
        service.set_table_acl(table_name, identifiers)

        # Wait 30 seconds for acl to propagate
        time.sleep(30)
        acl = service.get_table_acl(table_name) # {id: AccessPolicy()}

        # Replaces values, does not merge
        access_policy = AccessPolicy(permission=TablePermissions.QUERY,
                                     expiry=datetime.utcnow() + timedelta(hours=1))
        identifiers = {'id2': access_policy}
        service.set_table_acl(table_name, identifiers)

        # Wait 30 seconds for acl to propagate
        time.sleep(30)
        acl = service.get_table_acl(table_name) # {id2: AccessPolicy()}

        # Clear
        service.set_table_acl(table_name)

        # Wait 30 seconds for acl to propagate
        time.sleep(30)
        acl = service.get_table_acl(table_name) # {}

        service.delete_table(table_name)

    def sas_with_signed_identifiers(self):
        service = account.create_table_service()
        table_name = self._create_table(service)
        entity = {
            'PartitionKey': 'test',
            'RowKey': 'test1',
            'text': 'hello world',
            }
        service.insert_entity(table_name, entity)

        # Set access policy on table
        access_policy = AccessPolicy(permission=TablePermissions.QUERY,
                                     expiry=datetime.utcnow() + timedelta(hours=1))
        identifiers = {'id': access_policy}
        acl = service.set_table_acl(table_name, identifiers)

        # Wait 30 seconds for acl to propagate
        time.sleep(30)

        # Indicates to use the access policy set on the table
        token = service.generate_table_shared_access_signature(
            table_name,
            id='id'
        )

        # Create a service and use the SAS
        sas_service = TableService(
            account_name=ACCOUNT_NAME,
            sas_token=token,
        )

        entities = list(sas_service.query_entities(table_name))
        for entity in entities:
            print(entity.text) # hello world

        service.delete_table(table_name)

    def service_properties(self):
        service = account.create_table_service()

        # Basic
        service.set_table_service_properties(logging=Logging(delete=True), 
                                             hour_metrics=Metrics(enabled=True, include_apis=True), 
                                             minute_metrics=Metrics(enabled=True, include_apis=False), 
                                             cors=[CorsRule(allowed_origins=['*'], allowed_methods=['GET'])])

        # Wait 30 seconds for settings to propagate
        time.sleep(30)

        props = service.get_table_service_properties() # props = ServiceProperties() w/ all properties specified above

        # Omitted properties will not overwrite what's already on the service
        # Empty properties will clear
        service.set_table_service_properties(cors=[])

        # Wait 30 seconds for settings to propagate
        time.sleep(30)

        props = service.get_table_service_properties() # props = ServiceProperties() w/ CORS rules cleared
