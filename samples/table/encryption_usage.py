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

from cryptography.hazmat.primitives.keywrap import(
    aes_key_wrap,
    aes_key_unwrap,
)
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric.rsa import generate_private_key
from cryptography.hazmat.primitives.asymmetric.padding import (
    OAEP,
    MGF1,
)
from cryptography.hazmat.primitives.hashes import SHA1
from os import urandom
import uuid
from azure.storage.table import (
    Entity,
    TableBatch,
    EdmType,
    EntityProperty,
    TablePayloadFormat,
)
from azure.common import AzureException

# Sample implementations of the encryption-related interfaces.
class KeyWrapper:
    def __init__(self, kid):
        self.kek = urandom(32) 
        self.backend = default_backend()
        self.kid = 'local:' + kid
    def wrap_key(self, key, algorithm='A256KW'):
        if algorithm == 'A256KW':
            return aes_key_wrap(self.kek, key, self.backend)
        else:
            raise ValueError(_ERROR_UNKNOWN_KEY_WRAP_ALGORITHM)
    def unwrap_key(self, key, algorithm):
        if algorithm == 'A256KW':
            return aes_key_unwrap(self.kek, key, self.backend)
        else:
            raise ValueError(_ERROR_UNKNOWN_KEY_WRAP_ALGORITHM)
    def get_key_wrap_algorithm(self):
        return 'A256KW'
    def get_kid(self):
        return self.kid

class KeyResolver:
    def __init__(self):
        self.keys = {}
    def put_key(self, key):
        self.keys[key.get_kid()] = key
    def resolve_key(self, kid):
        return self.keys[kid]

class RSAKeyWrapper:
    def __init__(self, kid):
        self.private_key = generate_private_key(public_exponent = 65537,
                                                key_size = 2048,
                                                backend = default_backend())
        self.public_key = self.private_key.public_key()
        self.kid = 'local:' + kid
    def wrap_key(self, key, algorithm='RSA'):
        if algorithm == 'RSA':
            return self.public_key.encrypt(key,
                                     OAEP(
                                         mgf = MGF1(algorithm=SHA1()),
                                         algorithm=SHA1(),
                                         label=None)
                                     )
        else:
            raise ValueError(_ERROR_UNKNOWN_KEY_WRAP_ALGORITHM)
    def unwrap_key(self, key, algorithm):
        if algorithm == 'RSA':
            return self.private_key.decrypt(key,
                                        OAEP(
                                            mgf=MGF1(algorithm=SHA1()),
                                            algorithm=SHA1(),
                                            label=None)
                                        )
        else:
            raise ValueError(_ERROR_UNKNOWN_KEY_WRAP_ALGORITHM)
    def get_key_wrap_algorithm(self):
        return 'RSA'
    def get_kid(self):
        return self.kid

class TableEncryptionSamples():  

    def __init__(self, account):
        self.account = account

    def run_all_samples(self):
        self.service = self.account.create_table_service()

        self.put_encrypted_entity_properties()
        self.put_encrypted_entity_encryption_resolver()
        self.get_encrypted_entity()
        self.get_encrypted_entity_key_encryption_key()
        self.replace_encrypted_entity()
        self.query_encrypted_entities()
        self.batch_encrypted_entities()
        self.require_encryption()
        self.alternate_key_encryption_algorithms()
        self.merge_not_supported()

    def _get_table_reference(self, prefix='table'):
        table_name = '{}{}'.format(prefix, str(uuid.uuid4()).replace('-', ''))
        return table_name

    def _create_table(self, prefix='table'):
        table_name = self._get_table_reference(prefix)
        self.service.create_table(table_name)
        return table_name

    def _create_base_entity_dict(self):
        entity = {}
        # Partition key and row key must be strings and are required
        entity['PartitionKey'] = 'pk{}'.format(str(uuid.uuid4()).replace('-', ''))
        entity['RowKey'] = 'rk{}'.format(str(uuid.uuid4()).replace('-', ''))
        return entity

    def _create_base_entity_class(self):
        # Partition key and row key must be strings and are required
        entity = Entity()
        entity['PartitionKey'] = 'pk{}'.format(str(uuid.uuid4()).replace('-', ''))
        entity['RowKey'] = 'rk{}'.format(str(uuid.uuid4()).replace('-', ''))
        return entity

    def _create_entity_for_encryption(self):
        entity = self._create_base_entity_class()
        entity['foo'] = EntityProperty(EdmType.STRING, 'bar', True)
        return entity

    def _create_query_table_encrypted(self, entity_count):
        '''
        Creates a table with the specified name and adds entities with the
        default set of values. PartitionKey is set to 'MyPartition' and RowKey
        is set to a unique counter value starting at 1 (as a string). The
        'foo' attribute is set to be encrypted.
        '''
        table_name = self._create_table(prefix='querytable')
        self.service.require_encryption = True

        entity = self._create_entity_for_encryption()
        with self.service.batch(table_name) as batch:
            for i in range(1, entity_count + 1):
                entity['RowKey'] = entity['RowKey'] + str(i)
                batch.insert_entity(entity)
        return table_name

    # A sample encryption resolver. This resolver is a simple case that will mark
    # any property named 'foo' for encryption, regardless of the partition or row 
    # it is in.
    def encryption_resolver(self, pk, rk, property):
       return property == 'foo'

    def put_encrypted_entity_properties(self):
        table_name = self._create_table()

        # Can use a dict or the Entity class to encrypt entities.
        # The EntityProperty object takes an optional parameteter, 'encrypt'
        # that marks the property for encryption when set to true.
        entity1 = self._create_base_entity_dict()
        entity1['foo'] = EntityProperty(EdmType.STRING, 'bar', True)
        entity2 = self._create_base_entity_class()
        entity2.foo = EntityProperty(EdmType.STRING, 'bar', True)
        entity3 = self._create_base_entity_class()
        entity3['badValue'] = EntityProperty(EdmType.INT64, 12, True)
        entity4 = self._create_base_entity_class()
        
        # KeyWrapper implements the key encryption key interface outlined
        # in the insert/get entity documentation.
        # Setting this property will tell these APIs to encrypt the entity.
        self.service.key_encryption_key = KeyWrapper('key1')
        self.service.insert_entity(table_name, entity1)
        self.service.insert_entity(table_name, entity2)

        # Note: The internal encryption process requires two properties, so there
        # are only 250 custom properties available when encrypting.
        # Note: str is the only type valid for encryption. Trying to encrypt other
        # properties will throw.
        
        self.service.delete_table(table_name)

    def put_encrypted_entity_encryption_resolver(self):
        table_name = self._create_table()

        entity = self._create_base_entity_class()
        entity['foo'] = 'bar'
        self.service.key_encryption_key = KeyWrapper('key1')

        # An encryption resolver is a function that takes in the Partition Key,
        # Row Key, and property name and returns true if the property should be 
        # encrypted and false otherwise. This can be used in place of explictly
        # setting each property to be encrypted through the EntityProperty class.
        self.service.encryption_resolver_function = self.encryption_resolver

        self.service.insert_entity(table_name, entity)

        self.service.delete_table(table_name)

    def get_encrypted_entity(self):
        table_name = self._create_table()
        entity = self._create_entity_for_encryption()
        self.service.key_encryption_key = KeyWrapper('key1')
        self.service.insert_entity(table_name, entity)

        # Entities can be decrypted by setting a key_resolver function on the service
        # without directly setting the key_encryption_key property itself. The function takes 
        # in the key_id (retrieved from the encrypted entity metadata) and returns the 
        # corresponding key_encryption_key. 
        key_resolver = KeyResolver()
        key_resolver.put_key(self.service.key_encryption_key)
        self.service.key_resolver_function = key_resolver.resolve_key
        self.service.key_encryption_key = None

        # Decrypted entities are stored in their raw string form, regardless of whether
        # they were stored in an EntityProperty when encrypted.

        # Retrieving and decrypting an encrypted entity works regardless of the accepted
        # payload format.
        entity_full = self.service.get_entity(table_name, entity['PartitionKey'], entity['RowKey'],
                                              accept=TablePayloadFormat.JSON_FULL_METADATA)
        entity_none = self.service.get_entity(table_name, entity['PartitionKey'], entity['RowKey'],
                                        accept=TablePayloadFormat.JSON_NO_METADATA)
        
        # Note: Properties that are encrypted on upload but not decrypted on download due to lack
        # of an encryption policy are stored in an EntityProperty with as an EdmBinary type.
        # Note: The encryption metadata headers are preserved on the entity if 
        # it is not decrypted when downloaded.
        # Note: Decrypted entities are stored in their raw string form, regardless of whether
        # they were stored in an EntityProperty when encrypted.

        self.service.key_resolver_function = None
        self.service.delete_table(table_name)

    def query_encrypted_entities(self):
        self.service.key_encryption_key = KeyWrapper('key1')
        key_resolver = KeyResolver()
        key_resolver.put_key(self.service.key_encryption_key)
        self.service.key_resolver_function = key_resolver.resolve_key
        table_name = self._create_query_table_encrypted(5)

        # Querying for entire entities will transparently decrypt retrieved entities.
        response = self.service.query_entities(table_name, num_results=5)

        # Performing a projection on a subset of properties will also implicilty
        # retrieve the encryption metatdata properties when an encryption policy is set.
        response = self.service.query_entities(table_name, num_results=5,
                                               select='PartitionKey,RowKey,foo')

        self.service.delete_table(table_name)

    def batch_encrypted_entities(self):
        table_name = self._create_table()
        entity1 = self._create_entity_for_encryption()
        entity2 = self._create_entity_for_encryption()
        entity2['PartitionKey'] = entity1['PartitionKey']

        # Batches will encrypt the entities at the time of inserting into the batch, not
        # committing the batch to the service, so the encryption policy must be 
        # passed in at the time of batch creation.
        kek = KeyWrapper('key1')
        batch = TableBatch(require_encryption=True, key_encryption_key=kek)
        batch.insert_entity(entity1)
        batch.insert_entity(entity2)
        self.service.commit_batch(table_name, batch)
        
        # When using the batch as a context manager, the tableservice object will
        # automatically apply its encryption policy to the batch. 
        entity3 = self._create_entity_for_encryption()
        entity4 = self._create_entity_for_encryption()
        entity4['PartitionKey'] = entity3['PartitionKey']
        self.service.key_encryption_key = KeyWrapper('key1')
        with self.service.batch(table_name) as batch:
            batch.insert_entity(entity3)
            batch.insert_entity(entity4)

        # Note that batches follow all the same client-side-encryption behavior as
        # the corresponding individual table operations.

        self.service.delete_table(table_name)

    def require_encryption(self):
        self.service.key_encryption_key = None
        self.service.key_resolver_function = None
        self.service.require_encryption = False
        table_name = self._create_table()
        entity_unencrypted = self._create_base_entity_class()
        entity_unencrypted['foo'] = 'bar'
        self.service.insert_entity(table_name, entity_unencrypted)

        # If the require_encryption flag is set, the service object will throw if there
        # is no encryption policy set on upload.
        self.service.key_encryption_key = None
        self.service.require_encryption = True
        try:
            self.service.insert_entity(table_name, entity_unencrypted)
            raise Exception
        except ValueError:
            pass

        # If the require_encryption flag is set, the service object will throw if there
        # is no encryption policy set on download.
        kek = KeyWrapper('key1')
        self.service.key_encryption_key = kek

        key_resolver = KeyResolver()
        key_resolver.put_key(self.service.key_encryption_key)
        self.service.key_resolver_function = key_resolver.resolve_key

        entity_encrypted = self._create_entity_for_encryption()
        self.service.insert_entity(table_name, entity_encrypted)

        self.service.key_encryption_key = None
        self.service.key_resolver_function = None
        try:
            self.service.get_entity(table_name, entity_encrypted['PartitionKey'], 
                                    entity_encrypted['RowKey']) 
            raise Exception
        except ValueError:
            pass

        # If the require_encryption flag is set, but the retrieved object is not encrypted,
        # the service object will throw.
        self.service.key_resolver_function = key_resolver.resolve_key
        try:
            self.service.get_entity(table_name, entity_unencrypted['PartitionKey'],
                                    entity_unencrypted['RowKey'])
            raise Exception
        except AzureException:
            pass

        self.service.delete_table(table_name)

    def alternate_key_encryption_algorithms(self):
        table_name = self._create_table()
        entity = self._create_entity_for_encryption()

        # The key wrapping algorithm used by the key_encryption_key is entirely
        # up to the choice of the user. For instance, RSA may be used.
        self.service.key_encryption_key = RSAKeyWrapper('key2')
        self.service.insert_entity(table_name, entity)

        key_resolver = KeyResolver()
        key_resolver.put_key(self.service.key_encryption_key)
        self.service.key_resolver_function = key_resolver.resolve_key
        entity = self.service.get_entity(table_name, entity['PartitionKey'], entity['RowKey'])

        self.service.delete_table(table_name)

    def merge_not_supported(self):
        table_name = self._create_table()
        entity = self._create_entity_for_encryption()
        self.service.key_encryption_key = KeyWrapper('key1')
        self.service.insert_entity(table_name, entity)

        # Merging encrypted entities is not supported. Calling merge with 
        # an encryption policy set will cause merge entities to fail.
        # If the require_encryption flag is set, merge_entities will fail.
        # Note that insert_or_merge exhibits the same encryption behavior.
        self.service.require_encryption = True
        try:
            self.service.merge_entity(table_name, entity)
            raise Exception
        except ValueError:
            pass
        
        self.service.require_encryption = False
        try:
            self.service.merge_entity(table_name, entity)
            raise Exception
        except ValueError:
            pass

        self.service.delete_table(table_name)

    def get_encrypted_entity_key_encryption_key(self):
        table_name = self._create_table()
        entity = self._create_entity_for_encryption()
        kek = KeyWrapper('key1')
        self.service.key_encryption_key = kek
        self.service.insert_entity(table_name, entity)
        
        # If the key_encryption_key property is set, the tableservice object will
        # try to decrypt entities using that key. If both the key_resolver and key_encryption_key
        # properties are set, the result of the key_resolver will take precedence and the decryption
        # will fail if that key is not successful.
        entity = self.service.get_entity(table_name, entity['PartitionKey'], entity['RowKey'])

        self.service.delete_table(table_name)

    def replace_encrypted_entity(self):
        table_name = self._create_table()
        entity = self._create_entity_for_encryption()
        self.service.key_encryption_key = KeyWrapper('key1')
        self.service.insert_entity(table_name, entity)

        # An entity, encrypted or decrypted, may be replaced by an encrypted entity.
        entity['foo'].value = 'updated'
        self.service.update_entity(table_name, entity)

        self.service.delete_table(table_name)