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
from azure.storage.blob import(
    BlockBlobService,
    PageBlobService,
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

class BlobEncryptionSamples():  

    def __init__(self, account):
        self.account = account

    def run_all_samples(self):

        self.block_blob_service = self.account.create_block_blob_service()
        self.page_blob_service = self.account.create_page_blob_service()
        self.service_dict = {'block_blob':self.block_blob_service,
                             'page_blob':self.page_blob_service}
        self.put_encrypted_blob()
        self.get_encrypted_blob()
        self.get_encrypted_blob_key_encryption_key()
        self.require_encryption()
        self.alternate_key_algorithms()

    def _get_resource_reference(self, prefix):
        return '{}{}'.format(prefix, str(uuid.uuid4()).replace('-', ''))

    def _get_blob_reference(self, prefix='blob'):
        return self._get_resource_reference(prefix)

    def _create_encrypted_blob(self, container_name, type, prefix='blob'):
        blob_name = self._get_resource_reference(prefix)
        self.service_dict[type].create_blob_from_text(container_name, blob_name, u'hello world')
        return blob_name

    def _create_container(self, prefix='container'):
        container_name = self._get_resource_reference(prefix)
        self.block_blob_service.create_container(container_name)
        return container_name

    def _get_random_bytes(self, size):
        rand = random.Random()
        result = bytearray(size)
        for i in range(size):
            result[i] = rand.randint(0, 255)
        return bytes(result)

    def put_encrypted_blob(self):
        container_name = self._create_container()
        block_blob_name = self._get_blob_reference(prefix='block_blob')
        page_blob_name = self._get_blob_reference(prefix='page_blob')
        
        # KeyWrapper implements the key encryption key interface outlined
        # in the create_blob_from_* documentation on each service. Setting
        # this property will tell the service to encrypt the blob. Blob encryption
        # is supported only for uploading whole blobs and only at the time of creation.
        kek = KeyWrapper('key1')
        for service in self.service_dict.values():
            service.key_encryption_key = kek

        self.block_blob_service.create_blob_from_text(container_name, block_blob_name, u'Foo')
        self.page_blob_service.create_blob_from_bytes(container_name, page_blob_name, b'Foo.' * 128)

        # Even when encrypting, uploading large blobs will still automatically 
        # chunk the data and parallelize the upload with max_connections
        # defaulting to 2.
        self.block_blob_service.create_blob_from_bytes(container_name, block_blob_name,
                                        b'Foo' * self.block_blob_service.MAX_SINGLE_PUT_SIZE)

        self.block_blob_service.delete_container(container_name)

    def get_encrypted_blob(self):
        container_name = self._create_container()
        block_blob_name = self._get_blob_reference(prefix='block_blob')
        kek = KeyWrapper('key1')
        self.block_blob_service.key_encryption_key = kek
        data = urandom(13 * self.block_blob_service.MAX_SINGLE_PUT_SIZE + 1)
        self.block_blob_service.create_blob_from_bytes(container_name, block_blob_name, data)

        # Setting the key_resolver_function will tell the service to automatically
        # try to decrypt retrieved blobs. The key_resolver is a function that
        # takes in a key_id and returns a corresponding key_encryption_key.
        key_resolver = KeyResolver()
        key_resolver.put_key(kek)
        self.block_blob_service.key_resolver_function = key_resolver.resolve_key

        # Downloading works as usual with support for decrypting both entire blobs
        # and decrypting range gets.
        blob_full = self.block_blob_service.get_blob_to_bytes(container_name, block_blob_name)
        blob_range = self.block_blob_service.get_blob_to_bytes(container_name, block_blob_name,
                                                start_range=len(data)//2 + 5,
                                                end_range=(3*len(data)//4) + 1)

        self.block_blob_service.delete_container(container_name)

    def get_encrypted_blob_key_encryption_key(self):
        container_name = self._create_container()
        block_blob_name = self._get_blob_reference(prefix='block_blob')
        data = b'Foo'
        kek = KeyWrapper('key1')
        self.block_blob_service.key_encryption_key = kek
        self.block_blob_service.create_blob_from_bytes(container_name, block_blob_name, data)

        # If the key_encryption_key property is set on download, the blobservice
        # will try to decrypt blobs using that key. If both the key_resolver and 
        # key_encryption_key are set, the result of the key_resolver will take precedence
        # and the decryption will fail if that key is not successful.
        self.block_blob_service.key_resolver_function = None
        blob = self.block_blob_service.get_blob_to_bytes(container_name, block_blob_name)

        self.block_blob_service.delete_container(container_name)

    def require_encryption(self):
        self.block_blob_service.key_encryption_key = None
        self.block_blob_service.key_resolver_function = None
        self.block_blob_service.require_encryption = False
        container_name = self._create_container()
        encrypted_blob_name = self._get_blob_reference(prefix='block_blob')
        unencrypted_blob_name = self._get_blob_reference(prefix='unencrypted_blob')
        data = b'Foo'
        self.block_blob_service.create_blob_from_bytes(container_name, unencrypted_blob_name, data)

        # If the require_encryption flag is set, the service object will throw if 
        # there is no encryption policy set on upload.
        self.block_blob_service.require_encryption = True
        try:
            self.block_blob_service.create_blob_from_bytes(container_name, encrypted_blob_name, data)
            raise Exception
        except ValueError:
            pass
        
        # If the require_encryption flag is set, the service object will throw if
        # there is no encryption policy set on download.
        kek = KeyWrapper('key1')
        key_resolver = KeyResolver()
        key_resolver.put_key(kek)
        
        self.block_blob_service.key_encryption_key = kek
        self.block_blob_service.create_blob_from_bytes(container_name, encrypted_blob_name, data)
        
        self.block_blob_service.key_encryption_key = None
        try:
            self.block_blob_service.get_blob_to_bytes(container_name, encrypted_blob_name)
            raise Exception
        except ValueError:
            pass

        # If the require_encryption flag is set, but the retrieved blob is not
        # encrypted, the service object will throw.
        self.block_blob_service.key_resolver_function = key_resolver.resolve_key
        try:
            self.block_blob_service.get_blob_to_bytes(container_name, unencrypted_blob_name)
            raise Exception
        except AzureException:
            pass

        self.block_blob_service.delete_container(container_name)

    def alternate_key_algorithms(self):
        container_name = self._create_container()
        block_blob_name = self._get_blob_reference(prefix='block_blob')

        # The key wrapping algorithm used by the key_encryption_key 
        # is entirely up to the choice of the user. For example,
        # RSA may be used.
        kek = RSAKeyWrapper('key2')
        key_resolver = KeyResolver()
        key_resolver.put_key(kek)
        self.block_blob_service.key_encryption_key = kek
        self.block_blob_service.key_resolver_function = key_resolver.resolve_key

        self.block_blob_service.create_blob_from_bytes(container_name, block_blob_name, b'Foo')
        blob = self.block_blob_service.get_blob_to_bytes(container_name, block_blob_name)

        self.block_blob_service.delete_container(container_name)