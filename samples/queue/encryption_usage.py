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

class KeyWrapper:
    def __init__(self, kid='key1'):
        self.kek = urandom(32) 
        self.backend = default_backend()
        self.kid = kid
    def wrap_key(self, key):
        return aes_key_wrap(self.kek, key, self.backend)
    def unwrap_key(self, key, algorithm):
        return aes_key_unwrap(self.kek, key, self.backend)
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
    def __init__(self, kid='key2'):
        self.private_key = generate_private_key(public_exponent = 65537,
                                                key_size = 2048,
                                                backend = default_backend())
        self.public_key = self.private_key.public_key()
        self.kid = kid
    def wrap_key(self, key):
        return self.public_key.encrypt(key,
                                 OAEP(
                                     mgf = MGF1(algorithm=SHA1()),
                                     algorithm=SHA1(),
                                     label=None)
                                 )
    def unwrap_key(self, key, algorithm):
        return self.private_key.decrypt(key,
                                    OAEP(
                                        mgf=MGF1(algorithm=SHA1()),
                                        algorithm=SHA1(),
                                        label=None)
                                    )
    def get_key_wrap_algorithm(self):
        return 'RSA'
    def get_kid(self):
        return self.kid

class QueueEncryptionSamples():

    def __init__(self, account):
        self.account = account

    def run_all_samples(self):
        self.service = self.account.create_queue_service()

        self.put_encrypted_message()
        self.peek_get_update_encrypted()
        self.decrypt_with_resolver()
        self.require_encryption()
        self.alternate_key_algorithms()

    def _get_queue_reference(self, prefix='queue'):
        queue_name = '{}{}'.format(prefix, str(uuid.uuid4()).replace('-', ''))
        return queue_name

    def _create_queue(self, prefix='queue'):
        queue_name = self._get_queue_reference(prefix)
        self.service.create_queue(queue_name)
        return queue_name

    def put_encrypted_message(self):
        queue_name = self._create_queue()

        #KeyWrapper implements the key encryption key interface
        #outlined in the get/update message documentation.
        #Setting the key_encryption_key property will tell these
        #APIs to encrypt messages.
        self.service.key_encryption_key = KeyWrapper()
        self.service.put_message(queue_name, 'message1')

        self.service.delete_queue(queue_name)

    def peek_get_update_encrypted(self):
        queue_name = self._create_queue()

        # The KeyWrapper is still needed for encryption
        self.service.key_encryption_key = KeyWrapper()
        self.service.put_message(queue_name, 'message1')

        # KeyResolver is used to resolve a key from its id.
        # Its interface is defined in the get/peek messages documentation.
        self.service.key_resolver = KeyResolver()
        self.service.key_resolver.put_key(self.service.key_encryption_key)
        self.service.peek_messages(queue_name)
        messages = self.service.get_messages(queue_name)
        self.service.update_message(queue_name,
                                    messages[0].id,
                                    messages[0].pop_receipt,
                                    0,
                                    content='encrypted_message2')

        self.service.delete_queue(queue_name)

    def require_encryption(self):
        queue_name = self._create_queue()
        
        self.service.put_message(queue_name,'Not encrypted')
        #Set the require_encryption property on the service to 
        #ensure all messages sent/received are encrypted.
        self.service.require_encryption = True

        #If the property is set, but no kek is specified upon 
        #upload, the method will throw.
        try:
            self.service.put_message(queue_name, 'message1')
        except:
            pass

        self.service.key_encryption_key = KeyWrapper()
        self.service.key_resolver = KeyResolver()
        self.service.key_resolver.put_key(self.service.key_encryption_key)

        #If encryption is required, but a retrieved message is not
        #encrypted, the method will throw.
        try:
            self.service.peek_message(queue_name, 'message1')
        except:
            pass

        self.service.delete_queue(queue_name)

    def alternate_key_algorithms(self):
        queue_name = self._create_queue()

        #To use an alternate method of key wrapping, simply set the 
        #key_encryption_key property to a wrapper that uses a different algorithm.
        self.service.key_encryption_key = RSAKeyWrapper()
        self.service.key_resolver = None

        self.service.put_message(queue_name, 'message')

        self.service.key_resolver = KeyResolver()
        self.service.key_resolver.put_key(self.service.key_encryption_key)
        message = self.service.peek_messages(queue_name)

        self.service.delete_queue(queue_name)

    def decrypt_with_key_encryption_key(self):
        queue_name = self._create_queue()

        # The KeyWrapper object also defines methods necessary for
        # decryption as defined in the get/peek messages documentation. 
        # Since the key_encryption_key property is still set, messages
        # will be decrypted automatically.
        kek = KeyWrapper()
        self.service.key_encryption_key = kek
        self.service.put_message(queue_name, 'message1')

        #When decrypting, if both a kek and resolver are set,
        #the resolver will take precedence. Remove the resolver to just use the kek.
        self.service.key_resolver = None
        messages = self.service.peek_messages(queue_name)

        self.service.delete_queue(queue_name)