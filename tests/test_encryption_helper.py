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