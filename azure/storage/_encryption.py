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

from cryptography.hazmat import backends
from cryptography.hazmat.primitives.ciphers.algorithms import AES
from cryptography.hazmat.primitives.ciphers.modes import CBC
from cryptography.hazmat.primitives.padding import PKCS7
from cryptography.hazmat.primitives.ciphers import Cipher
import os
from ._error import (
    _validate_not_none,
    _validate_key_encryption_key_wrap,
    _validate_key_encryption_key_unwrap,
    _validate_encryption_protocol_version,
    _validate_kek_id,
    _ERROR_UNSUPPORTED_ENCRYPTION_ALGORITHM
)
from ._constants import (
    _ENCRYPTION_PROTOCOL_V1,
)
from ._common_conversion import (
    _encode_base64,
    _decode_base64_to_bytes,
)

class _EncryptionAlgorithm(object):
    '''
    Specifies which client encryption algorithm is used.
    '''
    AES_CBC_256 = 'AES_CBC_256'

class _WrappedContentKey:
    '''
    Represents the envelope key details stored on the service.
    '''

    def __init__(self, algorithm, encrypted_key, key_id):
        '''
        :param str algorithm:
            The algorithm used for wrapping.
        :param bytes encrypted_key:
            The encrypted content-encryption-key.
        :param str key_id:
            The key-encryption-key identifier string.
        '''

        _validate_not_none('algorithm', algorithm)
        _validate_not_none('encrypted_key', encrypted_key)
        _validate_not_none('key_id', key_id)

        self.algorithm = algorithm
        self.encrypted_key = encrypted_key
        self.key_id = key_id

class _EncryptionAgent:
    '''
    Represents the encryption agent stored on the service.
    It consists of the encryption protocol version and encryption algorithm used.
    '''

    def __init__(self, encryption_algorithm, protocol):
        '''
        :param _EncryptionAlgorithm encryption_algorithm:
            The algorithm used for encrypting the message contents.
        :param str protocol:
            The protocol version used for encryption.
        '''

        _validate_not_none('encryption_algorithm', encryption_algorithm)
        _validate_not_none('protocol', protocol)

        self.encryption_algorithm = str(encryption_algorithm)
        self.protocol = protocol

class _EncryptionData:
    '''
    Represents the encryption data that is stored on the service.
    '''

    def __init__(self, content_encryption_IV, encryption_agent, wrapped_content_key):
        '''
        :param bytes content_encryption_IV:
            The content encryption initialization vector.
        :param _EncryptionAgent encryption_agent:
            The encryption agent.
        :param _WrappedContentKey wrapped_content_key:
            An object that stores the wrapping algorithm, the key identifier, 
            and the encrypted key bytes.
        '''

        _validate_not_none('content_encryption_IV', content_encryption_IV)
        _validate_not_none('encryption_agent', encryption_agent)
        _validate_not_none('wrapped_content_key', wrapped_content_key)

        self.content_encryption_IV = content_encryption_IV
        self.encryption_agent = encryption_agent
        self.wrapped_content_key = wrapped_content_key

def _encrypt(message, key_encryption_key):
    '''
    Encrypts the given plain text message using AES256 in CBC mode with 128 bit padding.
    Wraps the generated content-encryption-key using the user-provided key-encryption-key (kek). Returns
    related encryption metadata along with the encrypted message.

    :param obj message:
        The plaintext to be encrypted.
    :param object key_encryption_key:
        The user-provided key-encryption-key. Must implement the following methods:
        wrap_key(key)--wraps the specified key using an algorithm of the user's choice.
        get_key_wrap_algorithm()--returns the algorithm used to wrap the specified symmetric key.
        get_kid()--returns a string key id for this key-encryption-key.
    :return: A dictionary containing the encrypted message contents in an 'EncryptedMessageContents'
        field and the encryption data in an 'EncryptionData' field.
    :rtype: dict
    '''

    _validate_not_none('message', message)
    _validate_not_none('key_encryption_key', key_encryption_key)
    _validate_key_encryption_key_wrap(key_encryption_key)

    content_encryption_key = os.urandom(32)
    initialization_vector = os.urandom(16)

    #Create AES cipher in CBC mode
    backend = backends.default_backend()
    algorithm = AES(content_encryption_key)
    mode = CBC(initialization_vector)
    cipher = Cipher(algorithm, mode, backend)

    #PKCS7 with 16 byte blocks ensures compatibility with AES
    padder = PKCS7(128).padder()
    padded_data = padder.update(message) + padder.finalize() #encode converts the string to bytes

    #encrypt the data
    encryptor = cipher.encryptor()
    encrypted_data = encryptor.update(padded_data) + encryptor.finalize()

    #encrypt the cek
    wrapped_cek = key_encryption_key.wrap_key(content_encryption_key)

    #build and return the resultant dictionary
    wrapped_content_key = _WrappedContentKey(key_encryption_key.get_key_wrap_algorithm(), wrapped_cek, key_encryption_key.get_kid())
    encryption_agent = _EncryptionAgent(_EncryptionAlgorithm.AES_CBC_256, _ENCRYPTION_PROTOCOL_V1)
    encryption_data = _EncryptionData(initialization_vector, encryption_agent, wrapped_content_key)

    return {'EncryptedMessageContents':encrypted_data, 'EncryptionData':encryption_data}

def _decrypt(message, encryption_data, key_encryption_key=None, resolver=None):
    '''
    Decrypts the given ciphertext using AES256 in CBC mode with 128 bit padding.
    Unwraps the content-encryption-key using the user-provided or resolved key-encryption-key (kek). Returns the original plaintex.

    :param str message:
        The ciphertext to be decrypted.
    :param _EncryptionData encryption_data:
        The metadata associated with this ciphertext.
    :param object key_encryption_key:
        The user-provided key-encryption-key. Must implement the following methods:
        unwrap_key(key, algorithm)--returns the unwrapped form of the specified symmetric key using the string-specified algorithm.
        get_kid()--returns a string key id for this key-encryption-key.
    :param function resolver(kid):
        The user-provided key resolver. Uses the kid string to return a key-encryption-key implementing the interface defined above.
    :return: The decrypted plaintext.
    :rtype: str
    '''

    _validate_not_none('content_encryption_IV', encryption_data.content_encryption_IV)
    _validate_not_none('encrypted_key', encryption_data.wrapped_content_key.encrypted_key)
    _validate_not_none('message', message)
    
    _validate_encryption_protocol_version(encryption_data.encryption_agent.protocol)

    content_encryption_key = None

    #if the resolver exists, give priority to the key it finds
    if resolver is not None:
        key_encryption_key = resolver(encryption_data.wrapped_content_key.key_id)

    _validate_not_none('key_encryption_key', key_encryption_key)
    _validate_key_encryption_key_unwrap(key_encryption_key)
    _validate_kek_id(encryption_data.wrapped_content_key.key_id, key_encryption_key.get_kid())

    #Will throw an exception if the specified algorithm is not supported
    content_encryption_key = key_encryption_key.unwrap_key(encryption_data.wrapped_content_key.encrypted_key,
                                                           encryption_data.wrapped_content_key.algorithm)
    _validate_not_none('content_encryption_key', content_encryption_key)

    if not ( _EncryptionAlgorithm.AES_CBC_256 == encryption_data.encryption_agent.encryption_algorithm):
        raise ValueError(_ERROR_UNSUPPORTED_ENCRYPTION_ALGORITHM)

    #Create decryption cipher
    backend = backends.default_backend()
    algorithm = AES(content_encryption_key)
    mode = CBC(encryption_data.content_encryption_IV)
    cipher = Cipher(algorithm, mode, backend)

    #decrypt data
    decrypted_data = message
    decryptor = cipher.decryptor()
    decrypted_data = (decryptor.update(decrypted_data) + decryptor.finalize())

    #unpad data
    unpadder = PKCS7(128).unpadder()
    decrypted_data = (unpadder.update(decrypted_data) + unpadder.finalize())

    return decrypted_data
    