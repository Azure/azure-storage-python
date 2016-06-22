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

from azure.common import (
    AzureException,
)
from .._constants import (
    _ENCRYPTION_PROTOCOL_V1,
)
from .._encryption import (
    _encrypt,
    _decrypt,
    _EncryptionData,
    _EncryptionAgent,
    _WrappedContentKey,
)
from json import (
    dumps,
    loads,
    JSONDecodeError,
)
from base64 import(
    b64encode,
    b64decode,
)
from .._error import(
    _ERROR_UNSUPPORTED_ENCRYPTION_VERSION,
    _ERROR_DECRYPTION_FAILURE,
    _ERROR_MESSAGE_NOT_ENCRYPTED
)
from .._common_conversion import (
    _encode_base64,
    _decode_base64_to_bytes
)

def _encrypt_queue_message(message, key_encryption_key):
    '''
    Accepts a plain text message and returns a json-formatted string
    containing the encrypted message and the encryption metadata.
    :param object message:
        The plain text messge to be encrypted.
    :param object key_encryption_key:
        The user-provided key-encryption-key. Must implement the following methods:
        wrap_key(key)--wraps the specified key using an algorithm of the user's choice.
        get_key_wrap_algorithm()--returns the algorithm used to wrap the specified symmetric key.
        get_kid()--returns a string key id for this key-encryption-key.
    :return: A json-formatted string containing the encrypted message and the encryption metadata.
    :rtype: str
    '''
    
    #Queue encoding functions all return unicode strings, and encryption should operate on binary strings
    encryption_result = _encrypt(message.encode('utf-8'), key_encryption_key)

    #Build the dictionary structure
    queue_message = {}
    #Base64 encode the result before casting to a string
    queue_message['EncryptedMessageContents'] = _encode_base64(encryption_result['EncryptedMessageContents'])
    
    wrapped_content_key = {}
    wrapped_content_key['KeyId'] = encryption_result['EncryptionData'].wrapped_content_key.key_id
    wrapped_content_key['EncryptedKey'] = b64encode(encryption_result['EncryptionData']
                                                                .wrapped_content_key.encrypted_key).decode(encoding='utf-8')
    wrapped_content_key['Algorithm'] = encryption_result['EncryptionData'].wrapped_content_key.algorithm

    encryption_agent = {}
    encryption_agent['Protocol'] = encryption_result['EncryptionData'].encryption_agent.protocol
    encryption_agent['EncryptionAlgorithm'] = encryption_result['EncryptionData'].encryption_agent.encryption_algorithm

    encryption_data = {}
    encryption_data['WrappedContentKey'] = wrapped_content_key
    encryption_data['EncryptionAgent'] = encryption_agent
    encryption_data['ContentEncryptionIV'] = b64encode(encryption_result['EncryptionData']
                                                                    .content_encryption_IV).decode(encoding='utf-8')

    queue_message['EncryptionData'] = encryption_data

    return dumps(queue_message)

def _decrypt_queue_message(message, require_encryption, key_encryption_key, resolver):
    '''
    Returns the decrypted message contents from an EncryptedQueueMessage.
    If no encryption metadata is present, will return the unaltered message.
    :param str message:
        The JSON formatted QueueEncryptedMessage contents with all associated metadata.
    :param bool require_encryption:
        If set, will enforce that the retrieved messages are encrypted and decrypt them.
    :param object key_encryption_key:
        The user-provided key-encryption-key. Must implement the following methods:
        unwrap_key(key, algorithm)--returns the unwrapped form of the specified symmetric key using the string-specified algorithm.
        get_kid()--returns a string key id for this key-encryption-key.
    :param function resolver(kid):
        The user-provided key resolver. Uses the kid string to return a key-encryption-key implementing the interface defined above.
    :return: The plain text message from the queue message.
    :rtype: str
    '''

    try:
        message = loads(message)

        if message['EncryptionData']['EncryptionAgent']['Protocol'] == _ENCRYPTION_PROTOCOL_V1:
            #Build data structures from dictionary
            encryption_data = message['EncryptionData']

            wrapped_content_key = encryption_data['WrappedContentKey']
            wrapped_content_key = _WrappedContentKey(wrapped_content_key['Algorithm'],
                                                        b64decode(wrapped_content_key['EncryptedKey'].encode(encoding='utf-8')),
                                                        wrapped_content_key['KeyId'])

            encryption_agent = encryption_data['EncryptionAgent']
            encryption_agent = _EncryptionAgent(encryption_agent['EncryptionAlgorithm'],
                                                encryption_agent['Protocol'])

            encryption_data = _EncryptionData(b64decode(encryption_data['ContentEncryptionIV'].encode(encoding='utf-8')),
                                                encryption_agent,
                                                wrapped_content_key)
            decoded_data = _decode_base64_to_bytes(message['EncryptedMessageContents'])
            return _decrypt(decoded_data, encryption_data, key_encryption_key, resolver).decode('utf-8')
        else:
            raise ValueError(_ERROR_UNSUPPORTED_ENCRYPTION_VERSION)
    except (KeyError, JSONDecodeError) as e:
        #Message was not json formatted and so was not encrypted
        #Or the user provided a json formatted message
        if require_encryption:
            raise ValueError(_ERROR_MESSAGE_NOT_ENCRYPTED)
        else:
            return message
    except:
        raise AzureException(_ERROR_DECRYPTION_FAILURE)