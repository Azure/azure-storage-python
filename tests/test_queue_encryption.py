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
import unittest
from azure.common import (
    AzureHttpError,
    AzureException,
)
from tests.testcase import (
    StorageTestCase,
    record,
)
from azure.storage.queue import (
    QueueService,
)
from tests.test_encryption_helper import (
    KeyWrapper,
    KeyResolver,
    RSAKeyWrapper,
)
from os import urandom
from json import (
    loads,
    dumps,
)
from azure.storage._encryption import (
    _WrappedContentKey,
    _EncryptionAgent,
    _EncryptionData,
)
from base64 import(
    b64encode,
    b64decode,
)
from azure.storage._error import(
    _ERROR_VALUE_NONE,
    _ERROR_OBJECT_INVALID,
    _ERROR_DECRYPTION_FAILURE,
    _ERROR_MESSAGE_NOT_ENCRYPTED,
    _ERROR_ENCRYPTION_REQUIRED,
)
from azure.storage.queue.models import QueueMessageFormat
from cryptography.hazmat import backends
from cryptography.hazmat.primitives.ciphers.algorithms import AES
from cryptography.hazmat.primitives.ciphers.modes import CBC
from cryptography.hazmat.primitives.padding import PKCS7
from cryptography.hazmat.primitives.ciphers import Cipher
from azure.storage._common_conversion import _decode_base64_to_bytes

#------------------------------------------------------------------------------
TEST_QUEUE_PREFIX = 'encryptionqueue'
#------------------------------------------------------------------------------


class StorageQueueEncryptionTest(StorageTestCase):

    def setUp(self):
        super(StorageQueueEncryptionTest, self).setUp()

        self.qs = self._create_storage_service(QueueService, self.settings)

        self.test_queues = []

    def tearDown(self):
        if not self.is_playback():
            for queue_name in self.test_queues:
                try:
                    self.qs.delete_queue(queue_name)
                except:
                    pass
        return super(StorageQueueEncryptionTest, self).tearDown()

    #--Helpers-----------------------------------------------------------------
    def _get_queue_reference(self, prefix=TEST_QUEUE_PREFIX):
        queue_name = self.get_resource_name(prefix)
        self.test_queues.append(queue_name)
        return queue_name

    def _create_queue(self, prefix=TEST_QUEUE_PREFIX):
        queue_name = self._get_queue_reference(prefix)
        self.qs.create_queue(queue_name)
        return queue_name
    #--------------------------------------------------------------------------

    @record
    def test_get_messages_encrypted_kek(self):
        # Arrange
        self.qs.key_encryption_key = KeyWrapper()
        queue_name = self._create_queue()
        self.qs.put_message(queue_name, u'encrypted_message_2')

        # Act
        li = self.qs.get_messages(queue_name)

        # Assert
        self.assertEqual(li[0].content, u'encrypted_message_2')

    @record
    def test_get_messages_encrypted_resolver(self):
        # Arrange
        self.qs.key_encryption_key = KeyWrapper()
        queue_name = self._create_queue()
        self.qs.put_message(queue_name, u'encrypted_message_2')
        key_resolver = KeyResolver()
        key_resolver.put_key(self.qs.key_encryption_key)
        self.qs.key_resolver = key_resolver.resolve_key
        self.qs.key_encryption_key = None #Ensure that the resolver is used

        # Act
        li = self.qs.get_messages(queue_name)

        # Assert
        self.assertEqual(li[0].content, u'encrypted_message_2')

    @record
    def test_peek_messages_encrypted_kek(self): 
        # Arrange
        self.qs.key_encryption_key = KeyWrapper()
        queue_name = self._create_queue()
        self.qs.put_message(queue_name, u'encrypted_message_3')

        # Act
        li = self.qs.peek_messages(queue_name)

        # Assert
        self.assertEqual(li[0].content, u'encrypted_message_3')

    @record
    def test_peek_messages_encrypted_resolver(self):
        # Arrange
        self.qs.key_encryption_key = KeyWrapper()
        queue_name = self._create_queue()
        self.qs.put_message(queue_name, u'encrypted_message_4')
        key_resolver = KeyResolver()
        key_resolver.put_key(self.qs.key_encryption_key)
        self.resolver = key_resolver.resolve_key

        # Act
        li = self.qs.peek_messages(queue_name)

        # Assert
        self.assertEqual(li[0].content, u'encrypted_message_4')
    
    @record
    def test_peek_messages_encrypted_kek_RSA(self):
        # Arrange
        self.qs.key_encryption_key = RSAKeyWrapper()
        queue_name = self._create_queue()
        self.qs.put_message(queue_name, u'encrypted_message_3')

        # Act
        li = self.qs.peek_messages(queue_name)

        # Assert
        self.assertEqual(li[0].content, u'encrypted_message_3')

    @record
    def test_update_encrypted_message(self):
        # Arrange
        queue_name = self._create_queue()
        self.qs.key_encryption_key = KeyWrapper()
        self.qs.put_message(queue_name, u'Update Me')
        list_result1 = self.qs.get_messages(queue_name)

        # Act
        message = self.qs.update_message(queue_name,
                                         list_result1[0].id,
                                         list_result1[0].pop_receipt,
                                         0,
                                         content = u'Updated',)
        list_result2 = self.qs.get_messages(queue_name)

        # Assert
        message = list_result2[0]
        self.assertEqual(u'Updated', message.content)

    @record
    def test_update_encrypted_binary_message(self):
        # Arrange
        queue_name = self._create_queue()
        self.qs.key_encryption_key = KeyWrapper()
        self.qs.encode_function = QueueMessageFormat.binary_base64encode
        self.qs.decode_function = QueueMessageFormat.binary_base64decode
        binary_message = urandom(100)
        self.qs.put_message(queue_name, binary_message)
        list_result1 = self.qs.get_messages(queue_name)

        # Act
        binary_message = urandom(100)
        self.qs.update_message(queue_name,
                               list_result1[0].id,
                               list_result1[0].pop_receipt,
                               0,
                               content = binary_message,)
        list_result2 = self.qs.get_messages(queue_name)

        # Assert
        message = list_result2[0]
        self.assertEqual(binary_message, message.content)

    @record
    def test_update_encrypted_raw_text_message(self):
        # Arrange
        queue_name = self._create_queue()
        self.qs.key_encryption_key = KeyWrapper()
        self.qs.encode_function = QueueMessageFormat.noencode
        self.qs.decode_function = QueueMessageFormat.nodecode
        raw_text = u'Update Me'
        self.qs.put_message(queue_name, raw_text)
        list_result1 = self.qs.get_messages(queue_name)

        # Act
        raw_text = u'Updated'
        self.qs.update_message(queue_name,
                               list_result1[0].id,
                               list_result1[0].pop_receipt,
                               0,
                               content = raw_text,)
        list_result2 = self.qs.get_messages(queue_name)

        # Assert
        message = list_result2[0]
        self.assertEqual(raw_text, message.content)

    @record
    def test_update_encrypted_json_message(self):
        # Arrange
        queue_name = self._create_queue()
        self.qs.key_encryption_key = KeyWrapper()
        self.qs.encode_function = QueueMessageFormat.noencode
        self.qs.decode_function = QueueMessageFormat.nodecode
        message_dict = {'val1': 1, 'val2':'2'}
        json_text = dumps(message_dict)
        self.qs.put_message(queue_name, json_text)
        list_result1 = self.qs.get_messages(queue_name)

        # Act
        message_dict['val1'] = 0
        message_dict['val2'] = 'updated'
        json_text = dumps(message_dict)
        self.qs.update_message(queue_name,
                               list_result1[0].id,
                               list_result1[0].pop_receipt,
                               0,
                               content = json_text,)
        list_result2 = self.qs.get_messages(queue_name)

        # Assert
        message = list_result2[0]
        self.assertEqual(message_dict, loads(message.content))

    @record 
    def test_invalid_value_kek_wrap(self):
        # Arrange
        queue_name = self._create_queue()
        self.qs.key_encryption_key = KeyWrapper()

        self.qs.key_encryption_key.get_kid = None
        try:
            self.qs.put_message(queue_name, u'message')
            self.fail()
        except AttributeError as e:
            self.assertEqual(str(e), _ERROR_OBJECT_INVALID.format('key encryption key', 'get_kid'))

        self.qs.key_encryption_key = KeyWrapper()

        self.qs.key_encryption_key.get_kid = None
        with self.assertRaises(AttributeError):
            self.qs.put_message(queue_name, u'message')

        self.qs.key_encryption_key = KeyWrapper()

        self.qs.key_encryption_key.wrap_key = None
        with self.assertRaises(AttributeError):
            self.qs.put_message(queue_name, u'message')

    @record
    def test_missing_attribute_kek_wrap(self):
        # Arrange
        queue_name = self._create_queue()

        valid_key = KeyWrapper()

        # Act
        invalid_key_1 = lambda: None #functions are objects, so this effectively creates an empty object
        invalid_key_1.get_key_wrap_algorithm = valid_key.get_key_wrap_algorithm
        invalid_key_1.get_kid = valid_key.get_kid
        #No attribute wrap_key
        self.qs.key_encryption_key = invalid_key_1
        with self.assertRaises(AttributeError):
            self.qs.put_message(queue_name, u'message')

        invalid_key_2 = lambda: None #functions are objects, so this effectively creates an empty object
        invalid_key_2.wrap_key = valid_key.wrap_key
        invalid_key_2.get_kid = valid_key.get_kid
        #No attribute get_key_wrap_algorithm
        self.qs.key_encryption_key = invalid_key_2
        with self.assertRaises(AttributeError):
            self.qs.put_message(queue_name, u'message')

        invalid_key_3 = lambda: None #functions are objects, so this effectively creates an empty object
        invalid_key_3.get_key_wrap_algorithm = valid_key.get_key_wrap_algorithm
        invalid_key_3.wrap_key = valid_key.wrap_key
        #No attribute get_kid
        self.qs.key_encryption_key = invalid_key_3
        with self.assertRaises(AttributeError):
            self.qs.put_message(queue_name, u'message')

    @record
    def test_invalid_value_kek_unwrap(self):
        # Arrange
        queue_name = self._create_queue()
        self.qs.key_encryption_key = KeyWrapper()
        self.qs.put_message(queue_name, u'message')

        # Act
        self.qs.key_encryption_key.unwrap_key = None
        with self.assertRaises(AzureException):
            self.qs.peek_messages(queue_name)

        self.qs.key_encryption_key.get_kid = None
        with self.assertRaises(AzureException):
            self.qs.peek_messages(queue_name)

    @record
    def test_missing_attribute_kek_unrwap(self):
        # Arrange
        queue_name = self._create_queue()
        self.qs.key_encryption_key = KeyWrapper()
        self.qs.put_message(queue_name, u'message')

        # Act
        valid_key = KeyWrapper()
        invalid_key_1 = lambda: None #functions are objects, so this effectively creates an empty object
        invalid_key_1.unwrap_key = valid_key.unwrap_key
        #No attribute get_kid
        self.qs.key_encryption_key = invalid_key_1
        try:
            self.qs.peek_messages(queue_name) 
            self.fail()
        except AzureException as e:
            self.assertEqual(str(e),_ERROR_DECRYPTION_FAILURE)

        invalid_key_2 = lambda: None #functions are objects, so this effectively creates an empty object
        invalid_key_2.get_kid = valid_key.get_kid
        #No attribute unwrap_key
        self.qs.key_encryption_key = invalid_key_2
        with self.assertRaises(AzureException):
            self.qs.peek_messages(queue_name) 

    @record
    def test_validate_encryption(self):
        # Arrange
        queue_name = self._create_queue()
        kek = KeyWrapper()
        self.qs.key_encryption_key = kek
        self.qs.put_message(queue_name, u'message')

        # Act
        self.qs.key_encryption_key = None # Message will not be decrypted
        li = self.qs.peek_messages(queue_name)
        message = li[0].content
        message = loads(message)

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
        message = message['EncryptedMessageContents']
        content_encryption_key = kek.unwrap_key(encryption_data.wrapped_content_key.encrypted_key,
                                                           encryption_data.wrapped_content_key.algorithm)

        #Create decryption cipher
        backend = backends.default_backend()
        algorithm = AES(content_encryption_key)
        mode = CBC(encryption_data.content_encryption_IV)
        cipher = Cipher(algorithm, mode, backend)

        #decode and decrypt data
        decrypted_data = _decode_base64_to_bytes(message)
        decryptor = cipher.decryptor()
        decrypted_data = (decryptor.update(decrypted_data) + decryptor.finalize())

        #unpad data
        unpadder = PKCS7(128).unpadder()
        decrypted_data = (unpadder.update(decrypted_data) + unpadder.finalize())

        decrypted_data = decrypted_data.decode(encoding='utf-8')

        # Assert
        self.assertEqual(decrypted_data, u'message')

    @record
    def test_put_with_strict_mode(self):
        # Arrange
        queue_name = self._create_queue()
        kek = KeyWrapper()
        self.qs.key_encryption_key = kek
        self.qs.require_encryption = True

        self.qs.put_message(queue_name, u'message')
        self.qs.key_encryption_key = None

        # Assert
        try:
            self.qs.put_message(queue_name, u'message')
            self.fail()
        except ValueError as e:
            self.assertEqual(str(e), _ERROR_ENCRYPTION_REQUIRED)

    @record
    def test_get_with_strict_mode(self):
        # Arrange
        queue_name = self._create_queue()
        self.qs.put_message(queue_name, u'message')

        self.qs.require_encryption = True
        self.qs.key_encryption_key = KeyWrapper()
        try:
            self.qs.get_messages(queue_name)
        except ValueError as e:
            self.assertEqual(str(e), _ERROR_MESSAGE_NOT_ENCRYPTED)

    @record
    def test_encryption_add_encrypted_64k_message(self):
        # Arrange
        queue_name = self._create_queue()
        message = u'a'*1024*64

        # Act
        self.qs.put_message(queue_name, message)

        # Assert
        self.qs.key_encryption_key = KeyWrapper()
        with self.assertRaises(AzureHttpError):
            self.qs.put_message(queue_name, message)

    @record
    def test_encryption_nonmatching_kid(self):
        # Arrange
        queue_name = self._create_queue()
        self.qs.key_encryption_key = KeyWrapper()
        self.qs.put_message(queue_name, u'message')

        # Act
        self.qs.key_encryption_key.kid = 'Invalid'

        # Assert
        try:
            self.qs.get_messages(queue_name)
            self.fail()
        except AzureException as e:
            self.assertEqual(str(e), _ERROR_DECRYPTION_FAILURE)
#------------------------------------------------------------------------------
if __name__ == '__main__':
    unittest.main()
