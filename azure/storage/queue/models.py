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
from xml.sax.saxutils import escape as xml_escape
from xml.sax.saxutils import unescape as xml_unescape
from base64 import (
    b64encode,
    b64decode,
)
from ._error import (
    _validate_message_type_bytes,
    _validate_message_type_text,
    _ERROR_MESSAGE_NOT_BASE64,
)

class Queue(object):

    ''' 
    Queue class.
     
    :ivar name: 
        The name of the queue.
    :vartype name: str
    :ivar metadata: 
        A dict containing name-value pairs associated with the queue as metadata.
        This var is set to None unless the include=metadata param was included 
        for the list queues operation. If this parameter was specified but the 
        queue has no metadata, metadata will be set to an empty dictionary.
    :vartype metadata: dict
    '''

    def __init__(self):
        self.name = None
        self.metadata = None


class QueueMessage(object):

    ''' 
    Queue message class. 

    :ivar id: 
        A GUID value assigned to the message by the Queue service that 
        identifies the message in the queue. This value may be used together 
        with the value of pop_receipt to delete a message from the queue after 
        it has been retrieved with the get messages operation. 
    :vartype id: str
    :ivar insertion_time: 
        A UTC date value representing the time the messages was inserted.
    :vartype insertion_time: date
    :ivar expiration_time: 
        A UTC date value representing the time the message expires.
    :vartype expiration_time: date
    :ivar dequeue_count: 
        Begins with a value of 1 the first time the message is dequeued. This 
        value is incremented each time the message is subsequently dequeued.
    :vartype dequeue_count: int
    :ivar content: 
        The message content. Type is determined by the decode_function set on 
        the service. Default is str.
    :vartype message: obj
    :ivar pop_receipt: 
        A receipt str which can be used together with the message_id element to 
        delete a message from the queue after it has been retrieved with the get 
        messages operation. Only returned by get messages operations. Set to 
        None for peek messages.
    :vartype pop_receipt: str
    :ivar time_next_visible: 
        A UTC date value representing the time the message will next be visible. 
        Only returned by get messages operations. Set to None for peek messages.
    :vartype time_next_visible: date
    '''

    def __init__(self):
        self.id = None
        self.insertion_time = None
        self.expiration_time = None
        self.dequeue_count = None
        self.content = None
        self.pop_receipt = None
        self.time_next_visible = None


class QueueMessageFormat:
    ''' 
    Encoding and decoding methods which can be used to modify how the queue service 
    encodes and decodes queue messages. Set these to queueservice.encode_function 
    and queueservice.decode_function to modify the behavior. The defaults are 
    text_xmlencode and text_xmldecode, respectively.
    '''

    @staticmethod
    def text_base64encode(data):
        _validate_message_type_text(data)
        return b64encode(data.encode('utf-8')).decode('utf-8')
     
    @staticmethod
    def text_base64decode(data):    
        try:
            return b64decode(data.encode('utf-8')).decode('utf-8')
        except (ValueError, TypeError):
            # ValueError for Python 3, TypeError for Python 2
            raise ValueError(_ERROR_MESSAGE_NOT_BASE64)

    @staticmethod
    def binary_base64encode(data):
        _validate_message_type_bytes(data)
        return b64encode(data).decode('utf-8')
     
    @staticmethod
    def binary_base64decode(data):
        try:
            return b64decode(data.encode('utf-8'))
        except (ValueError, TypeError):
            # ValueError for Python 3, TypeError for Python 2
            raise ValueError(_ERROR_MESSAGE_NOT_BASE64)

    @staticmethod
    def text_xmlencode(data):
        _validate_message_type_text(data)
        return xml_escape(data)
       
    @staticmethod 
    def text_xmldecode(data):
        return xml_unescape(data)

    @staticmethod
    def noencode(data):
        return data
        
    @staticmethod
    def nodecode(data):
        return data


class QueuePermissions(object):

    '''
    QueuePermissions class to be used with `azure.storage.queue.QueueService.generate_queue_shared_access_signature`
    method and for the AccessPolicies used with `azure.storage.queue.QueueService.set_queue_acl`. 

    :param bool read:
        Read metadata and properties, including message count. Peek at messages.
    :param bool add:
        Add messages to the queue.
    :param bool update:
        Update messages in the queue. Note: Use the Process permission with 
        Update so you can first get the message you want to update.
    :param bool process: 
        Get and delete messages from the queue.
    :param str _str: 
        A string representing the permissions.
    '''
    def __init__(self, read=False, add=False, update=False, process=False, _str=None):
        if not _str:
            _str = ''
        self.read = read or ('r' in _str)
        self.add = add or ('a' in _str)
        self.update = update or ('u' in _str)
        self.process = process or ('p' in _str)
    
    def __or__(self, other):
        return QueuePermissions(_str=str(self) + str(other))

    def __add__(self, other):
        return QueuePermissions(_str=str(self) + str(other))
    
    def __str__(self):
        return (('r' if self.read else '') +
                ('a' if self.add else '') +
                ('u' if self.update else '') +
                ('p' if self.process else ''))

''' Read metadata and properties, including message count. Peek at messages. '''
QueuePermissions.READ = QueuePermissions(read=True)

''' Add messages to the queue. '''
QueuePermissions.ADD = QueuePermissions(add=True)

''' 
Update messages in the queue. Note: Use the Process permission with 
Update so you can first get the message you want to update.
'''
QueuePermissions.UPDATE = QueuePermissions(update=True)

''' Get and delete messages from the queue. '''
QueuePermissions.PROCESS = QueuePermissions(process=True)
