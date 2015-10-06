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

    :ivar message_id: 
        A GUID value assigned to the message by the Queue service that 
        identifies the message in the queue. This value may be used together 
        with the value of pop_receipt to delete a message from the queue after 
        it has been retrieved with the get messages operation. 
    :vartype message_id: str
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
    :ivar message_text: 
        A UTC date value representing the time the messages was inserted.
    :vartype message_text: date
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
        self.message_id = None
        self.insertion_time = None
        self.expiration_time = None
        self.dequeue_count = None
        self.message_text = None
        self.pop_receipt = None
        self.time_next_visible = None


class QueueSharedAccessPermissions(object):
    '''Permissions for a queue.'''

    '''
    Read metadata and properties, including message count.
    Peek at messages.
    '''
    READ = 'r'

    '''Add messages to the queue.'''
    ADD = 'a'

    '''Update messages in the queue.'''
    UPDATE = 'u'

    '''Get and delete messages from the queue.'''
    PROCESS = 'p'
