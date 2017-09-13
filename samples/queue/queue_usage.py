# -------------------------------------------------------------------------
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
# --------------------------------------------------------------------------
import time
import uuid

from azure.common import (
    AzureConflictHttpError,
    AzureMissingResourceHttpError,
)

from azure.storage.common import (
    Logging,
    Metrics,
    CorsRule,
)
from azure.storage.queue import (
    QueueMessageFormat,
)


class QueueSamples():
    def __init__(self, account):
        self.account = account

    def run_all_samples(self):
        self.service = self.account.create_queue_service()

        self.create_queue()
        self.delete_queue()
        self.exists()
        self.metadata()

        self.put_message()
        self.get_messages()
        self.peek_messages()
        self.clear_messages()
        self.delete_message()
        self.update_message()

        self.list_queues()
        self.alternative_encoding()

        # This method contains sleeps, so don't run by default
        # self.service_properties()

    def _get_queue_reference(self, prefix='queue'):
        queue_name = '{}{}'.format(prefix, str(uuid.uuid4()).replace('-', ''))
        return queue_name

    def _create_queue(self, prefix='queue'):
        queue_name = self._get_queue_reference(prefix)
        self.service.create_queue(queue_name)
        return queue_name

    def create_queue(self):
        # Basic
        queue_name1 = self._get_queue_reference()
        created = self.service.create_queue(queue_name1)  # True

        # Metadata
        metadata = {'val1': 'foo', 'val2': 'blah'}
        queue_name2 = self._get_queue_reference()
        created = self.service.create_queue(queue_name2, metadata=metadata)  # True

        # Fail on exist
        queue_name3 = self._get_queue_reference()
        created = self.service.create_queue(queue_name3)  # True
        created = self.service.create_queue(queue_name3)  # False
        try:
            self.service.create_queue(queue_name3, fail_on_exist=True)
        except AzureConflictHttpError:
            pass

        self.service.delete_queue(queue_name1)
        self.service.delete_queue(queue_name2)
        self.service.delete_queue(queue_name3)

    def delete_queue(self):
        # Basic
        queue_name = self._create_queue()
        deleted = self.service.delete_queue(queue_name)  # True

        # Fail not exist
        queue_name = self._get_queue_reference()
        deleted = self.service.delete_queue(queue_name)  # False
        try:
            self.service.delete_queue(queue_name, fail_not_exist=True)
        except AzureMissingResourceHttpError:
            pass

    def exists(self):
        queue_name = self._get_queue_reference()

        # Does not exist
        exists = self.service.exists(queue_name)  # False

        # Exists
        self.service.create_queue(queue_name)
        exists = self.service.exists(queue_name)  # True

        self.service.delete_queue(queue_name)

    def metadata(self):
        queue_name = self._create_queue()
        metadata = {'val1': 'foo', 'val2': 'blah'}

        # Basic
        self.service.set_queue_metadata(queue_name, metadata=metadata)
        metadata = self.service.get_queue_metadata(queue_name)  # metadata={'val1': 'foo', 'val2': 'blah'}

        approximate_message_count = metadata.approximate_message_count  # approximate_message_count = 0

        # Replaces values, does not merge
        metadata = {'new': 'val'}
        self.service.set_queue_metadata(queue_name, metadata=metadata)
        metadata = self.service.get_queue_metadata(queue_name)  # metadata={'new': 'val'}

        # Capital letters
        metadata = {'NEW': 'VAL'}
        self.service.set_queue_metadata(queue_name, metadata=metadata)
        metadata = self.service.get_queue_metadata(queue_name)  # metadata={'new': 'VAL'}

        # Clearing
        self.service.set_queue_metadata(queue_name)
        metadata = self.service.get_queue_metadata(queue_name)  # metadata={}

        self.service.delete_queue(queue_name)

    def put_message(self):
        queue_name = self._create_queue()

        # Basic
        # immediately visibile and expires in 7 days
        self.service.put_message(queue_name, u'message1')

        # Visbility timeout
        # visible in 5 seconds and expires in 7 days
        self.service.put_message(queue_name, u'message2', visibility_timeout=5)

        # Time to live
        # immediately visibile and expires in 60 seconds
        self.service.put_message(queue_name, u'message3', time_to_live=60)

        self.service.delete_queue(queue_name)

    def get_messages(self):
        queue_name = self._create_queue()
        self.service.put_message(queue_name, u'message1')
        self.service.put_message(queue_name, u'message2')
        self.service.put_message(queue_name, u'message3')
        self.service.put_message(queue_name, u'message4')

        # Azure queues are not strictly ordered so the below messages returned are estimates
        # Ex: We may return message2 for the first sample and then message1 and message3 for the second

        # Basic, only gets 1 message
        messages = self.service.get_messages(queue_name)
        for message in messages:
            print(message.content)  # message1

        # Num messages
        messages = self.service.get_messages(queue_name, num_messages=2)
        for message in messages:
            print(message.content)  # message2, message3

        # Visibility
        messages = self.service.get_messages(queue_name, visibility_timeout=10)
        for message in messages:
            print(message.content)  # message4
        # message4 has a visibility timeout of only 10 seconds rather than 30

        self.service.delete_queue(queue_name)

    def peek_messages(self):
        queue_name = self._create_queue()
        self.service.put_message(queue_name, u'message1')
        self.service.put_message(queue_name, u'message2')

        # Azure queues are not strictly ordered so the below messages returned are estimates
        # Ex: We may return message2 for the first sample and then message1 and message2 for the second

        # Basic
        # does not change the visibility timeout
        # does not return pop_receipt, or time_next_visible
        messages = self.service.peek_messages(queue_name)
        for message in messages:
            print(message.content)  # message1

        # Num messages
        messages = self.service.get_messages(queue_name, num_messages=2)
        for message in messages:
            print(message.content)  # message1, message2

        self.service.delete_queue(queue_name)

    def clear_messages(self):
        queue_name = self._create_queue()
        self.service.put_message(queue_name, u'message1')
        self.service.put_message(queue_name, u'message2')

        # Basic
        self.service.clear_messages(queue_name)
        messages = self.service.peek_messages(queue_name)  # messages = []

        self.service.delete_queue(queue_name)

    def delete_message(self):
        queue_name = self._create_queue()
        self.service.put_message(queue_name, u'message1')
        self.service.put_message(queue_name, u'message2')
        messages = self.service.get_messages(queue_name)

        # Basic
        # Deleting requires the message id and pop receipt (returned by get_messages)
        self.service.delete_message(queue_name, messages[0].id, messages[0].pop_receipt)

        messages = self.service.peek_messages(queue_name)
        for message in messages:
            print(message.content)  # either message1 or message 2

        self.service.delete_queue(queue_name)

    def update_message(self):
        queue_name = self._create_queue()
        self.service.put_message(queue_name, u'message1')
        messages = self.service.get_messages(queue_name)

        # Basic
        # Must update visibility timeout, but can use 0
        # updates the visibility timeout and returns pop_receipt and time_next_visible
        message = self.service.update_message(queue_name,
                                              messages[0].id,
                                              messages[0].pop_receipt,
                                              0)

        # With Content
        # Use pop_receipt from previous update
        # message will appear in 30 seconds with the new content
        message = self.service.update_message(queue_name,
                                              messages[0].id,
                                              message.pop_receipt,
                                              30,
                                              content=u'new text')

        self.service.delete_queue(queue_name)

    def list_queues(self):
        queue_name1 = self._get_queue_reference()
        self.service.create_queue('queue1', metadata={'val1': 'foo', 'val2': 'blah'})

        queue_name2 = self._create_queue('queue2')
        queue_name3 = self._create_queue('thirdq')

        # Basic
        # Commented out as this will list every queue in your account
        # queues = list(self.service.list_queues())
        # for queue in queues:
        #    print(queue.name) # queue1, queue2, thirdq, all other queues created in the self.service        

        # Num results
        # Will return in alphabetical order. 
        queues = list(self.service.list_queues(num_results=2))
        for queue in queues:
            print(queue.name)  # queue1, queue2, or whichever 2 queues are alphabetically first in your account

        # Prefix
        queues = list(self.service.list_queues(prefix='queue'))
        for queue in queues:
            print(queue.name)  # queue1, queue2, and any other queues in your account with this prefix

        # Metadata
        queues = list(self.service.list_queues(prefix='queue', include_metadata=True))
        queue = next((q for q in queues if q.name == 'queue1'), None)
        metadata = queue.metadata  # {'val1': 'foo', 'val2': 'blah'}

        self.service.delete_queue(queue_name1)
        self.service.delete_queue(queue_name2)
        self.service.delete_queue(queue_name3)

    def alternative_encoding(self):
        queue_name = self._create_queue()

        # set encoding/decoding to base64 with byte strings
        # default encoding is xml encoded/decoded unicode strings
        # base64 encoding is used in some other storage libraries, use for compatibility
        self.service.encode_function = QueueMessageFormat.binary_base64encode
        self.service.decode_function = QueueMessageFormat.binary_base64decode

        content = b'bytedata'
        self.service.put_message(queue_name, content)

        messages = self.service.peek_messages(queue_name)
        for message in messages:
            print(message.content)  # b'bytedata'

        self.service.delete_queue(queue_name)

    def service_properties(self):
        # Basic
        self.service.set_queue_service_properties(logging=Logging(delete=True),
                                                  hour_metrics=Metrics(enabled=True, include_apis=True),
                                                  minute_metrics=Metrics(enabled=True, include_apis=False),
                                                  cors=[CorsRule(allowed_origins=['*'], allowed_methods=['GET'])])

        # Wait 30 seconds for settings to propagate
        time.sleep(30)

        props = self.service.get_queue_service_properties()  # props = ServiceProperties() w/ all properties specified above

        # Omitted properties will not overwrite what's already on the self.service
        # Empty properties will clear
        self.service.set_queue_service_properties(cors=[])

        # Wait 30 seconds for settings to propagate
        time.sleep(30)

        props = self.service.get_queue_service_properties()  # props = ServiceProperties() w/ CORS rules cleared
