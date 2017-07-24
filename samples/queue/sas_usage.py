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
from datetime import datetime, timedelta

from azure.storage.common import (
    AccessPolicy,
    ResourceTypes,
    AccountPermissions,
)
from azure.storage.queue import (
    QueueService,
    QueuePermissions,
)


class QueueSasSamples():
    def __init__(self, account):
        self.account = account

    def run_all_samples(self):
        self.service = self.account.create_queue_service()

        self.queue_sas()
        self.account_sas()

        self.queue_acl()
        self.sas_with_signed_identifiers()

    def _create_queue(self, prefix='queue'):
        queue_name = '{}{}'.format(prefix, str(uuid.uuid4()).replace('-', ''))
        self.service.create_queue(queue_name)
        return queue_name

    def queue_sas(self):
        queue_name = self._create_queue()
        self.service.put_message(queue_name, u'message1')

        # Access only to the messages in the given queue
        # Process permissions to access messages
        # Expires in an hour
        token = self.service.generate_queue_shared_access_signature(
            queue_name,
            QueuePermissions.PROCESS,
            datetime.utcnow() + timedelta(hours=1),
        )

        # Create a service and use the SAS
        sas_service = QueueService(
            account_name=self.account.account_name,
            sas_token=token,
        )

        messages = sas_service.get_messages(queue_name)
        for message in messages:
            print(message.content)  # message1

        self.service.delete_queue(queue_name)

    def account_sas(self):
        queue_name = self._create_queue()
        metadata = {'val1': 'foo', 'val2': 'blah'}
        self.service.set_queue_metadata(queue_name, metadata=metadata)

        # Access to read operations on the queues themselves
        # Expires in an hour
        token = self.service.generate_account_shared_access_signature(
            ResourceTypes.CONTAINER,
            AccountPermissions.READ,
            datetime.utcnow() + timedelta(hours=1),
        )

        # Create a service and use the SAS
        sas_service = QueueService(
            account_name=self.account.account_name,
            sas_token=token,
        )
        metadata = sas_service.get_queue_metadata(queue_name)  # metadata={'val1': 'foo', 'val2': 'blah'}

        self.service.delete_queue(queue_name)

    def queue_acl(self):
        queue_name = self._create_queue()

        # Create a READ level access policy and set it on the queue
        access_policy = AccessPolicy(permission=QueuePermissions.READ,
                                     expiry=datetime.utcnow() + timedelta(hours=1))
        identifiers = {'id': access_policy}
        self.service.set_queue_acl(queue_name, identifiers)

        # Wait 30 seconds for acl to propagate
        time.sleep(30)
        acl = self.service.get_queue_acl(queue_name)  # {id: AccessPolicy()}

        # Replaces values, does not merge
        access_policy = AccessPolicy(permission=QueuePermissions.READ,
                                     expiry=datetime.utcnow() + timedelta(hours=1))
        identifiers = {'id2': access_policy}
        self.service.set_queue_acl(queue_name, identifiers)

        # Wait 30 seconds for acl to propagate
        time.sleep(30)
        acl = self.service.get_queue_acl(queue_name)  # {id2: AccessPolicy()}

        # Clear
        self.service.set_queue_acl(queue_name)

        # Wait 30 seconds for acl to propagate
        time.sleep(30)
        acl = self.service.get_queue_acl(queue_name)  # {}

        self.service.delete_queue(queue_name)

    def sas_with_signed_identifiers(self):
        queue_name = self._create_queue()
        self.service.put_message(queue_name, u'message1')

        # Set access policy on queue
        access_policy = AccessPolicy(permission=QueuePermissions.PROCESS,
                                     expiry=datetime.utcnow() + timedelta(hours=1))
        identifiers = {'id': access_policy}
        acl = self.service.set_queue_acl(queue_name, identifiers)

        # Wait 30 seconds for acl to propagate
        time.sleep(30)

        # Indicates to use the access policy set on the queue
        token = self.service.generate_queue_shared_access_signature(
            queue_name,
            id='id'
        )

        # Create a service and use the SAS
        sas_service = QueueService(
            account_name=self.account.account_name,
            sas_token=token,
        )

        messages = sas_service.get_messages(queue_name)
        for message in messages:
            print(message.content)  # message1

        self.service.delete_queue(queue_name)
