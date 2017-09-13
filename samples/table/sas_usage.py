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
from azure.storage.table import (
    TableService,
    TablePermissions,
)


class TableSasSamples():
    def __init__(self, account):
        self.account = account

    def run_all_samples(self):
        self.service = self.account.create_table_service()

        self.table_sas()
        self.account_sas()

        self.table_acl()
        self.sas_with_signed_identifiers()

    def _create_table(self, prefix='table'):
        table_name = '{}{}'.format(prefix, str(uuid.uuid4()).replace('-', ''))
        self.service.create_table(table_name)
        return table_name

    def table_sas(self):
        table_name = self._create_table()
        entity = {
            'PartitionKey': 'test',
            'RowKey': 'test1',
            'text': 'hello world',
        }
        self.service.insert_entity(table_name, entity)

        # Access only to the entities in the given table
        # Query permissions to access entities
        # Expires in an hour
        token = self.service.generate_table_shared_access_signature(
            table_name,
            TablePermissions.QUERY,
            datetime.utcnow() + timedelta(hours=1),
        )

        # Create a service and use the SAS
        sas_service = TableService(
            account_name=self.account.account_name,
            sas_token=token,
        )

        entities = sas_service.query_entities(table_name)
        for entity in entities:
            print(entity.text)  # hello world

        self.service.delete_table(table_name)

    def account_sas(self):
        table_name = self._create_table()
        entity = {
            'PartitionKey': 'test',
            'RowKey': 'test1',
            'text': 'hello world',
        }
        self.service.insert_entity(table_name, entity)

        # Access to all entities in all the tables
        # Expires in an hour
        token = self.service.generate_account_shared_access_signature(
            ResourceTypes.OBJECT,
            AccountPermissions.READ,
            datetime.utcnow() + timedelta(hours=1),
        )

        # Create a service and use the SAS
        sas_service = TableService(
            account_name=self.account.account_name,
            sas_token=token,
        )

        entities = list(sas_service.query_entities(table_name))
        for entity in entities:
            print(entity.text)  # hello world

        self.service.delete_table(table_name)

    def table_acl(self):
        table_name = self._create_table()

        # Basic
        access_policy = AccessPolicy(permission=TablePermissions.QUERY,
                                     expiry=datetime.utcnow() + timedelta(hours=1))
        identifiers = {'id': access_policy}
        self.service.set_table_acl(table_name, identifiers)

        # Wait 30 seconds for acl to propagate
        time.sleep(30)
        acl = self.service.get_table_acl(table_name)  # {id: AccessPolicy()}

        # Replaces values, does not merge
        access_policy = AccessPolicy(permission=TablePermissions.QUERY,
                                     expiry=datetime.utcnow() + timedelta(hours=1))
        identifiers = {'id2': access_policy}
        self.service.set_table_acl(table_name, identifiers)

        # Wait 30 seconds for acl to propagate
        time.sleep(30)
        acl = self.service.get_table_acl(table_name)  # {id2: AccessPolicy()}

        # Clear
        self.service.set_table_acl(table_name)

        # Wait 30 seconds for acl to propagate
        time.sleep(30)
        acl = self.service.get_table_acl(table_name)  # {}

        self.service.delete_table(table_name)

    def sas_with_signed_identifiers(self):
        table_name = self._create_table()
        entity = {
            'PartitionKey': 'test',
            'RowKey': 'test1',
            'text': 'hello world',
        }
        self.service.insert_entity(table_name, entity)

        # Set access policy on table
        access_policy = AccessPolicy(permission=TablePermissions.QUERY,
                                     expiry=datetime.utcnow() + timedelta(hours=1))
        identifiers = {'id': access_policy}
        acl = self.service.set_table_acl(table_name, identifiers)

        # Wait 30 seconds for acl to propagate
        time.sleep(30)

        # Indicates to use the access policy set on the table
        token = self.service.generate_table_shared_access_signature(
            table_name,
            id='id'
        )

        # Create a service and use the SAS
        sas_service = TableService(
            account_name=self.account.account_name,
            sas_token=token,
        )

        entities = list(sas_service.query_entities(table_name))
        for entity in entities:
            print(entity.text)  # hello world

        self.service.delete_table(table_name)
