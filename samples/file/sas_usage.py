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
from azure.storage.file import (
    FileService,
    SharePermissions,
    FilePermissions,
)


class FileSasSamples():
    def __init__(self, account):
        self.account = account

    def run_all_samples(self):
        self.service = self.account.create_file_service()

        self.share_sas()
        self.file_sas()
        self.account_sas()

        self.share_acl()
        self.sas_with_signed_identifiers()

    def _create_share(self, prefix='share'):
        share_name = '{}{}'.format(prefix, str(uuid.uuid4()).replace('-', ''))
        self.service.create_share(share_name)
        return share_name

    def share_sas(self):
        share_name = self._create_share()
        self.service.create_file_from_text(share_name, None, 'file1', b'hello world')

        # Access only to the files in the given share
        # Read permissions to access files
        # Expires in an hour
        token = self.service.generate_share_shared_access_signature(
            share_name,
            SharePermissions.READ,
            datetime.utcnow() + timedelta(hours=1),
        )

        # Create a service and use the SAS
        sas_service = FileService(
            account_name=self.account.account_name,
            sas_token=token,
        )

        file = sas_service.get_file_to_text(share_name, None, 'file1')
        content = file.content  # hello world

        self.service.delete_share(share_name)

    def file_sas(self):
        share_name = self._create_share()
        self.service.create_directory(share_name, 'dir1')
        self.service.create_file_from_text(share_name, 'dir1', 'file1', b'hello world')

        # Read access only to this particular file
        # Expires in an hour
        token = self.service.generate_file_shared_access_signature(
            share_name,
            'dir1',
            'file1',
            FilePermissions.READ,
            datetime.utcnow() + timedelta(hours=1),
        )

        # Create a service and use the SAS
        sas_service = FileService(
            account_name=self.account.account_name,
            sas_token=token,
        )

        file = sas_service.get_file_to_text(share_name, 'dir1', 'file1')
        content = file.content  # hello world

        self.service.delete_share(share_name)

    def account_sas(self):
        share_name = self._create_share()
        metadata = {'val1': 'foo', 'val2': 'blah'}
        self.service.set_share_metadata(share_name, metadata=metadata)

        # Access to read operations on the shares themselves
        # Expires in an hour
        token = self.service.generate_account_shared_access_signature(
            ResourceTypes.CONTAINER,
            AccountPermissions.READ,
            datetime.utcnow() + timedelta(hours=1),
        )

        # Create a service and use the SAS
        sas_service = FileService(
            account_name=self.account.account_name,
            sas_token=token,
        )
        metadata = sas_service.get_share_metadata(share_name)  # metadata={'val1': 'foo', 'val2': 'blah'}

        self.service.delete_share(share_name)

    def share_acl(self):
        share_name = self._create_share()

        # Basic
        access_policy = AccessPolicy(permission=SharePermissions.READ,
                                     expiry=datetime.utcnow() + timedelta(hours=1))
        identifiers = {'id': access_policy}
        self.service.set_share_acl(share_name, identifiers)

        # Wait 30 seconds for acl to propagate
        time.sleep(30)
        acl = self.service.get_share_acl(share_name)  # {id: AccessPolicy()}

        # Replaces values, does not merge
        access_policy = AccessPolicy(permission=SharePermissions.READ,
                                     expiry=datetime.utcnow() + timedelta(hours=1))
        identifiers = {'id2': access_policy}
        self.service.set_share_acl(share_name, identifiers)

        # Wait 30 seconds for acl to propagate
        time.sleep(30)
        acl = self.service.get_share_acl(share_name)  # {id2: AccessPolicy()}

        # Clear
        self.service.set_share_acl(share_name)

        # Wait 30 seconds for acl to propagate
        time.sleep(30)
        acl = self.service.get_share_acl(share_name)  # {}

        self.service.delete_share(share_name)

    def sas_with_signed_identifiers(self):
        share_name = self._create_share()
        self.service.create_directory(share_name, 'dir1')
        self.service.create_file_from_text(share_name, 'dir1', 'file1', b'hello world')

        # Set access policy on share
        access_policy = AccessPolicy(permission=SharePermissions.READ,
                                     expiry=datetime.utcnow() + timedelta(hours=1))
        identifiers = {'id': access_policy}
        acl = self.service.set_share_acl(share_name, identifiers)

        # Wait 30 seconds for acl to propagate
        time.sleep(30)

        # Indicates to use the access policy set on the share
        token = self.service.generate_share_shared_access_signature(
            share_name,
            id='id'
        )

        # Create a service and use the SAS
        sas_service = FileService(
            account_name=self.account.account_name,
            sas_token=token,
        )

        file = sas_service.get_file_to_text(share_name, 'dir1', 'file1')
        content = file.content  # hello world

        self.service.delete_share(share_name)
