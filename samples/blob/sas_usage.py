# -------------------------------------------------------------------------
# Copyright (c) Microsoft.  All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this blob except in compliance with the License.
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

from azure.storage.blob import (
    BlockBlobService,
    ContainerPermissions,
    BlobPermissions,
    PublicAccess,
)
from azure.storage.common import (
    AccessPolicy,
    ResourceTypes,
    AccountPermissions,
)


class BlobSasSamples():
    def __init__(self, account):
        self.account = account

    def run_all_samples(self):
        self.service = self.account.create_block_blob_service()

        self.container_sas()
        self.blob_sas()
        self.account_sas()

        self.public_access()
        self.container_acl()
        self.sas_with_signed_identifiers()

    def _get_container_reference(self, prefix='container'):
        return '{}{}'.format(prefix, str(uuid.uuid4()).replace('-', ''))

    def _create_container(self, prefix='container'):
        container_name = self._get_container_reference(prefix)
        self.service.create_container(container_name)
        return container_name

    def public_access(self):
        container_name = self._get_container_reference()

        # Create a container with 'Blob' level public access
        self.service.create_container(container_name, public_access=PublicAccess.Blob)
        self.service.create_blob_from_text(container_name, 'blob1', 'hello world')

        # Get the public access level
        acl = self.service.get_container_acl(container_name)
        access = acl.public_access  # 'blob'

        # Show a particular blob can be read with no credentials
        anonymous_service = BlockBlobService(self.account.account_name)
        anonymous_service.get_blob_to_text(container_name, 'blob1')

        # Set the container public access level to 'Container'
        self.service.set_container_acl(container_name, public_access=PublicAccess.Container)

        # Wait 30 seconds for it to take effect
        time.sleep(30)

        # Show blobs can be listed with no credentials
        anonymous_service = BlockBlobService(self.account.account_name)
        anonymous_service.list_blobs(container_name)

        # Turn container public access off by simply not sending it with set_container_acl
        self.service.set_container_acl(container_name)

        # Wait 30 seconds for it to take effect
        time.sleep(30)

        try:
            anonymous_service.get_blob_to_text(container_name, 'blob1')
        except:
            print('Public access is turned off')
            pass

        self.service.delete_container(container_name)

    def container_sas(self):
        container_name = self._create_container()
        self.service.create_blob_from_text(container_name, 'blob1', b'hello world')

        # Access only to the blobs in the given container
        # Read permissions to access blobs
        # Expires in an hour
        token = self.service.generate_container_shared_access_signature(
            container_name,
            ContainerPermissions.READ,
            datetime.utcnow() + timedelta(hours=1),
        )

        # Create a service and use the SAS
        sas_service = BlockBlobService(
            account_name=self.account.account_name,
            sas_token=token,
        )

        blob = sas_service.get_blob_to_text(container_name, 'blob1')
        content = blob.content  # hello world

        self.service.delete_container(container_name)

    def blob_sas(self):
        container_name = self._create_container()
        self.service.create_blob_from_text(container_name, 'blob1', b'hello world')

        # Read access only to this particular blob
        # Expires in an hour
        token = self.service.generate_blob_shared_access_signature(
            container_name,
            'blob1',
            BlobPermissions.READ,
            datetime.utcnow() + timedelta(hours=1),
        )

        # Create a service and use the SAS
        sas_service = BlockBlobService(
            account_name=self.account.account_name,
            sas_token=token,
        )

        blob = sas_service.get_blob_to_text(container_name, 'blob1')
        content = blob.content  # hello world

        self.service.delete_container(container_name)

    def account_sas(self):
        container_name = self._create_container()
        metadata = {'val1': 'foo', 'val2': 'blah'}
        self.service.set_container_metadata(container_name, metadata=metadata)

        # Access to read operations on the containers themselves
        # Expires in an hour
        token = self.service.generate_account_shared_access_signature(
            ResourceTypes.CONTAINER,
            AccountPermissions.READ,
            datetime.utcnow() + timedelta(hours=1),
        )

        # Create a service and use the SAS
        sas_service = BlockBlobService(
            account_name=self.account.account_name,
            sas_token=token,
        )
        metadata = sas_service.get_container_metadata(container_name)  # metadata={'val1': 'foo', 'val2': 'blah'}

        self.service.delete_container(container_name)

    def container_acl(self):
        container_name = self._create_container()

        # Create a READ level access policy and set it on the container
        access_policy = AccessPolicy(permission=ContainerPermissions.READ,
                                     expiry=datetime.utcnow() + timedelta(hours=1))
        identifiers = {'id': access_policy}
        self.service.set_container_acl(container_name, identifiers)

        # Wait 30 seconds for acl to propagate
        time.sleep(30)
        acl = self.service.get_container_acl(container_name)  # {id: AccessPolicy()}

        # Replaces values, does not merge
        access_policy = AccessPolicy(permission=ContainerPermissions.READ,
                                     expiry=datetime.utcnow() + timedelta(hours=1))
        identifiers = {'id2': access_policy}
        self.service.set_container_acl(container_name, identifiers)

        # Wait 30 seconds for acl to propagate
        time.sleep(30)
        acl = self.service.get_container_acl(container_name)  # {id2: AccessPolicy()}

        # Clear
        self.service.set_container_acl(container_name)

        # Wait 30 seconds for acl to propagate
        time.sleep(30)
        acl = self.service.get_container_acl(container_name)  # {}

        self.service.delete_container(container_name)

    def sas_with_signed_identifiers(self):
        container_name = self._create_container()
        self.service.create_blob_from_text(container_name, 'blob1', b'hello world')

        # Set access policy on container
        access_policy = AccessPolicy(permission=ContainerPermissions.READ,
                                     expiry=datetime.utcnow() + timedelta(hours=1))
        identifiers = {'id': access_policy}
        acl = self.service.set_container_acl(container_name, identifiers)

        # Wait 30 seconds for acl to propagate
        time.sleep(30)

        # Indicates to use the access policy set on the container
        token = self.service.generate_container_shared_access_signature(
            container_name,
            id='id'
        )

        # Create a service and use the SAS
        sas_service = BlockBlobService(
            account_name=self.account.account_name,
            sas_token=token,
        )

        blob = sas_service.get_blob_to_text(container_name, 'blob1')
        content = blob.content  # hello world

        self.service.delete_container(container_name)
