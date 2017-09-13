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

from azure.storage.blob import (
    Include,
)
from azure.storage.common import (
    Metrics,
    CorsRule,
    Logging,
)


class ContainerSamples():
    def __init__(self, account):
        self.account = account

    def run_all_samples(self):
        self.service = self.account.create_block_blob_service()

        self.create_container()
        self.delete_container()
        self.container_metadata()
        self.container_properties()
        self.container_exists()
        self.lease_container()
        self.list_blobs()

        self.list_containers()

        # This method contains sleeps, so don't run by default
        # self.service_properties()

    def _get_container_reference(self, prefix='container'):
        return '{}{}'.format(prefix, str(uuid.uuid4()).replace('-', ''))

    def _create_container(self, prefix='container'):
        container_name = self._get_container_reference(prefix)
        self.service.create_container(container_name)
        return container_name

    def create_container(self):
        # Basic
        container_name1 = self._get_container_reference()
        created = self.service.create_container(container_name1)  # True

        # Metadata
        metadata = {'val1': 'foo', 'val2': 'blah'}
        container_name2 = self._get_container_reference()
        created = self.service.create_container(container_name2, metadata=metadata)  # True

        # Fail on exist
        container_name3 = self._get_container_reference()
        created = self.service.create_container(container_name3)  # True
        created = self.service.create_container(container_name3)  # False
        try:
            self.service.create_container(container_name3, fail_on_exist=True)
        except AzureConflictHttpError:
            pass

        self.service.delete_container(container_name1)
        self.service.delete_container(container_name2)
        self.service.delete_container(container_name3)

    def delete_container(self):
        # Basic
        container_name = self._create_container()
        deleted = self.service.delete_container(container_name)  # True

        # Fail not exist
        container_name = self._get_container_reference()
        deleted = self.service.delete_container(container_name)  # False
        try:
            self.service.delete_container(container_name, fail_not_exist=True)
        except AzureMissingResourceHttpError:
            pass

    def container_metadata(self):
        container_name = self._create_container()
        metadata = {'val1': 'foo', 'val2': 'blah'}

        # Basic
        self.service.set_container_metadata(container_name, metadata=metadata)
        metadata = self.service.get_container_metadata(container_name)  # metadata={'val1': 'foo', 'val2': 'blah'}

        # Replaces values, does not merge
        metadata = {'new': 'val'}
        self.service.set_container_metadata(container_name, metadata=metadata)
        metadata = self.service.get_container_metadata(container_name)  # metadata={'new': 'val'}

        # Capital letters
        metadata = {'NEW': 'VAL'}
        self.service.set_container_metadata(container_name, metadata=metadata)
        metadata = self.service.get_container_metadata(container_name)  # metadata={'new': 'VAL'}

        # Clearing
        self.service.set_container_metadata(container_name)
        metadata = self.service.get_container_metadata(container_name)  # metadata={}

        self.service.delete_container(container_name)

    def container_properties(self):
        container_name = self._create_container()
        metadata = {'val1': 'foo', 'val2': 'blah'}

        # Note that there are no settable container properties

        # Basic
        container = self.service.get_container_properties(container_name)
        etag = container.properties.etag  # The container etag

        # Metadata
        self.service.set_container_metadata(container_name, metadata=metadata)
        container = self.service.get_container_properties(container_name)
        metadata = container.metadata  # metadata={'val1': 'foo', 'val2': 'blah'}

        self.service.delete_container(container_name)

    def container_exists(self):
        container_name = self._get_container_reference()

        # Basic
        exists = self.service.exists(container_name)  # False
        self.service.create_container(container_name)
        exists = self.service.exists(container_name)  # True

        self.service.delete_container(container_name)

    def lease_container(self):
        container_name1 = self._create_container()
        container_name2 = self._create_container()
        container_name3 = self._create_container()

        # Acquire
        # Defaults to infinite lease
        infinite_lease_id = self.service.acquire_container_lease(container_name1)

        # Acquire
        # Set lease time, may be between 15 and 60 seconds
        fixed_lease_id = self.service.acquire_container_lease(container_name2, lease_duration=30)

        # Acquire
        # Proposed lease id
        proposed_lease_id_1 = '55e97f64-73e8-4390-838d-d9e84a374321'
        modified_lease_id = self.service.acquire_container_lease(container_name3,
                                                                 proposed_lease_id=proposed_lease_id_1,
                                                                 lease_duration=30)
        modified_lease_id  # equal to proposed_lease_id_1

        # Renew
        # Resets the 30 second lease timer
        # Note that the lease may be renewed even if it has expired as long as 
        # the container has not been leased again since the expiration of that lease
        self.service.renew_container_lease(container_name3, proposed_lease_id_1)

        # Change
        # Change the lease ID of an active lease. 
        proposed_lease_id_2 = '55e97f64-73e8-4390-838d-d9e84a374322'
        self.service.change_container_lease(container_name3, modified_lease_id, proposed_lease_id=proposed_lease_id_2)

        # Release
        # Releasing the lease allows another client to immediately acquire the 
        # lease for the container as soon as the release is complete. 
        self.service.release_container_lease(container_name3, proposed_lease_id_2)

        # Break
        # A matching lease ID is not required. 
        # By default, a fixed-duration lease breaks after the remaining lease period 
        # elapses, and an infinite lease breaks immediately.
        infinite_lease_break_time = self.service.break_container_lease(container_name1)
        infinite_lease_break_time  # 0

        # Break
        # By default this would leave whatever time remained of the 30 second 
        # lease period, but a break period can be provided to indicate when the 
        # break should take affect
        lease_break_time = self.service.break_container_lease(container_name2, lease_break_period=10)
        lease_break_time  # 10

        # To clean up we need to immediately break the remaining leases so the containers 
        # can be deleted
        self.service.break_container_lease(container_name2, lease_break_period=0)

        self.service.delete_container(container_name1)
        self.service.delete_container(container_name2)
        self.service.delete_container(container_name3)

    def list_blobs(self):
        container_name = self._create_container()

        self.service.create_blob_from_bytes(container_name, 'blob1', b'')
        self.service.create_blob_from_bytes(container_name, 'blob2', b'')
        self.service.create_blob_from_bytes(container_name, 'dir1/blob1', b'')

        # Basic
        # List from root
        blobs = list(self.service.list_blobs(container_name))
        print('Basic List:')
        for blob in blobs:
            print(blob.name)  # blob1, blob2, dir1/blob1

        # Prefix
        blobs = list(self.service.list_blobs(container_name, prefix='blob'))
        print('Prefix List:')
        for blob in blobs:
            print(blob.name)  # blob1, blob2

        # Num results
        blobs = list(self.service.list_blobs(container_name, num_results=2))
        print('Num Results List:')
        for blob in blobs:
            print(blob.name)  # blob1, blob2

        # Virtual 'directories' w/ delimiter
        blobs_and_dirs = list(self.service.list_blobs(container_name, delimiter='/'))
        print('Delimiter List:')
        for res in blobs_and_dirs:
            print(res.name)  # dir1/, blob1, blob2

        # Metadata
        self.service.set_blob_metadata(container_name, 'blob1', {'val1': 'foo', 'val2': 'blah'})
        blobs = list(self.service.list_blobs(container_name, include=Include.METADATA))
        blob = next((b for b in blobs if b.name == 'blob1'), None)
        metadata = blob.metadata  # {'val1': 'foo', 'val2': 'blah'}

        # Snapshot
        self.service.snapshot_blob(container_name, 'blob1')
        blobs = list(self.service.list_blobs(container_name, include=Include.SNAPSHOTS))
        print('Snapshot List:')
        for blob in blobs:
            print(blob.name + (
            '-' + blob.snapshot) if blob.snapshot else '')  # blob1, blob1-{snapshot_time}, blob2, dir1/blob1

        # Copy
        source_blob_url = self.service.make_blob_url(container_name, 'blob1')
        self.service.copy_blob(container_name, 'copyblob', source_blob_url)
        blobs = list(self.service.list_blobs(container_name, include=Include.COPY))
        copy_blob = next((b for b in blobs if b.name == 'copyblob'), None)
        copy_source = copy_blob.properties.copy.source  # copy properties, including source will be populated

        # Uncommitted
        self.service.put_block(container_name, 'uncommittedblob', b'block data', 1)
        blobs = list(self.service.list_blobs(container_name, include=Include.UNCOMMITTED_BLOBS))
        uncommitted_blob = next((b for b in blobs if b.name == 'uncommittedblob'), None)  # will exist

        # Multiple includes
        blobs = list(self.service.list_blobs(container_name, include=Include(uncommitted_blobs=True, snapshots=True)))
        print('Multiple Includes List:')
        for blob in blobs:
            print(blob.name)  # blob1, blob1, blob2, copyblob, dir1/blob1, uncommittedblob

    def list_containers(self):
        container_name1 = self._get_container_reference()
        self.service.create_container('container1', metadata={'val1': 'foo', 'val2': 'blah'})

        container_name2 = self._create_container('container2')
        container_name3 = self._create_container('thirdcontainer')

        # Basic
        # Commented out as this will list every container in your account
        # containers = list(self.service.list_containers())
        # print('Basic List:')
        # for container in containers:
        #    print(container.name) # container1, container2, thirdq, all other containers created in the service        

        # Num results
        # Will return in alphabetical order. 
        containers = list(self.service.list_containers(num_results=2))
        print('Num Results List:')
        for container in containers:
            print(
                container.name)  # container1, container2, or whichever 2 queues are alphabetically first in your account

        # Prefix
        containers = list(self.service.list_containers(prefix='container'))
        print('Prefix List:')
        for container in containers:
            print(container.name)  # container1, container2,  and any other containers in your account with this prefix

        # Metadata
        containers = list(self.service.list_containers(prefix='container', include_metadata=True))
        container = next((q for q in containers if q.name == 'container1'), None)
        metadata = container.metadata  # {'val1': 'foo', 'val2': 'blah'}

        self.service.delete_container(container_name1)
        self.service.delete_container(container_name2)
        self.service.delete_container(container_name3)

    def service_properties(self):
        # Basic
        self.service.set_blob_service_properties(logging=Logging(delete=True),
                                                 hour_metrics=Metrics(enabled=True, include_apis=True),
                                                 minute_metrics=Metrics(enabled=True, include_apis=False),
                                                 cors=[CorsRule(allowed_origins=['*'], allowed_methods=['GET'])])

        # Wait 30 seconds for settings to propagate
        time.sleep(30)

        props = self.service.get_blob_service_properties()  # props = ServiceProperties() w/ all properties specified above

        # Omitted properties will not overwrite what's already on the self.service
        # Empty properties will clear
        self.service.set_blob_service_properties(cors=[])

        # Wait 30 seconds for settings to propagate
        time.sleep(30)

        props = self.service.get_blob_service_properties()  # props = ServiceProperties() w/ CORS rules cleared
