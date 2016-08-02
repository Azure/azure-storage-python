# coding: utf-8

#-------------------------------------------------------------------------
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
#--------------------------------------------------------------------------
import uuid
import random
import io
import os
import time

from azure.storage.blob import (
    ContentSettings,
    BlobBlock,
    BlockListType,
)

class BlockBlobSamples():  

    def __init__(self, account):
        self.account = account

    def run_all_samples(self):
        self.service = self.account.create_block_blob_service()

        self.delete_blob()
        self.blob_metadata()   
        self.blob_properties()
        self.blob_exists()
        self.copy_blob()
        self.snapshot_blob()
        self.lease_blob()

        self.blob_with_bytes()
        self.blob_with_stream()
        self.blob_with_path()
        self.blob_with_text()
        
        self.blocks()

    def _get_resource_reference(self, prefix):
        return '{}{}'.format(prefix, str(uuid.uuid4()).replace('-', ''))

    def _get_blob_reference(self, prefix='blob'):
        return self._get_resource_reference(prefix)

    def _create_blob(self, container_name, prefix='blob'):
        blob_name = self._get_resource_reference(prefix)
        self.service.create_blob_from_text(container_name, blob_name, u'hello world')
        return blob_name

    def _create_container(self, prefix='container'):
        container_name = self._get_resource_reference(prefix)
        self.service.create_container(container_name)
        return container_name

    def _get_random_bytes(self, size):
        rand = random.Random()
        result = bytearray(size)
        for i in range(size):
            result[i] = rand.randint(0, 255)
        return bytes(result)

    def delete_blob(self):
        container_name = self._create_container()
        blob_name = self._create_blob(container_name)

        # Basic
        self.service.delete_blob(container_name, blob_name)

        self.service.delete_container(container_name)

    def blob_metadata(self):
        container_name = self._create_container()
        blob_name = self._create_blob(container_name)
        metadata = {'val1': 'foo', 'val2': 'blah'}

        # Basic
        self.service.set_blob_metadata(container_name, blob_name, metadata=metadata)
        metadata = self.service.get_blob_metadata(container_name, blob_name) # metadata={'val1': 'foo', 'val2': 'blah'}

        # Replaces values, does not merge
        metadata = {'new': 'val'}
        self.service.set_blob_metadata(container_name, blob_name, metadata=metadata)
        metadata = self.service.get_blob_metadata(container_name, blob_name) # metadata={'new': 'val'}

        # Capital letters
        metadata = {'NEW': 'VAL'}
        self.service.set_blob_metadata(container_name, blob_name, metadata=metadata)
        metadata = self.service.get_blob_metadata(container_name, blob_name) # metadata={'new': 'VAL'}

        # Clearing
        self.service.set_blob_metadata(container_name, blob_name)
        metadata = self.service.get_blob_metadata(container_name, blob_name) # metadata={}
    
        self.service.delete_container(container_name)

    def blob_properties(self):
        container_name = self._create_container()
        blob_name = self._get_blob_reference()

        metadata = {'val1': 'foo', 'val2': 'blah'}
        self.service.create_blob_from_text(container_name, blob_name, u'hello world', metadata=metadata)

        settings = ContentSettings(content_type='html', content_language='fr')       

        # Basic
        self.service.set_blob_properties(container_name, blob_name, content_settings=settings)
        blob = self.service.get_blob_properties(container_name, blob_name)
        content_language = blob.properties.content_settings.content_language # fr
        content_type = blob.properties.content_settings.content_type # html
        content_length = blob.properties.content_length # 512

        # Metadata
        # Can't set metadata, but get will return metadata already on the blob
        blob = self.service.get_blob_properties(container_name, blob_name)
        metadata = blob.metadata # metadata={'val1': 'foo', 'val2': 'blah'}

        # Replaces values, does not merge
        settings = ContentSettings(content_encoding='utf-8')
        self.service.set_blob_properties(container_name, blob_name, content_settings=settings)
        blob = self.service.get_blob_properties(container_name, blob_name)
        content_encoding = blob.properties.content_settings.content_encoding # utf-8
        content_language = blob.properties.content_settings.content_language # None

        self.service.delete_container(container_name)

    def blob_exists(self):
        container_name = self._create_container()
        blob_name = self._get_blob_reference()

        # Basic
        exists = self.service.exists(container_name, blob_name) # False
        self.service.create_blob_from_text(container_name, blob_name, u'hello world')
        exists = self.service.exists(container_name, blob_name) # True

        self.service.delete_container(container_name)

    def copy_blob(self):
        container_name = self._create_container()
        source_blob_name = self._create_blob(container_name)

        # Basic
        # Copy the blob from the directory to the root of the container
        source = self.service.make_blob_url(container_name, source_blob_name)
        copy = self.service.copy_blob(container_name, 'blob1copy', source)

        # Poll for copy completion
        while copy.status != 'success':
            count = count + 1
            if count > 5:
                print('Timed out waiting for async copy to complete.')
            time.sleep(30)
            copy = self.service.get_blob_properties(container_name, 'blob1copy').properties.copy

        # With SAS from a remote account to local blob
        # Commented out as remote container, directory, blob, and sas would need to be created
        '''
        source_blob_url = self.service.make_blob_url(
            remote_container_name,
            remote_blob_name,
            sas_token=remote_sas_token,
        )
        copy = self.service.copy_blob(destination_containername, 
                                 destination_blob_name, 
                                 source_blob_url)
        '''

        # Abort copy
        # Commented out as this involves timing the abort to be sent while the copy is still running
        # Abort copy is useful to do along with polling
        # self.service.abort_copy_blob(container_name, blob_name, copy.id)

        self.service.delete_container(container_name)

    def snapshot_blob(self):
        container_name = self._create_container()
        base_blob_name = self._create_blob(container_name)

        # Basic
        snapshot_blob = self.service.snapshot_blob(container_name, base_blob_name)
        snapshot_id = snapshot_blob.snapshot

        # Set Metadata (otherwise metadata will be copied from base blob)
        metadata = {'val1': 'foo', 'val2': 'blah'}
        snapshot_blob = self.service.snapshot_blob(container_name, base_blob_name, metadata=metadata)
        snapshot_id = snapshot_blob.snapshot

        self.service.delete_container(container_name)

    def lease_blob(self):
        container_name = self._create_container()
        blob_name1 = self._create_blob(container_name)
        blob_name2 = self._create_blob(container_name)
        blob_name3 = self._create_blob(container_name)

        # Acquire
        # Defaults to infinite lease
        infinite_lease_id = self.service.acquire_blob_lease(container_name, blob_name1)
        
        # Acquire
        # Set lease time, may be between 15 and 60 seconds
        fixed_lease_id = self.service.acquire_blob_lease(container_name, blob_name2, lease_duration=30)

        # Acquire
        # Proposed lease id
        proposed_lease_id_1 = '55e97f64-73e8-4390-838d-d9e84a374321'
        modified_lease_id = self.service.acquire_blob_lease(container_name,
                                                            blob_name3,
                                                            proposed_lease_id=proposed_lease_id_1,
                                                            lease_duration=30)
        modified_lease_id # equal to proposed_lease_id_1

        # Renew
        # Resets the 30 second lease timer
        # Note that the lease may be renewed even if it has expired as long as 
        # the container has not been leased again since the expiration of that lease
        self.service.renew_blob_lease(container_name, blob_name3, proposed_lease_id_1)

        # Change
        # Change the lease ID of an active lease. 
        proposed_lease_id_2 = '55e97f64-73e8-4390-838d-d9e84a374322'
        self.service.change_blob_lease(container_name, blob_name3, modified_lease_id, 
                                       proposed_lease_id=proposed_lease_id_2)

        # Release
        # Releasing the lease allows another client to immediately acquire the 
        # lease for the container as soon as the release is complete. 
        self.service.release_blob_lease(container_name, blob_name3, proposed_lease_id_2)

        # Break
        # A matching lease ID is not required. 
        # By default, a fixed-duration lease breaks after the remaining lease period 
        # elapses, and an infinite lease breaks immediately.
        infinite_lease_break_time = self.service.break_blob_lease(container_name, blob_name1)
        infinite_lease_break_time # 0

        # Break
        # By default this would leave whatever time remained of the 30 second 
        # lease period, but a break period can be provided to indicate when the 
        # break should take affect
        lease_break_time = self.service.break_blob_lease(container_name, blob_name2, lease_break_period=10)
        lease_break_time # 10

        self.service.delete_container(container_name)

    def blob_with_bytes(self):
        container_name = self._create_container()

        # Basic
        data = b'hello world'
        blob_name = self._get_blob_reference()
        self.service.create_blob_from_bytes(container_name, blob_name, data)
        blob = self.service.get_blob_to_bytes(container_name, blob_name)
        content = blob.content # hello world

        # Download range
        blob = self.service.get_blob_to_bytes(container_name, blob_name,
                                              start_range=3, end_range=10)
        content = blob.content # data from 3-10

        # Upload from index in byte array
        blob_name = self._get_blob_reference()
        self.service.create_blob_from_bytes(container_name, blob_name, data, index=3)

        # Content settings, metadata
        settings = ContentSettings(content_type='html', content_language='fr')   
        metadata={'val1': 'foo', 'val2': 'blah'}
        blob_name = self._get_blob_reference()
        self.service.create_blob_from_bytes(container_name, blob_name, data, 
                                       content_settings=settings,
                                       metadata=metadata)
        blob = self.service.get_blob_to_bytes(container_name, blob_name)
        metadata = blob.metadata # metadata={'val1': 'foo', 'val2': 'blah'}
        content_language = blob.properties.content_settings.content_language # fr
        content_type = blob.properties.content_settings.content_type # html

        # Progress
        # Use slightly larger data so the chunking is more visible
        data = self._get_random_bytes(8 * 1024 *1024)
        def upload_callback(current, total):
            print('({}, {})'.format(current, total))
        def download_callback(current, total):
            print('({}, {}) '.format(current, total))
        blob_name = self._get_blob_reference()

        print('upload: ')
        self.service.create_blob_from_bytes(container_name, blob_name, data, 
                                       progress_callback=upload_callback)

        print('download: ')
        blob = self.service.get_blob_to_bytes(container_name, blob_name, 
                                         progress_callback=download_callback)

        self.service.delete_container(container_name)

    def blob_with_stream(self):
        container_name = self._create_container()

        # Basic
        input_stream = io.BytesIO(self._get_random_bytes(15))
        output_stream = io.BytesIO()
        blob_name = self._get_blob_reference()
        self.service.create_blob_from_stream(container_name, blob_name, 
                                             input_stream, 15)
        blob = self.service.get_blob_to_stream(container_name, blob_name, 
                                          output_stream)
        content_length = blob.properties.content_length

        # Download range
        # Content settings, metadata
        # Progress
        # Parallelism
        # See blob_with_bytes for these examples. The code will be very similar.

        self.service.delete_container(container_name)

    def blob_with_path(self):
        container_name = self._create_container()
        INPUT_FILE_PATH = 'blob_input.temp.dat'
        OUTPUT_FILE_PATH = 'blob_output.temp.dat'

        data = self._get_random_bytes(4 * 1024)
        with open(INPUT_FILE_PATH, 'wb') as stream:
            stream.write(data)

        # Basic
        blob_name = self._get_blob_reference()
        self.service.create_blob_from_path(container_name, blob_name, INPUT_FILE_PATH)
        blob = self.service.get_blob_to_path(container_name, blob_name, OUTPUT_FILE_PATH)
        content_length = blob.properties.content_length

        # Open mode
        # Append to the blob instead of starting from the beginning
        # Append streams are not seekable and so must be downloaded serially by setting max_connections=1.
        blob = self.service.get_blob_to_path(container_name, blob_name, OUTPUT_FILE_PATH, open_mode='ab',
                                             max_connections=1)
        content_length = blob.properties.content_length # will be the same, but local blob length will be longer

        # Download range
        # Content settings, metadata
        # Progress
        # Parallelism
        # See blob_with_bytes for these examples. The code will be very similar.

        self.service.delete_container(container_name)

        if os.path.isfile(INPUT_FILE_PATH):
            try:
                os.remove(INPUT_FILE_PATH)
            except:
                pass

        if os.path.isfile(OUTPUT_FILE_PATH):
            try:
                os.remove(OUTPUT_FILE_PATH)
            except:
                pass

    def blob_with_text(self):
        container_name = self._create_container()

        # Basic
        data = u'hello world'
        blob_name = self._get_blob_reference()
        self.service.create_blob_from_text(container_name, blob_name, data)
        blob = self.service.get_blob_to_text(container_name, blob_name)
        content = blob.content # 'hello world'

        # Encoding
        text = u'hello 啊齄丂狛狜 world'
        data = text.encode('utf-16')
        blob_name = self._get_blob_reference()
        self.service.create_blob_from_text(container_name, blob_name, text, 'utf-16')
        blob = self.service.get_blob_to_text(container_name, blob_name, 'utf-16')
        content = blob.content # 'hello 啊齄丂狛狜 world'

        # Download range
        # Content settings, metadata
        # Progress
        # Parallelism
        # See blob_with_bytes for these examples. The code will be very similar.

        self.service.delete_container(container_name)

    def blocks(self):
        container_name = self._create_container()
        blob_name = self._get_blob_reference()

        # Put block
        # Block id's must be the same length
        self.service.put_block(container_name, blob_name, b'AAA', '1')
        self.service.put_block(container_name, blob_name, b'BBB', '2')
        self.service.put_block(container_name, blob_name, b'CCC', '3')

        # Get Block List
        # Defaults to committed only, specify all to get committed and uncommitted
        block_list = self.service.get_block_list(container_name, blob_name, 
                                                 block_list_type=BlockListType.All)
        uncommitted = len(block_list.uncommitted_blocks) # 3
        committed = len(block_list.committed_blocks) # 0

        # Note the blob does not yet appears as blocks have not been committed
        exists = self.service.exists(container_name, blob_name) # False

        # Commit the blocks
        # BlockBlock state defaults to Latest meaning the uncommitted and then 
        # the committed list is searched for the block id to commit
        block_list = [BlobBlock(id='1'), BlobBlock(id='2'), BlobBlock(id='3')]
        self.service.put_block_list(container_name, blob_name, block_list)

        # Get Block List
        # Defaults to committed only, specify all to get committed and uncommitted
        block_list = self.service.get_block_list(container_name, blob_name, 
                                                 block_list_type=BlockListType.All)
        uncommitted = len(block_list.uncommitted_blocks) # 0
        committed = len(block_list.committed_blocks) # 3

        # Add a block

        # Put the block
        self.service.put_block(container_name, blob_name, b'DDD', '4')

        # Get the existing blocks
        block_list = self.service.get_block_list(container_name, blob_name, 
                                                 block_list_type=BlockListType.All)
        uncommitted = len(block_list.uncommitted_blocks) # 1
        committed = len(block_list.committed_blocks) # 3

        # Added the new block to the existing list and commit
        new_block_list = block_list.committed_blocks
        new_block_list.append(block_list.uncommitted_blocks[0])
        self.service.put_block_list(container_name, blob_name, new_block_list)

        self.service.delete_container(container_name)