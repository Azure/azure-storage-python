# coding: utf-8

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
import uuid
import random
import io
import os
import time

from azure.storage.file import (
    ContentSettings,
)

class FileSamples():  

    def __init__(self, account):
        self.account = account

    def run_all_samples(self):
        self.service = self.account.create_file_service()

        self.create_file()
        self.delete_file()
        self.file_metadata()   
        self.file_properties()
        self.file_exists()
        self.resize_file()
        self.copy_file()
        self.file_range()

        self.file_with_bytes()
        self.file_with_stream()
        self.file_with_path()
        self.file_with_text()

    def _get_resource_reference(self, prefix):
        return '{}{}'.format(prefix, str(uuid.uuid4()).replace('-', ''))

    def _get_file_reference(self, prefix='file'):
        return self._get_resource_reference(prefix)

    def _create_share(self, prefix='share'):
        share_name = self._get_resource_reference(prefix)
        self.service.create_share(share_name)
        return share_name

    def _create_directory(self, share_name, prefix='dir'):
        dir_name = self._get_resource_reference(prefix)
        self.service.create_directory(share_name, dir_name)
        return dir_name

    def _get_random_bytes(self, size):
        rand = random.Random()
        result = bytearray(size)
        for i in range(size):
            result[i] = rand.randint(0, 255)
        return bytes(result)

    def create_file(self):
        share_name = self._create_share()
        directory_name = self._create_directory(share_name)

        # Basic
        file_name1 = self._get_file_reference()
        self.service.create_file(share_name, directory_name, file_name1, 512)

        # Properties
        settings = ContentSettings(content_type='html', content_language='fr')
        file_name2 = self._get_file_reference()
        self.service.create_file(share_name, directory_name, file_name2, 512, content_settings=settings)

        # Metadata
        metadata = {'val1': 'foo', 'val2': 'blah'}
        file_name2 = self._get_file_reference()
        self.service.create_file(share_name, directory_name, file_name2, 512, metadata=metadata)

        self.service.delete_share(share_name)

    def delete_file(self):
        share_name = self._create_share()
        directory_name = self._create_directory(share_name)
        file_name = self._get_file_reference()
        self.service.create_file(share_name, directory_name, file_name, 512)

        # Basic
        self.service.delete_file(share_name, directory_name, file_name)

        self.service.delete_share(share_name)

    def file_metadata(self):
        share_name = self._create_share()
        directory_name = self._create_directory(share_name)
        file_name = self._get_file_reference()
        self.service.create_file(share_name, directory_name, file_name, 512)
        metadata = {'val1': 'foo', 'val2': 'blah'}

        # Basic
        self.service.set_file_metadata(share_name, directory_name, file_name, metadata=metadata)
        metadata = self.service.get_file_metadata(share_name, directory_name, file_name) # metadata={'val1': 'foo', 'val2': 'blah'}

        # Replaces values, does not merge
        metadata = {'new': 'val'}
        self.service.set_file_metadata(share_name, directory_name, file_name, metadata=metadata)
        metadata = self.service.get_file_metadata(share_name, directory_name, file_name) # metadata={'new': 'val'}

        # Capital letters
        metadata = {'NEW': 'VAL'}
        self.service.set_file_metadata(share_name, directory_name, file_name, metadata=metadata)
        metadata = self.service.get_file_metadata(share_name, directory_name, file_name) # metadata={'new': 'VAL'}

        # Clearing
        self.service.set_file_metadata(share_name, directory_name, file_name)
        metadata = self.service.get_file_metadata(share_name, directory_name, file_name) # metadata={}
    
        self.service.delete_share(share_name)

    def file_properties(self):
        share_name = self._create_share()
        directory_name = self._create_directory(share_name)
        file_name = self._get_file_reference()

        metadata = {'val1': 'foo', 'val2': 'blah'}
        self.service.create_file(share_name, directory_name, file_name, 512, metadata=metadata)

        settings = ContentSettings(content_type='html', content_language='fr')       

        # Basic
        self.service.set_file_properties(share_name, directory_name, file_name, content_settings=settings)
        file = self.service.get_file_properties(share_name, directory_name, file_name)
        content_language = file.properties.content_settings.content_language # fr
        content_type = file.properties.content_settings.content_type # html
        content_length = file.properties.content_length # 512

        # Metadata
        # Can't set metadata, but get will return metadata already on the file
        file = self.service.get_file_properties(share_name, directory_name, file_name)
        metadata = file.metadata # metadata={'val1': 'foo', 'val2': 'blah'}

        # Replaces values, does not merge
        settings = ContentSettings(content_encoding='utf-8')
        self.service.set_file_properties(share_name, directory_name, file_name, content_settings=settings)
        file = self.service.get_file_properties(share_name, directory_name, file_name)
        content_encoding = file.properties.content_settings.content_encoding # utf-8
        content_language = file.properties.content_settings.content_language # None

        self.service.delete_share(share_name)

    def file_exists(self):
        share_name = self._create_share()
        directory_name = self._create_directory(share_name)
        file_name = self._get_file_reference()

        # Basic
        exists = self.service.exists(share_name, directory_name, file_name) # False
        self.service.create_file(share_name, directory_name, file_name, 512)
        exists = self.service.exists(share_name, directory_name, file_name) # True

        self.service.delete_share(share_name)

    def resize_file(self):
        share_name = self._create_share()
        directory_name = self._create_directory(share_name)
        file_name = self._get_file_reference()

        # Basic
        self.service.create_file(share_name, directory_name, file_name, 512)
        self.service.resize_file(share_name, directory_name, file_name, 1024)
        file = self.service.get_file_properties(share_name, directory_name, file_name)
        length = file.properties.content_length # 1024

        self.service.delete_share(share_name)

    def copy_file(self):
        share_name = self._create_share()
        directory_name = self._create_directory(share_name)
        source_file_name = self._get_file_reference()
        self.service.create_file(share_name, directory_name, source_file_name, 512)

        # Basic
        # Copy the file from the directory to the root of the share
        source = self.service.make_file_url(share_name, directory_name, source_file_name)
        copy = self.service.copy_file(share_name, None, 'file1copy', source)

        # Poll for copy completion
        while copy.status != 'success':
            count = count + 1
            if count > 5:
                print('Timed out waiting for async copy to complete.')
            time.sleep(30)
            copy = self.service.get_file_properties(share_name, dir_name, 'file1copy').properties.copy

        # With SAS from a remote account to local file
        # Commented out as remote share, directory, file, and sas would need to be created
        '''
        source_file_url = self.service.make_file_url(
            remote_share_name,
            remote_directory_name,
            remote_file_name,
            sas_token=remote_sas_token,
        )
        copy = self.service.copy_file(destination_sharename, 
                                 destination_directory_name, 
                                 destination_file_name, 
                                 source_file_url)
        '''

        # Abort copy
        # Commented out as this involves timing the abort to be sent while the copy is still running
        # Abort copy is useful to do along with polling
        # self.service.abort_copy_file(share_name, dir_name, file_name, copy.id)

        self.service.delete_share(share_name)

    def file_with_bytes(self):
        share_name = self._create_share()
        directory_name = self._create_directory(share_name)

        # Basic
        data = self._get_random_bytes(15)
        file_name = self._get_file_reference()
        self.service.create_file_from_bytes(share_name, directory_name, file_name, data)
        file = self.service.get_file_to_bytes(share_name, directory_name, file_name)
        content = file.content # data

        # Download range
        file = self.service.get_file_to_bytes(share_name, directory_name, file_name,
                                              start_range=3, end_range=10)
        content = file.content # data from 3-10

        # Upload from index in byte array
        file_name = self._get_file_reference()
        self.service.create_file_from_bytes(share_name, directory_name, file_name, data, index=3)

        # Content settings, metadata
        settings = ContentSettings(content_type='html', content_language='fr')   
        metadata={'val1': 'foo', 'val2': 'blah'}
        file_name = self._get_file_reference()
        self.service.create_file_from_bytes(share_name, directory_name, file_name, data, 
                                       content_settings=settings,
                                       metadata=metadata)
        file = self.service.get_file_to_bytes(share_name, directory_name, file_name)
        metadata = file.metadata # metadata={'val1': 'foo', 'val2': 'blah'}
        content_language = file.properties.content_settings.content_language # fr
        content_type = file.properties.content_settings.content_type # html

        # Progress
        # Use slightly larger data so the chunking is more visible
        data = self._get_random_bytes(8 * 1024 *1024)
        def upload_callback(current, total):
            print('({}, {})'.format(current, total))
        def download_callback(current, total):
            print('({}, {}) '.format(current, total))
        file_name = self._get_file_reference()

        print('upload: ')
        self.service.create_file_from_bytes(share_name, directory_name, file_name, data, 
                                       progress_callback=upload_callback)

        print('download: ')
        file = self.service.get_file_to_bytes(share_name, directory_name, file_name, 
                                         progress_callback=download_callback)

        self.service.delete_share(share_name)

    def file_with_stream(self):
        share_name = self._create_share()
        directory_name = self._create_directory(share_name)

        # Basic
        input_stream = io.BytesIO(self._get_random_bytes(15))
        output_stream = io.BytesIO()
        file_name = self._get_file_reference()
        self.service.create_file_from_stream(share_name, directory_name, file_name, 
                                             input_stream, 15)
        file = self.service.get_file_to_stream(share_name, directory_name, file_name, 
                                          output_stream)
        content_length = file.properties.content_length

        # Download range
        # Content settings, metadata
        # Progress
        # Parallelism
        # See file_with_bytes for these examples. The code will be very similar.

        self.service.delete_share(share_name)

    def file_with_path(self):
        share_name = self._create_share()
        directory_name = self._create_directory(share_name)
        INPUT_FILE_PATH = 'file_input.temp.dat'
        OUTPUT_FILE_PATH = 'file_output.temp.dat'

        data = self._get_random_bytes(4 * 1024)
        with open(INPUT_FILE_PATH, 'wb') as stream:
            stream.write(data)

        # Basic
        file_name = self._get_file_reference()
        self.service.create_file_from_path(share_name, directory_name, file_name, INPUT_FILE_PATH)
        file = self.service.get_file_to_path(share_name, directory_name, file_name, OUTPUT_FILE_PATH)
        content_length = file.properties.content_length

        # Open mode
        # Append to the file instead of starting from the beginning
        # Append streams are not seekable and so must be downloaded serially by setting max_connections=1.
        file = self.service.get_file_to_path(share_name, directory_name, file_name, OUTPUT_FILE_PATH, open_mode='ab',
                                             max_connections=1)
        content_length = file.properties.content_length # will be the same, but local file length will be longer

        # Download range
        # Content settings, metadata
        # Progress
        # Parallelism
        # See file_with_bytes for these examples. The code will be very similar.

        self.service.delete_share(share_name)

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

    def file_with_text(self):
        share_name = self._create_share()
        directory_name = self._create_directory(share_name)

        # Basic
        data = u'hello world'
        file_name = self._get_file_reference()
        self.service.create_file_from_text(share_name, directory_name, file_name, data)
        file = self.service.get_file_to_text(share_name, directory_name, file_name)
        content = file.content # 'hello world'

        # Encoding
        text = u'hello 啊齄丂狛狜 world'
        data = text.encode('utf-16')
        file_name = self._get_file_reference()
        self.service.create_file_from_text(share_name, directory_name, file_name, text, 'utf-16')
        file = self.service.get_file_to_text(share_name, directory_name, file_name, 'utf-16')
        content = file.content # 'hello 啊齄丂狛狜 world'

        # Download range
        # Content settings, metadata
        # Progress
        # Parallelism
        # See file_with_bytes for these examples. The code will be very similar.

        self.service.delete_share(share_name)

    def file_range(self):
        share_name = self._create_share()
        directory_name = self._create_directory(share_name)
        file_name = self._get_file_reference()
        self.service.create_file(share_name, directory_name, file_name, 2048)

        # Update the file between offset 512 and 15351535
        data = b'abcdefghijklmnop' * 64
        self.service.update_range(share_name, directory_name, file_name, data, 512, 1535)

        # List ranges
        print('list ranges: ')
        ranges = self.service.list_ranges(share_name, directory_name, file_name)
        for range in ranges:
            print('({}, {}) '.format(range.start, range.end)) # (512, 1535)

        # Clear part of that range
        self.service.clear_range(share_name, directory_name, file_name, 600, 800)

        self.service.delete_share(share_name)