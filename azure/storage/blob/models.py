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
from .._common_conversion import _str_or_none
class Container(object):

    ''' Blob container class. '''

    def __init__(self):
        self.name = None
        self.properties = ContainerProperties()
        self.metadata = None


class ContainerProperties(object):

    ''' Blob container's properties class. '''

    def __init__(self):
        self.last_modified = None
        self.etag = None
        self.lease_status = None
        self.lease_state = None
        self.lease_duration = None


class Blob(bytes):

    ''' Blob class'''
    def __new__(cls, blob=None, props=None, metadata=None):
        return bytes.__new__(cls, blob if blob else b'')

    def __init__(self, blob=None, props=None, metadata=None):
        self.name = None
        self.snapshot = None
        self.properties = props or BlobProperties()
        self.metadata = metadata


class BlobProperties(object):

    ''' Blob Properties '''

    def __init__(self):
        self.blob_type = None
        self.last_modified = None
        self.etag = None
        self.content_length = None
        self.append_blob_committed_block_count = None
        self.page_blob_sequence_number = None
        self.copy = CopyProperties()
        self.content_settings = ContentSettings()
        self.lease = LeaseProperties()


class ContentSettings(object):

    '''ContentSettings object used for Blob services.'''

    def __init__(
        self, content_type=None, content_encoding=None,
        content_language=None, content_disposition=None,
        cache_control=None, content_md5=None):
        
        self.content_type = content_type
        self.content_encoding = content_encoding
        self.content_language = content_language
        self.content_disposition = content_disposition
        self.cache_control = cache_control
        self.content_md5 = content_md5

    def to_headers(self):
        return [
            ('x-ms-blob-cache-control', _str_or_none(self.cache_control)),
            ('x-ms-blob-content-type', _str_or_none(self.content_type)),
            ('x-ms-blob-content-disposition',
                _str_or_none(self.content_disposition)),
            ('x-ms-blob-content-md5', _str_or_none(self.content_md5)),
            ('x-ms-blob-content-encoding',
                _str_or_none(self.content_encoding)),
            ('x-ms-blob-content-language',
                _str_or_none(self.content_language)),
        ]


class CopyProperties(object):
    '''Blob Copy Properties'''

    def __init__(self):
        self.id = None
        self.source = None
        self.status = None
        self.progress = None
        self.completion_time = None
        self.status_description = None


class LeaseProperties(object):

    '''Blob Lease Properties'''

    def __init__(self):
        self.status = None
        self.state = None
        self.duration = None


class BlobBlockState(object):
    '''Block blob block types'''

    '''Uncommitted blocks'''
    Uncommitted = 'Uncommitted'

    '''Committed blocks'''
    Committed = 'Committed'

    '''Latest blocks'''
    Latest = 'Latest'


class BlobBlock(object):

    ''' BlobBlock class '''

    def __init__(self, id=None, state=BlobBlockState.Latest):
        self.id = id
        self.state = state

    def _set_size(self, size):
        self.size = size


class BlobBlockList(object):

    ''' BlobBlockList class '''

    def __init__(self):
        self.committed_blocks = list()
        self.uncommitted_blocks = list()

class BlobList(object):
    '''BlobList class'''

    def __init__(self):
        self.next_marker = None
        self.blobs = list()
        self.prefixes = list()

    def __iter__(self):
        return iter(self.blobs)

    def __len__(self):
        return len(self.blobs)

    def __getitem__(self, index):
        return self.blobs[index]

class PageRange(object):

    ''' Page Range for page blob. '''

    def __init__(self, start=None, end=None):
        self.start = start
        self.end = end

class ContainerSharedAccessPermissions(object):
    '''Permissions for a container.'''

    '''
    Read the content, properties, metadata or block list of any blob in
    the container. Use any blob in the container as the source of a
    copy operation.
    '''
    READ = 'r'

    '''
    For any blob in the container, create or write content, properties,
    metadata, or block list. Snapshot or lease the blob. Resize the blob
    (page blob only). Use the blob as the destination of a copy operation
    within the same account.
    You cannot grant permissions to read or write container properties or
    metadata, nor to lease a container.
    '''
    WRITE = 'w'

    '''Delete any blob in the container.'''
    DELETE = 'd'

    '''List blobs in the container.'''
    LIST = 'l'


class BlobSharedAccessPermissions(object):
    '''Permissions for a blob.'''

    '''
    Read the content, properties, metadata and block list. Use the blob
    as the source of a copy operation.
    '''
    READ = 'r'

    '''
    Create or write content, properties, metadata, or block list.
    Snapshot or lease the blob. Resize the blob (page blob only). Use the
    blob as the destination of a copy operation within the same account.
    '''
    WRITE = 'w'

    '''Delete the blob.'''
    DELETE = 'd'

class LeaseActions(object):
    '''Actions for a lease'''

    '''Acquire the lease.'''
    Acquire = 'acquire'

    '''Renew the lease.'''
    Renew = 'renew'

    '''Release the lease.'''
    Release = 'release'

    '''Break the lease.'''
    Break = 'break'

    '''Change the lease ID.'''
    Change = 'change'

class _BlobTypes(object):
    '''Blob type options'''

    '''Block blob type.'''
    BlockBlob = 'BlockBlob'

    '''Page blob type.'''
    PageBlob = 'PageBlob'

    '''Append blob type.'''
    AppendBlob = 'AppendBlob'