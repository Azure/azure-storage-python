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
class Share(object):

    ''' File share class. '''

    def __init__(self):
        self.name = None
        self.properties = ShareProperties()
        self.metadata = None


class ShareProperties(object):

    ''' File share's properties class. '''

    def __init__(self):
        self.last_modified = None
        self.etag = None
        self.quota = None


class FileAndDirectoryResults(object):

    ''' 
    Enum result class holding a list of files
    and a list of directories. 
    '''

    def __init__(self):
        self.files = list()
        self.directories = list()

class File(bytes):

    ''' File class. '''

    def __new__(cls, file=None, properties=None, metadata=None):
        return bytes.__new__(cls, file if file else b'')

    def __init__(self, file=None, properties=None, metadata=None):
        self.name = None
        self.properties = properties or FileProperties()
        self.metadata = metadata


class FileProperties(object):

    ''' File Properties '''

    def __init__(self):
        self.last_modified = None
        self.etag = None
        self.content_length = None
        self.content_settings = ContentSettings()
        self.copy = CopyProperties()


class ContentSettings(object):

    '''ContentSettings object used for File services.'''

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
            ('x-ms-cache-control', _str_or_none(self.cache_control)),
            ('x-ms-content-type', _str_or_none(self.content_type)),
            ('x-ms-content-disposition',
                _str_or_none(self.content_disposition)),
            ('x-ms-content-md5', _str_or_none(self.content_md5)),
            ('x-ms-content-encoding',
                _str_or_none(self.content_encoding)),
            ('x-ms-content-language',
                _str_or_none(self.content_language)),
        ]


class CopyProperties(object):
    '''File Copy Properties'''

    def __init__(self):
        self.id = None
        self.source = None
        self.status = None
        self.progress = None
        self.completion_time = None
        self.status_description = None


class Directory(object):

    ''' Directory class. '''

    def __init__(self):
        self.name = None


class Range(object):

    ''' File Range. '''

    def __init__(self):
        self.start = None
        self.end = None

class ShareStats(object):

    ''' Share Stats. '''

    def __init__(self):
        self.share_usage = None

class ShareSharedAccessPermissions(object):
    '''Permissions for a share.'''

    '''
    Read the content, properties or metadata of any file in the share. Use any 
    file in the share as the source of a copy operation.
    '''
    READ = 'r'

    '''
    For any file in the share, create or write content, properties or metadata. 
    Resize the file. Use the file as the destination of a copy operation within 
    the same account.
    Note: You cannot grant permissions to read or write share properties or 
    metadata with a service SAS. Use an account SAS instead.
    '''
    WRITE = 'w'

    '''
    Delete any file in the share.
    Note: You cannot grant permissions to delete a share with a service SAS. Use 
    an account SAS instead.
    '''
    DELETE = 'd'

    '''List files and directories in the share.'''
    LIST = 'l'


class FileSharedAccessPermissions(object):
    '''Permissions for a file.'''

    '''
    Read the content, properties, metadata. Use the file as the source of a copy 
    operation.
    '''
    READ = 'r'

    '''
    Create a new file or copy a file to a new file.
    '''
    CREATE = 'c'

    '''
    Create or write content, properties, metadata. Resize the file. Use the file 
    as the destination of a copy operation within the same account.
    '''
    WRITE = 'w'

    '''Delete the file.'''
    DELETE = 'd'