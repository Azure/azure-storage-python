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

    def __init__(self, name=None, props=None, metadata=None):
        self.name = name
        self.properties = props or ShareProperties()
        self.metadata = metadata


class ShareProperties(object):

    ''' File share's properties class. '''

    def __init__(self):
        self.last_modified = None
        self.etag = None
        self.quota = None

class Directory(object):

    ''' Directory class. '''

    def __init__(self, name=None, props=None, metadata=None):
        self.name = name
        self.properties = props or DirectoryProperties()
        self.metadata = metadata

class DirectoryProperties(object):

    ''' File share's properties class. '''

    def __init__(self):
        self.last_modified = None
        self.etag = None

class File(object):

    ''' File class. '''
    def __init__(self, name=None, content=None, props=None, metadata=None):
        self.name = name
        self.content = content
        self.properties = props or FileProperties()
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


class Range(object):

    ''' File Range. '''

    def __init__(self):
        self.start = None
        self.end = None

class ShareStats(object):

    ''' Share Stats. '''

    def __init__(self):
        self.share_usage = None


class FilePermissions(object):

    '''
    FilePermissions class to be used with 
    `._FileService.generate_file_shared_access_signature` method.

    :param bool read:
        Read the content, properties, metadata. Use the file as the source of a copy 
        operation.
    :param bool create:
        Create a new file or copy a file to a new file.
    :param bool write: 
        Create or write content, properties, metadata. Resize the file. Use the file 
        as the destination of a copy operation within the same account.
    :param bool delete: 
        Delete the file.
    :param str _str: 
        A string representing the permissions.
    '''
    def __init__(self, read=False, create=False, write=False, delete=False, 
                 _str=None):
        if not _str:
            _str = ''
        self.read = read or ('r' in _str)
        self.create = create or ('c' in _str)
        self.write = write or ('w' in _str)
        self.delete = delete or ('d' in _str)
    
    def __or__(self, other):
        return FilePermissions(_str=str(self) + str(other))

    def __add__(self, other):
        return FilePermissions(_str=str(self) + str(other))
    
    def __str__(self):
        return (('r' if self.read else '') +
                ('c' if self.create else '') +
                ('w' if self.write else '') +
                ('d' if self.delete else ''))

'''
Read the content, properties, metadata. Use the file as the source of a copy 
operation.
'''
FilePermissions.READ = FilePermissions(read=True)

'''
Create a new file or copy a file to a new file.
'''
FilePermissions.CREATE = FilePermissions(create=True)

'''
Create or write content, properties, metadata. Resize the file. Use the file 
as the destination of a copy operation within the same account.
'''
FilePermissions.WRITE = FilePermissions(write=True)

'''Delete the file.'''
FilePermissions.DELETE = FilePermissions(delete=True)


class SharePermissions(object):

    '''
    SharePermissions class to be used with `azure.storage.file.FileService.generate_share_shared_access_signature`
    method and for the AccessPolicies used with `azure.storage.file.FileService.set_share_acl`. 

    :param bool read:
        Read the content, properties or metadata of any file in the share. Use any 
        file in the share as the source of a copy operation.
    :param bool write: 
        For any file in the share, create or write content, properties or metadata. 
        Resize the file. Use the file as the destination of a copy operation within 
        the same account.
        Note: You cannot grant permissions to read or write share properties or 
        metadata with a service SAS. Use an account SAS instead.
    :param bool delete: 
        Delete any file in the share.
        Note: You cannot grant permissions to delete a share with a service SAS. Use 
        an account SAS instead.
    :param bool list: 
        List files and directories in the share.
    :param str _str: 
        A string representing the permissions.
    '''
    def __init__(self, read=False, write=False, delete=False, list=False, 
                 _str=None):
        if not _str:
            _str = ''
        self.read = read or ('r' in _str)
        self.write = write or ('w' in _str)
        self.delete = delete or ('d' in _str)
        self.list = list or ('l' in _str)
    
    def __or__(self, other):
        return SharePermissions(_str=str(self) + str(other))

    def __add__(self, other):
        return SharePermissions(_str=str(self) + str(other))
    
    def __str__(self):
        return (('r' if self.read else '') +
                ('w' if self.write else '') +
                ('d' if self.delete else '') + 
                ('l' if self.list else ''))

'''
Read the content, properties or metadata of any file in the share. Use any 
file in the share as the source of a copy operation.
'''
SharePermissions.READ = SharePermissions(read=True)

'''
For any file in the share, create or write content, properties or metadata. 
Resize the file. Use the file as the destination of a copy operation within 
the same account.
Note: You cannot grant permissions to read or write share properties or 
metadata with a service SAS. Use an account SAS instead.
'''
SharePermissions.WRITE = SharePermissions(write=True)

'''
Delete any file in the share.
Note: You cannot grant permissions to delete a share with a service SAS. Use 
an account SAS instead.
'''
SharePermissions.DELETE = SharePermissions(delete=True)

'''List files and directories in the share.'''
SharePermissions.LIST = SharePermissions(list=True)