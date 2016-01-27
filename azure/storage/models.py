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
import sys
if sys.version_info < (3,):
    _unicode_type = unicode
else:
    _unicode_type = str

from ._error import (
    _validate_not_none,
)

class HeaderDict(dict):

    def __getitem__(self, index):
        return super(HeaderDict, self).__getitem__(index.lower())

class _list(list):
    '''Used so that a continuation token can be set on the return object'''
    pass

class _dict(dict):
    '''Used so that additional properties can be set on the return dictionary'''
    pass

class ListGenerator(object):
    def __init__(self, resources, list_method, list_args, list_kwargs):
        self.items = resources
        self.next_marker = resources.next_marker

        self._list_method = list_method
        self._list_args = list_args
        self._list_kwargs = list_kwargs

    def __iter__(self):
        # return results
        for i in self.items:
            yield i

        while True:
            # if no more results on the service, return
            if not self.next_marker:
                break

            # update the marker args
            self._list_kwargs['marker'] = self.next_marker

            # handle max results, if present
            max_results = self._list_kwargs.get('max_results')
            if max_results is not None:
                max_results = max_results - len(self.items)

                # if we've reached max_results, return
                # else, update the max_results arg
                if max_results <= 0:
                    break
                else:
                    self._list_kwargs['max_results'] = max_results

            # get the next segment
            resources = self._list_method(*self._list_args, **self._list_kwargs)
            self.items = resources
            self.next_marker = resources.next_marker

            # return results
            for i in self.items:
                yield i

class RetentionPolicy(object):

    '''
    RetentionPolicy class to be used with ServiceProperties.
    
    :param bool enabled: 
        Indicates whether a retention policy is enabled for the 
        storage service. If disabled, logging and metrics data will be retained 
        infinitely by the service unless explicitly deleted.
    :param int days: 
        Required if enabled is true. Indicates the number of 
        days that metrics or logging data should be retained. All data older 
        than this value will be deleted. The minimum value you can specify is 1; 
        the largest value is 365 (one year).
    '''

    def __init__(self, enabled=False, days=None):
        _validate_not_none("enabled", enabled)
        if enabled:
            _validate_not_none("days", days)

        self.enabled = enabled
        self.days = days


class Logging(object):

    '''
    Logging class to be used with ServiceProperties.

    :param bool delete: 
        Indicates whether all delete requests should be logged.
    :param bool read: 
        Indicates whether all read requests should be logged.
    :param bool write: 
        Indicates whether all write requests should be logged.
    :param RetentionPolicy retention_policy: 
        The retention policy for the metrics.
    '''

    def __init__(self, delete=False, read=False, write=False,
                 retention_policy=None):
        _validate_not_none("read", read)
        _validate_not_none("write", write)
        _validate_not_none("delete", delete)

        self.version = u'1.0'
        self.delete = delete
        self.read = read
        self.write = write
        self.retention_policy = retention_policy if retention_policy else RetentionPolicy()


class Metrics(object):

    ''' 
    Metrics class to be used with ServiceProperties.

    :param bool enabled: 
        Indicates whether metrics are enabled for 
        the service.
    :param bool include_apis: 
        Required if enabled is True. Indicates whether metrics 
        should generate summary statistics for called API operations.
    :param RetentionPolicy retention_policy: 
        The retention policy for the metrics.
    '''

    def __init__(self, enabled=False, include_apis=None,
                 retention_policy=None):
        _validate_not_none("enabled", enabled)
        if enabled:
            _validate_not_none("include_apis", include_apis)

        self.version = u'1.0'
        self.enabled = enabled
        self.include_apis = include_apis
        self.retention_policy = retention_policy if retention_policy else RetentionPolicy()


class CorsRule(object):

    '''
    Cors Rule class to be used with ServiceProperties.
    
    :param allowed_origins: 
        A list of origin domains that will be allowed via CORS, or "*" to allow 
        all domains. The list of must contain at least one entry. Limited to 64 
        origin domains. Each allowed origin can have up to 256 characters.
    :type allowed_origins: list of str
    :param allowed_methods:
        A list of HTTP methods that are allowed to be executed by the origin. 
        The list of must contain at least one entry. For Azure Storage, 
        permitted methods are DELETE, GET, HEAD, MERGE, POST, OPTIONS or PUT.
    :type allowed_methods: list of str
    :param int max_age_in_seconds:
        The number of seconds that the client/browser should cache a 
        preflight response.
    :param exposed_headers:
        Defaults to an empty list. A list of response headers to expose to CORS 
        clients. Limited to 64 defined headers and two prefixed headers. Each 
        header can be up to 256 characters.
    :type exposed_headers: list of str
    :param allowed_headers:
        Defaults to an empty list. A list of headers allowed to be part of 
        the cross-origin request. Limited to 64 defined headers and 2 prefixed 
        headers. Each header can be up to 256 characters.
    :type allowed_headers: list of str
    '''

    def __init__(self, allowed_origins, allowed_methods, max_age_in_seconds=0,
                 exposed_headers=None, allowed_headers=None):
        _validate_not_none("allowed_origins", allowed_origins)
        _validate_not_none("allowed_methods", allowed_methods)
        _validate_not_none("max_age_in_seconds", max_age_in_seconds)

        self.allowed_origins = allowed_origins if allowed_origins else list()
        self.allowed_methods = allowed_methods if allowed_methods else list()
        self.max_age_in_seconds = max_age_in_seconds
        self.exposed_headers = exposed_headers if exposed_headers else list()
        self.allowed_headers = allowed_headers if allowed_headers else list()


class ServiceProperties(object):
    ''' Only for IntelliSense and telling user the return type. '''

    pass


class AccessPolicy(object):

    ''' 
    Access Policy class used by the set and get acl methods in each service.

    A stored access policy can specify the start time, expiry time, and 
    permissions for the Shared Access Signatures with which it's associated. 
    Depending on how you want to control access to your table resource, you can 
    specify all of these parameters within the stored access policy, and omit 
    them from the URL for the Shared Access Signature. Doing so permits you to 
    modify the associated signature's behavior at any time, as well as to revoke 
    it. Or you can specify one or more of the access policy parameters within 
    the stored access policy, and the others on the URL. Finally, you can 
    specify all of the parameters on the URL. In this case, you can use the 
    stored access policy to revoke the signature, but not to modify its behavior.

    Together the Shared Access Signature and the stored access policy must 
    include all fields required to authenticate the signature. If any required 
    fields are missing, the request will fail. Likewise, if a field is specified 
    both in the Shared Access Signature URL and in the stored access policy, the 
    request will fail with status code 400 (Bad Request).

    :param str permission:
        The permissions associated with the shared access signature. The 
        user is restricted to operations allowed by the permissions. 
        Required unless an id is given referencing a stored access policy 
        which contains this field. This field must be omitted if it has been 
        specified in an associated stored access policy.
    :param expiry:
        The time at which the shared access signature becomes invalid. 
        Required unless an id is given referencing a stored access policy 
        which contains this field. This field must be omitted if it has 
        been specified in an associated stored access policy. Azure will always 
        convert values to UTC. If a date is passed in without timezone info, it 
        is assumed to be UTC.
    :type expiry: date or str
    :param start:
        The time at which the shared access signature becomes valid. If 
        omitted, start time for this call is assumed to be the time when the 
        storage service receives the request. Azure will always convert values 
        to UTC. If a date is passed in without timezone info, it is assumed to 
        be UTC.
    :type start: date or str
    '''

    def __init__(self, permission=None, expiry=None, start=None):
        self.start = start
        self.expiry = expiry
        self.permission = permission


class ResourceTypes(object):

    '''
    Specifies the resource types that are accessible with the account SAS.

    :param bool service:
        Access to service-level APIs (e.g., Get/Set Service Properties, 
        Get Service Stats, List Containers/Queues/Tables/Shares) 
    :param bool container:
        Access to container-level APIs (e.g., Create/Delete Container, 
        Create/Delete Queue, Create/Delete Table, Create/Delete Share, 
        List Blobs/Files and Directories) 
    :param bool object:
        Access to object-level APIs for blobs, queue messages, table entities, and 
        files(e.g. Put Blob, Query Entity, Get Messages, Create File, etc.) 
    :param str _str: 
        A string representing the resource types.
    '''
    def __init__(self, service=False, container=False, object=False, _str=None):

        if not _str:
            _str = ''
        self.service = service or ('s' in _str)
        self.container = container or ('c' in _str)
        self.object = object or ('o' in _str)
    
    def __or__(self, other):
        return ResourceTypes(_str=str(self) + str(other))

    def __add__(self, other):
        return ResourceTypes(_str=str(self) + str(other))
    
    def __str__(self):
        return (('s' if self.service else '') +
                ('c' if self.container else '') +
                ('o' if self.object else ''))

''' 
Access to service-level APIs (e.g., Get/Set Service Properties, 
Get Service Stats, List Containers/Queues/Tables/Shares) 
'''
ResourceTypes.SERVICE = ResourceTypes(service=True)

''' 
Access to container-level APIs (e.g., Create/Delete Container, 
Create/Delete Queue, Create/Delete Table, Create/Delete Share, 
List Blobs/Files and Directories) 
'''
ResourceTypes.CONTAINER = ResourceTypes(container=True)

''' 
Access to object-level APIs for blobs, queue messages, table entities, and 
files(e.g. Put Blob, Query Entity, Get Messages, Create File, etc.) 
'''
ResourceTypes.OBJECT = ResourceTypes(object=True)


class Services(object):

    '''
    Specifies the services accessible with the account SAS. Possible values include:

    :param bool blob:
        Access to any blob service, for example, the `.BlockBlobService`
    :param bool queue:
        Access to the `.QueueService`
    :param bool table:
        Access to the `.TableService`
    :param bool file:
        Access to the `.FileService`
    :param str _str: 
        A string representing the services.
    '''
    def __init__(self, blob=False, queue=False, table=False, file=False, _str=None):

        if not _str:
            _str = ''
        self.blob = blob or ('b' in _str)
        self.queue = queue or ('q' in _str)
        self.table = table or ('t' in _str)
        self.file = file or ('f' in _str)
    
    def __or__(self, other):
        return Services(_str=str(self) + str(other))

    def __add__(self, other):
        return Services(_str=str(self) + str(other))
    
    def __str__(self):
        return (('b' if self.blob else '') +
                ('q' if self.queue else '') +
                ('t' if self.table else '') +
                ('f' if self.file else ''))

''' 
The blob service.
'''
Services.BLOB = Services(blob=True)

''' 
The queue service.
'''
Services.QUEUE = Services(queue=True)

''' 
The table service
'''
Services.TABLE = Services(table=True)

''' 
The file service.
'''
Services.FILE = Services(file=True)


class AccountPermissions(object):

    '''
    TablePermissions class to be used with TableService generate_shared_access_signature 
    method and for the AccessPolicies used with set_table_acl. 
    There are two types of SAS which may be used to grant table access. One is to grant 
    access to a specific table (table-specific) to do operations on its entities. 
    Another is to grant access to the entire table service for a specific account 
    and allow certain operations based on perms found here.

    :param bool read:
        Valid for all signed resources types (Service, Container, and Object). 
        Permits read permissions to the specified resource type.
    :param bool write:
        Valid for all signed resources types (Service, Container, and Object). 
        Permits write permissions to the specified resource type.
    :param bool delete: 
        Valid for Container and Object resource types, except for queue messages.
    :param bool list:
        Valid for Service and Container resource types only.
    :param bool add:
        Valid for the following Object resource types only: queue messages, 
        table entities, and append blobs.
    :param bool create:
        Valid for the following Object resource types only: blobs and files. 
        Users can create new blobs or files, but may not overwrite existing 
        blobs or files.
    :param bool update:
        Valid for the following Object resource types only: queue messages and 
        table entities.
    :param bool process:
        Valid for the following Object resource type only: queue messages.
    :param str _str: 
        A string representing the permissions.
    '''
    def __init__(self, read=False, write=False, delete=False, list=False, 
                 add=False, create=False, update=False, process=False, _str=None):

        if not _str:
            _str = ''
        self.read = read or ('r' in _str)
        self.write = write or ('w' in _str)
        self.delete = delete or ('d' in _str)
        self.list = list or ('l' in _str)
        self.add = add or ('a' in _str)
        self.create = create or ('c' in _str)
        self.update = update or ('u' in _str)
        self.process = process or ('p' in _str)
    
    def __or__(self, other):
        return TablePermissions(_str=str(self) + str(other))

    def __add__(self, other):
        return TablePermissions(_str=str(self) + str(other))
    
    def __str__(self):
        return (('r' if self.read else '') +
                ('w' if self.write else '') +
                ('d' if self.delete else '') +
                ('l' if self.list else '') +
                ('a' if self.add else '') +
                ('c' if self.create else '') +
                ('u' if self.update else '') +
                ('p' if self.process else ''))

''' 
Valid for all signed resources types (Service, Container, and Object). 
Permits read permissions to the specified resource type. 
'''
AccountPermissions.READ = AccountPermissions(read=True)

''' 
Valid for all signed resources types (Service, Container, and Object). 
Permits write permissions to the specified resource type. 
'''
AccountPermissions.WRITE = AccountPermissions(write=True)

''' 
Valid for Container and Object resource types, except for queue messages. 
'''
AccountPermissions.DELETE = AccountPermissions(delete=True)

''' 
Valid for Service and Container resource types only. 
'''
AccountPermissions.LIST = AccountPermissions(list=True)

''' 
Valid for the following Object resource types only: queue messages, table 
entities, and append blobs. 
'''
AccountPermissions.ADD = AccountPermissions(add=True)

''' 
Valid for the following Object resource types only: blobs and files. Users 
can create new blobs or files, but may not overwrite existing blobs or files. 
'''
AccountPermissions.CREATE = AccountPermissions(create=True)

''' 
Valid for the following Object resource types only: queue messages and table 
entities. 
'''
AccountPermissions.UPDATE = AccountPermissions(update=True)

''' 
Valid for the following Object resource type only: queue messages. 
'''
AccountPermissions.PROCESS = AccountPermissions(process=True)
