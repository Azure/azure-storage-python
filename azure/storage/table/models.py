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
from azure.common import (
    AzureException,
    AzureHttpError,
)
from ._error import (
    _ERROR_ATTRIBUTE_MISSING,
)

class AzureBatchValidationError(AzureException):

    '''Indicates that a batch operation cannot proceed due to invalid input'''


class AzureBatchOperationError(AzureHttpError):

    '''Indicates that a batch operation failed'''

    def __init__(self, message, status_code, batch_code):
        super(AzureBatchOperationError, self).__init__(message, status_code)
        self.code = batch_code

class Entity(dict):
    ''' Entity class. The attributes of entity will be created dynamically. '''

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(_ERROR_ATTRIBUTE_MISSING.format('Entity', name))

    __setattr__ = dict.__setitem__

    def __delattr__(self, name):
        try:
            del self[name]
        except KeyError:
            raise AttributeError(_ERROR_ATTRIBUTE_MISSING.format('Entity', name))

    def __dir__(self):
        return dir({}) + list(self.keys())


class EntityProperty(object):
    ''' Entity property. contains type and value.  '''

    def __init__(self, type=None, value=None):
        self.type = type
        self.value = value


class Table(object):
    ''' Only for IntelliSense and telling user the return type. '''

    pass


class TablePayloadFormat(object):
    '''
    Specifies the accepted content type of the response payload. More information
    can be found here: https://msdn.microsoft.com/en-us/library/azure/dn535600.aspx
    '''

    '''Returns no type information for the entity properties.'''
    JSON_NO_METADATA = 'application/json;odata=nometadata'

    '''Returns minimal type information for the entity properties.'''
    JSON_MINIMAL_METADATA = 'application/json;odata=minimalmetadata'

    '''Returns minimal type information for the entity properties plus some extra odata properties.'''
    JSON_FULL_METADATA = 'application/json;odata=fullmetadata'


class EdmType(object):
    BINARY = 'Edm.Binary'
    INT64 = 'Edm.Int64'
    GUID = 'Edm.Guid'
    DATETIME = 'Edm.DateTime'
    STRING = 'Edm.String'
    INT32 = 'Edm.Int32'
    DOUBLE = 'Edm.Double'
    BOOLEAN = 'Edm.Boolean'


class TablePermissions(object):

    '''
    TablePermissions class to be used with `azure.storage.table.TableService.generate_table_shared_access_signature`
    method and for the AccessPolicies used with `azure.storage.table.TableService.set_table_acl`. 

    :param bool query:
        Get entities and query entities.
    :param bool add:
        Add entities. Add and Update permissions are required for upsert operations.
    :param bool update:
        Update entities. Add and Update permissions are required for upsert operations.
    :param bool delete: 
        Delete entities.
    :param str _str: 
        A string representing the permissions.
    '''
    def __init__(self, query=False, add=False, update=False, delete=False, _str=None):
        if not _str:
            _str = ''
        self.query = query or ('r' in _str)
        self.add = add or ('a' in _str)
        self.update = update or ('u' in _str)
        self.delete = delete or ('d' in _str)
    
    def __or__(self, other):
        return TablePermissions(_str=str(self) + str(other))

    def __add__(self, other):
        return TablePermissions(_str=str(self) + str(other))
    
    def __str__(self):
        return (('r' if self.query else '') +
                ('a' if self.add else '') +
                ('u' if self.update else '') +
                ('d' if self.delete else ''))

''' Get entities and query entities. '''
TablePermissions.QUERY = TablePermissions(query=True)

''' Add entities. '''
TablePermissions.ADD = TablePermissions(add=True)

''' Update entities. '''
TablePermissions.UPDATE = TablePermissions(update=True)

''' Delete entities. '''
TablePermissions.DELETE = TablePermissions(delete=True)
