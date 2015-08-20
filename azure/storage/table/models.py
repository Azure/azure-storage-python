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
from .._common_models import (
    WindowsAzureData,
)


class Entity(WindowsAzureData):

    ''' Entity class. The attributes of entity will be created dynamically. '''
    pass


class EntityProperty(WindowsAzureData):

    ''' Entity property. contains type and value.  '''

    def __init__(self, type=None, value=None):
        self.type = type
        self.value = value


class Table(WindowsAzureData):

    ''' Only for IntelliSense and telling user the return type. '''
    pass


class TableSharedAccessPermissions(object):
    '''Permissions for a table.'''

    '''Get entities and query entities.'''
    QUERY = 'r'

    '''Add entities.'''
    ADD = 'a'

    '''Update entities.'''
    UPDATE = 'u'

    '''Delete entities.'''
    DELETE = 'd'

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
