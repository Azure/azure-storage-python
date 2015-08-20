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
import types

from dateutil import parser
from dateutil.tz import tzutc
if sys.version_info < (3,):
    from urllib2 import quote as url_quote
else:
    from urllib.parse import quote as url_quote
from json import (
    loads,
)
from azure.common import (
    AzureException,
)
from .._common_serialization import (
    _set_continuation_from_response_headers,
)
from .._common_conversion import (
    _decode_base64_to_bytes,
)
from ._error import (
    _ERROR_TYPE_NOT_SUPPORTED,
    _ERROR_INVALID_PROPERTY_RESOLVER,
)
from .models import (
    Entity,
    EntityProperty,
    Table,
    EdmType,
)


class _list(list):
    '''Used so that a continuation token can be set on the return object'''
    pass

# Tables of conversions to and from entity types.  We support specific
# datatypes, and beyond that the user can use an EntityProperty to get
# custom data type support.

def _from_entity_binary(value):
    return EntityProperty(EdmType.BINARY, _decode_base64_to_bytes(value))


def _from_entity_int64(value):
    return EntityProperty(EdmType.INT64, int(value))


def _from_entity_datetime(value):
    # Note that Azure always returns UTC datetime, and dateutil parser
    # will set the tzinfo on the date it returns
    return parser.parse(value)


_EDM_TYPES = [EdmType.BINARY, EdmType.INT64, EdmType.GUID, EdmType.DATETIME,
              EdmType.STRING, EdmType.INT32, EdmType.DOUBLE, EdmType.BOOLEAN]


_ENTITY_TO_PYTHON_CONVERSIONS = {
    EdmType.BINARY: _from_entity_binary,
    EdmType.INT64: _from_entity_int64,
    EdmType.DOUBLE: float,
    EdmType.DATETIME: _from_entity_datetime,
}


def _convert_json_response_to_entity(response, property_resolver):
    if response is None:
        return response

    root = loads(response.body.decode('utf-8'))

    return _convert_json_to_entity(root, property_resolver)


def _convert_json_to_entity(entry_element, property_resolver):
    ''' Convert json response to entity.

    The entity format is:
    {
       "Address":"Mountain View",
       "Age":23,
       "AmountDue":200.23,
       "CustomerCode@odata.type":"Edm.Guid",
       "CustomerCode":"c9da6455-213d-42c9-9a79-3e9149a57833",
       "CustomerSince@odata.type":"Edm.DateTime",
       "CustomerSince":"2008-07-10T00:00:00",
       "IsActive":true,
       "NumberOfOrders@odata.type":"Edm.Int64",
       "NumberOfOrders":"255",
       "PartitionKey":"mypartitionkey",
       "RowKey":"myrowkey"
    }
    '''
    entity = Entity()

    properties = {}
    edmtypes = {}
    odata = {}

    for name, value in entry_element.items():
        if name.startswith('odata.'):
            odata[name[6:]] = value
        elif name.endswith('@odata.type'):
            edmtypes[name[:-11]] = value
        else:
            properties[name] = value

    # Partition key is a known property
    partition_key = properties.pop('PartitionKey', None)
    if partition_key:
        _set_entity_attr(entity, 'PartitionKey', partition_key)

    # Row key is a known property
    row_key = properties.pop('RowKey', None)
    if row_key:
        _set_entity_attr(entity, 'RowKey', row_key)

    # Timestamp is a known property
    timestamp = properties.pop('Timestamp', None)
    if timestamp:
        _set_entity_attr(entity, 'Timestamp',
                         _from_entity_datetime(timestamp))
        
    for name, value in properties.items():
        mtype = edmtypes.get(name);

        # use the property resolver if present
        if property_resolver:
            mtype = property_resolver(partition_key, row_key, 
                                      name, value, mtype)

            # throw if the type returned is not a valid edm type
            if mtype and mtype not in _EDM_TYPES:
                raise AzureException(_ERROR_TYPE_NOT_SUPPORTED.format(mtype))

        # no type info, property should parse automatically
        if not mtype:
            _set_entity_attr(entity, name, value)
        else:  # need an object to hold the property
            conv = _ENTITY_TO_PYTHON_CONVERSIONS.get(mtype)
            if conv is not None:
                try:
                    property = conv(value)
                except Exception as e:
                    # throw if the type returned by the property resolver
                    # cannot be used in the conversion
                    if property_resolver:
                        raise AzureException(
                            _ERROR_INVALID_PROPERTY_RESOLVER.format(name, value, mtype))
                    else:
                        raise e
            else:
                property = EntityProperty(mtype, value)
            _set_entity_attr(entity, name, property)

    # extract etag from entry
    etag = odata.get('etag')
    if timestamp:
         etag = 'W/"datetime\'' + url_quote(timestamp) + '\'"'
    _set_entity_attr(entity, 'etag', etag)

    return entity


def _set_entity_attr(entity, name, value):
    try:
        setattr(entity, name, value)
    except UnicodeEncodeError:
        # Python 2 doesn't support unicode attribute names, so we'll
        # add them and access them directly through the dictionary
        entity.__dict__[name] = value


def _convert_json_response_to_tables(response):
    ''' Converts the response to tables class.
    '''
    if response is None:
        return response

    tables = _list()

    _set_continuation_from_response_headers(tables, response)

    root = loads(response.body.decode('utf-8'))

    if 'TableName' in root:
        table = Table()
        table.name = root['TableName']
        tables.append(table)
    else:
        for element in root['value']:
            table = Table()
            table.name = element['TableName']
            tables.append(table)

    return tables


def _convert_json_response_to_entities(response, property_resolver):
    ''' Converts the response to tables class.
    '''
    if response is None:
        return response

    entities = _list()

    _set_continuation_from_response_headers(entities, response)

    root = loads(response.body.decode('utf-8'))

    if 'value' in root:
        for entity in root['value']:
            entities.append(_convert_json_to_entity(entity, 
                                                 property_resolver))
    else:
        entities.append(_convert_json_to_entity(entity, 
                                             property_resolver))

    return entities
