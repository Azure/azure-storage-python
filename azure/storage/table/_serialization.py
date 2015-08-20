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

from datetime import datetime
from dateutil.tz import tzutc
from time import time
from wsgiref.handlers import format_date_time
from json import (
    dumps,
)
from math import(
    isnan,
    isinf,
)
from .._common_conversion import (
    _encode_base64,
    _str,
    _str_or_none,
)
from .._serialization import _update_storage_header
from ._error import (
    _ERROR_CANNOT_SERIALIZE_VALUE_TO_ENTITY,
    _ERROR_INT32_VALUE_TOO_LARGE,
)
from .models import (
    Entity,
    EntityProperty,
    TablePayloadFormat,
    EdmType,
)


_DEFAULT_ACCEPT_HEADER = ('Accept', TablePayloadFormat.JSON_MINIMAL_METADATA)
_DEFAULT_CONTENT_TYPE_HEADER = ('Content-Type', 'application/json')
_DEFAULT_PREFER_HEADER = ('Prefer', 'return-no-content')


def _get_entity_path(table_name, partition_key, row_key):
    return '/{0}(PartitionKey=\'{1}\',RowKey=\'{2}\')'.format(
            _str(table_name), 
            _str(partition_key), 
            _str(row_key))


def _update_storage_table_header(request):
    ''' add additional headers for storage table request. '''

    request = _update_storage_header(request)

    # set service version
    request.headers.append(('DataServiceVersion', '3.0;NetFx'))
    request.headers.append(('MaxDataServiceVersion', '3.0'))

    # set date
    current_time = format_date_time(time())
    request.headers.append(('x-ms-date', current_time))
    request.headers.append(('Date', current_time))
    return request.headers


def _to_entity_int(data):
    int_max = (2 << 30) - 1
    if data > (int_max) or data < (int_max + 1) * (-1):
        raise TypeError(_ERROR_INT32_VALUE_TOO_LARGE.format(data))
    else:
        return None, data


def _to_entity_bool(value):
    return None, value


def _to_entity_datetime(value):
    # Azure expects the date value passed in to be UTC.
    # Azure will always return values as UTC.
    # If a date is passed in without timezone info, it is assumed to be UTC.
    if value.tzinfo:
        value = value.astimezone(tzutc())
    return EdmType.DATETIME, value.strftime('%Y-%m-%dT%H:%M:%SZ')


def _to_entity_float(value):
    if isnan(value):
        return EdmType.DOUBLE, 'NaN'
    if value == float('inf'):
        return EdmType.DOUBLE, 'Infinity'
    if value == float('-inf'):
        return EdmType.DOUBLE, '-Infinity'
    return None, value


def _to_entity_property(value):
    if value.type == EdmType.BINARY:
        return value.type, _encode_base64(value.value)

    return value.type, str(value.value)


def _to_entity_none(value):
    return None, None


def _to_entity_str(value):
    return None, value

# Conversion from Python type to a function which returns a tuple of the
# type string and content string.
_PYTHON_TO_ENTITY_CONVERSIONS = {
    int: _to_entity_int,
    bool: _to_entity_bool,
    datetime: _to_entity_datetime,
    float: _to_entity_float,
    EntityProperty: _to_entity_property,
    str: _to_entity_str,
}

if sys.version_info < (3,):
    _PYTHON_TO_ENTITY_CONVERSIONS.update({
        long: _to_entity_int,
        types.NoneType: _to_entity_none,
        unicode: _to_entity_str,
    })


def _convert_entity_to_json(source):
    ''' Converts an entity object to json to send.
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

    if isinstance(source, Entity):
        source = vars(source)

    properties = {}

    # set properties type for types we know if value has no type info.
    # if value has type info, then set the type to value.type
    for name, value in source.items():
        mtype = ''
        conv = _PYTHON_TO_ENTITY_CONVERSIONS.get(type(value))
        if conv is None and sys.version_info >= (3,) and value is None:
            conv = _to_entity_none
        if conv is None:
            raise TypeError(
                _ERROR_CANNOT_SERIALIZE_VALUE_TO_ENTITY.format(
                    type(value).__name__))

        mtype, value = conv(value)

        # form the property node
        properties[name] = value
        if mtype:
            properties[name + '@odata.type'] = mtype

    # generate the entity_body
    return dumps(properties)


def _convert_table_to_json(table_name):
    '''
    Create json to send for a given table name. Since json format for table is
    the same as entity and the only difference is that table has only one
    property 'TableName', so we just call _convert_entity_to_json.

    table_name:
        the name of the table
    '''
    return _convert_entity_to_json({'TableName': table_name})