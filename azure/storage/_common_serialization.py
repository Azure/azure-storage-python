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
import ast
import sys
import warnings

if sys.version_info < (3,):
    from urllib2 import quote as url_quote
    from urllib2 import unquote as url_unquote
else:
    from urllib.parse import quote as url_quote
    from urllib.parse import unquote as url_unquote

from datetime import datetime
from dateutil.tz import tzutc
from xml.sax.saxutils import escape as xml_escape

try:
    from xml.etree import cElementTree as ETree
except ImportError:
    from xml.etree import ElementTree as ETree

from ._common_models import (
    WindowsAzureData,
    _Base64String,
    HeaderDict,
    _unicode_type,
    _dict_of,
    _list_of,
    _scalar_list_of,
    _xml_attribute,
)
from ._common_conversion import (
    _decode_base64_to_text,
)
from ._common_error import (
    _ERROR_VALUE_SHOULD_BE_BYTES,
    _WARNING_VALUE_SHOULD_BE_BYTES,
)


def _get_etree_text(element):
    text = element.text
    return text if text is not None else ''

def _to_datetime(strtime):
    return datetime.strptime(strtime, "%Y-%m-%dT%H:%M:%S.%f")

def _to_utc_datetime(value):
    # Azure expects the date value passed in to be UTC.
    # Azure will always return values as UTC.
    # If a date is passed in without timezone info, it is assumed to be UTC.
    if value.tzinfo:
        value = value.astimezone(tzutc())
    return value.strftime('%Y-%m-%dT%H:%M:%SZ')

_KNOWN_SERIALIZATION_XFORMS = {
    'include_apis': 'IncludeAPIs',
    'message_id': 'MessageId',
    'content_md5': 'Content-MD5',
    'last_modified': 'Last-Modified',
    'cache_control': 'Cache-Control',
    'copy_id': 'CopyId',
}


def _get_serialization_name(element_name):
    """converts a Python name into a serializable name"""
    known = _KNOWN_SERIALIZATION_XFORMS.get(element_name)
    if known is not None:
        return known

    if element_name.startswith('x_ms_'):
        return element_name.replace('_', '-')
    if element_name.endswith('_id'):
        element_name = element_name.replace('_id', 'ID')
    for name in ['content_', 'last_modified', 'if_', 'cache_control']:
        if element_name.startswith(name):
            element_name = element_name.replace('_', '-_')

    return ''.join(name.capitalize() for name in element_name.split('_'))


def _get_request_body_bytes_only(param_name, param_value):
    '''Validates the request body passed in and converts it to bytes
    if our policy allows it.'''
    if param_value is None:
        return b''

    if isinstance(param_value, bytes):
        return param_value

    raise TypeError(_ERROR_VALUE_SHOULD_BE_BYTES.format(param_name))


def _get_request_body(request_body):
    '''Converts an object into a request body.  If it's None
    we'll return an empty string, if it's one of our objects it'll
    convert it to XML and return it.  Otherwise we just use the object
    directly'''
    if request_body is None:
        return b''

    if isinstance(request_body, bytes):
        return request_body

    if isinstance(request_body, _unicode_type):
        return request_body.encode('utf-8')

    request_body = str(request_body)
    if isinstance(request_body, _unicode_type):
        return request_body.encode('utf-8')

    return request_body


def _update_request_uri_query_local_storage(request, use_local_storage):
    ''' create correct uri and query for the request '''
    uri, query = _update_request_uri_query(request)
    if use_local_storage:
        return '/' + DEV_ACCOUNT_NAME + uri, query
    return uri, query


def _update_request_uri_query(request):
    '''pulls the query string out of the URI and moves it into
    the query portion of the request object.  If there are already
    query parameters on the request the parameters in the URI will
    appear after the existing parameters'''

    if '?' in request.path:
        request.path, _, query_string = request.path.partition('?')
        if query_string:
            query_params = query_string.split('&')
            for query in query_params:
                if '=' in query:
                    name, _, value = query.partition('=')
                    request.query.append((name, value))

    request.path = url_quote(request.path, '/()$=\',')

    # add encoded queries to request.path.
    if request.query:
        request.path += '?'
        for name, value in request.query:
            if value is not None:
                request.path += name + '=' + url_quote(value, '/()$=\',') + '&'
        request.path = request.path[:-1]

    return request.path, request.query


def _parse_response_for_dict(response):
    ''' Extracts name-values from response header. Filter out the standard
    http headers.'''

    if response is None:
        return None
    http_headers = ['server', 'date', 'location', 'host',
                    'via', 'proxy-connection', 'connection']
    return_dict = HeaderDict()
    if response.headers:
        for name, value in response.headers:
            if not name.lower() in http_headers:
                return_dict[name] = value

    return return_dict


def _parse_response_for_dict_prefix(response, prefixes):
    ''' Extracts name-values for names starting with prefix from response
    header. Filter out the standard http headers.'''

    if response is None:
        return None
    return_dict = {}
    orig_dict = _parse_response_for_dict(response)
    if orig_dict:
        for name, value in orig_dict.items():
            for prefix_value in prefixes:
                if name.lower().startswith(prefix_value.lower()):
                    return_dict[name] = value
                    break
        return return_dict
    else:
        return None


def _parse_response_for_dict_filter(response, filter):
    ''' Extracts name-values for names in filter from response header. Filter
    out the standard http headers.'''
    if response is None:
        return None
    return_dict = {}
    orig_dict = _parse_response_for_dict(response)
    if orig_dict:
        for name, value in orig_dict.items():
            if name.lower() in filter:
                return_dict[name] = value
        return return_dict
    else:
        return None

    
def _extract_etag(response):
    ''' Extracts the etag from the response headers. '''
    if response and response.headers:
        for name, value in response.headers:
            if name.lower() == 'etag':
                return value

    return None