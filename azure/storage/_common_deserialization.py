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
from dateutil import parser
from ._common_conversion import _str_or_none

def _int_or_none(value):
    return value if value is None else int(value)

GET_PROPERTIES_ATTRIBUTE_MAP = {
    'last-modified': (None, 'last_modified', parser.parse),
    'etag': (None, 'etag', _str_or_none),
    'x-ms-blob-type': (None, 'blob_type', _str_or_none),
    'content-length': (None, 'content_length', _int_or_none),
    'x-ms-blob-sequence-number': (None, 'page_blob_sequence_number', _int_or_none),
    'x-ms-blob-committed-block-count': (None, 'append_blob_committed_block_count', _int_or_none),
    'content-type': ('content_settings', 'content_type', _str_or_none),
    'cache-control': ('content_settings', 'cache_control', _str_or_none),
    'content-encoding': ('content_settings', 'content_encoding', _str_or_none),
    'content-disposition': ('content_settings', 'content_disposition', _str_or_none),
    'content-language': ('content_settings', 'content_language', _str_or_none),
    'content-md5': ('content_settings', 'content_md5', _str_or_none),
    'x-ms-lease-status': ('lease', 'status', _str_or_none),
    'x-ms-lease-state': ('lease', 'state', _str_or_none),
    'x-ms-lease-duration': ('lease', 'duration', _str_or_none),
    'x-ms-copy-id': ('copy', 'id', _str_or_none),
    'x-ms-copy-source': ('copy', 'source', _str_or_none),
    'x-ms-copy-status': ('copy', 'status', _str_or_none),
    'x-ms-copy-progress': ('copy', 'progress', _str_or_none),
    'x-ms-copy-completion-time': ('copy', 'completion_time', _str_or_none),
    'x-ms-copy-status-description': ('copy', 'status_description', _str_or_none),
}

def _parse_properties(response, properties_class):
    '''
    Extracts out resource properties and metadata informaiton.
    Ignores the standard http headers.
    '''

    if response is None and response.headers is not None:
        return None, None

    props = properties_class()
    metadata = {}
    for key, value in response.headers:
        info = GET_PROPERTIES_ATTRIBUTE_MAP.get(key)
        if info:
            if info[0] is None:
                setattr(props, info[1], info[2](value))
            else:
                attr = getattr(props, info[0])
                setattr(attr, info[1], info[2](value))
        elif key.startswith('x-ms-meta-'):
            metadata[key] = _str_or_none(value)

    return props, metadata