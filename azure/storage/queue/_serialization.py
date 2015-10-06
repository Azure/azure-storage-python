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
import types
import sys
if sys.version_info >= (3,):
    from io import BytesIO
else:
    from cStringIO import StringIO as BytesIO

try:
    from xml.etree import cElementTree as ETree
except ImportError:
    from xml.etree import ElementTree as ETree

from time import time
from wsgiref.handlers import format_date_time
from .._serialization import _update_storage_header
from .._common_serialization import (
    xml_escape,
)
from .._common_conversion import (
    _str,
)

def _update_storage_queue_header(request, authentication):
    request = _update_storage_header(request)
    current_time = format_date_time(time())
    request.headers.append(('x-ms-date', current_time))
    request.headers.append(
        ('Content-Type', 'application/octet-stream Charset=UTF-8'))
    authentication.sign_request(request)

    return request.headers

def _convert_queue_message_xml(message_text):
    '''
    <?xml version="1.0" encoding="utf-8"?>
    <QueueMessage>
        <MessageText></MessageText>
    </QueueMessage>
    '''
    queue_message_element = ETree.Element('QueueMessage');

    # Enabled
    message_text = xml_escape(_str(message_text))
    ETree.SubElement(queue_message_element, 'MessageText').text = message_text

    # Add xml declaration and serialize
    with BytesIO() as stream:
        ETree.ElementTree(queue_message_element).write(stream, xml_declaration=True, encoding='utf-8', method='xml')
        output = stream.getvalue()

    return output
