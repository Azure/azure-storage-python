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
from xml.sax.saxutils import escape as xml_escape
try:
    from xml.etree import cElementTree as ETree
except ImportError:
    from xml.etree import ElementTree as ETree
from .._common_conversion import (
    _encode_base64,
    _str,
)
import sys
if sys.version_info >= (3,):
    from io import BytesIO
else:
    from cStringIO import StringIO as BytesIO

def _get_path(container_name=None, blob_name=None):
    '''
    Creates the path to access a blob resource.

    container_name:
        Name of container.
    blob_name:
        The path to the blob.
    '''
    if container_name and blob_name:
        return '/{0}/{1}'.format(
            _str(container_name),
            _str(blob_name))
    elif container_name:
        return '/{0}'.format(_str(container_name))
    else:
        return '/'

def _convert_block_list_to_xml(block_id_list):
    '''
    <?xml version="1.0" encoding="utf-8"?>
    <BlockList>
      <Committed>first-base64-encoded-block-id</Committed>
      <Uncommitted>second-base64-encoded-block-id</Uncommitted>
      <Latest>third-base64-encoded-block-id</Latest>
    </BlockList>

    Convert a block list to xml to send.

    block_id_list:
        A list of BlobBlock containing the block ids and block state that are used in put_block_list.
    Only get block from latest blocks.
    '''
    if block_id_list is None:
        return ''

    block_list_element = ETree.Element('BlockList');
    
    # Enabled
    for block in block_id_list:
        if block.id is None:
            raise ValueError("All blocks in block list need to have valid block ids.")
        id = xml_escape(_str(format(_encode_base64(block.id))))
        ETree.SubElement(block_list_element, block.state).text = id

    # Add xml declaration and serialize
    try:
        stream = BytesIO()
        ETree.ElementTree(block_list_element).write(stream, xml_declaration=True, encoding='utf-8', method='xml')
    except:
        raise
    finally:
        output = stream.getvalue()
        stream.close()
    
    # return xml value
    return output
