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
from .._serialization import _update_storage_header
from .._common_conversion import _str

def _update_storage_file_header(request, authentication):
    request = _update_storage_header(request)
    authentication.sign_request(request)

    return request.headers

def _get_path(share_name=None, directory_name=None, file_name=None):
    '''
    Creates the path to access a file resource.

    share_name:
        Name of share.
    directory_name:
        The path to the directory.
    file_name:
        Name of file.
    '''
    if share_name and directory_name and file_name:
        return '/{0}/{1}/{2}'.format(
            _str(share_name),
            _str(directory_name),
            _str(file_name))
    elif share_name and directory_name:
        return '/{0}/{1}'.format(
            _str(share_name),
            _str(directory_name))
    elif share_name and file_name:
        return '/{0}/{1}'.format(
            _str(share_name),
            _str(file_name))
    elif share_name:
        return '/{0}'.format(_str(share_name))
    else:
        return '/'
