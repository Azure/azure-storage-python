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
from azure.common import AzureHttpError
from .._common_error import (
    _dont_fail_not_exist,
    _dont_fail_on_exist,
    _validate_not_none,
    _validate_type_bytes,
    _ERROR_VALUE_NEGATIVE,
    _ERROR_STORAGE_MISSING_INFO,
)
from .._common_conversion import (
    _encode_base64,
    _int_or_none,
    _str,
    _str_or_none,
)
from .._common_serialization import (
    _get_request_body,
    _get_request_body_bytes_only,
    _parse_response_for_dict,
    _parse_response_for_dict_filter,
    _parse_response_for_dict_prefix,
    _update_request_uri_query_local_storage,
    _ETreeXmlToObject,
)
from .._http import HTTPRequest
from ._chunking import (
    _download_file_chunks,
    _upload_file_chunks,
)
from .models import (
    Share,
    ShareEnumResults,
    RangeList,
    Range,
)
from ..auth import StorageSharedKeyAuthentication
from ..connection import StorageConnectionParameters
from ..constants import (
    FILE_SERVICE_HOST_BASE,
    DEFAULT_HTTP_TIMEOUT,
    DEV_FILE_HOST,
    X_MS_VERSION,
)
from ._serialization import (
    _create_file_result,
    _parse_file_enum_results_list,
    _update_storage_file_header,
)
from ..sharedaccesssignature import (
    SharedAccessSignature,
    ResourceType,
)
from ..storageclient import _StorageClient
from os import path
import sys
if sys.version_info >= (3,):
    from io import BytesIO
else:
    from cStringIO import StringIO as BytesIO

class FileService(_StorageClient):

    '''
    This is the main class managing File resources.
    '''

    _FILE_MAX_DATA_SIZE = 64 * 1024 * 1024
    _FILE_MAX_CHUNK_DATA_SIZE = 4 * 1024 * 1024

    def __init__(self, account_name=None, account_key=None, protocol='https',
                 host_base=FILE_SERVICE_HOST_BASE, dev_host=DEV_FILE_HOST,
                 timeout=DEFAULT_HTTP_TIMEOUT, connection_string=None,
                 request_session=None):
        '''
        account_name:
            your storage account name, required for all operations.
        account_key:
            your storage account key, required for all operations.
        protocol:
            Protocol. Defaults to https.
        host_base:
            Live host base url. Defaults to Azure url. Override this
            for on-premise.
        dev_host:
            Dev host url. Defaults to localhost.
        timeout:
            Timeout for the http request, in seconds.
        connection_string:
            If specified, the first four parameters (account_name,
            account_key, protocol, host_base) may be overridden
            by values specified in the connection_string. The next three parameters
            (dev_host, timeout) cannot be specified with a
            connection_string. See 
            http://azure.microsoft.com/en-us/documentation/articles/storage-configure-connection-string/
            for the connection string format.
        request_session:
            Session object to use for http requests. If this is
            specified, it replaces the default use of httplib.
        '''
        if connection_string is not None:
            connection_params = StorageConnectionParameters(connection_string)
            account_name = connection_params.account_name
            account_key = connection_params.account_key
            protocol = connection_params.protocol.lower()
            host_base = connection_params.host_base_file
            
        super(FileService, self).__init__(
            account_name, account_key, protocol, host_base, dev_host, timeout, None, request_session)

        if self.account_key:
            self.authentication = StorageSharedKeyAuthentication(
                self.account_name,
                self.account_key,
            )
        else:
            raise TypeError(_ERROR_STORAGE_MISSING_INFO)

    def make_file_url(self, share_name, directory_name, file_name, 
                      account_name=None, protocol=None, host_base=None):
        '''
        Creates the url to access a file.

        share_name:
            Name of share.
        directory_name:
            The path to the directory.
        file_name:
            Name of file.
        account_name:
            Name of the storage account. If not specified, uses the account
            specified when FileService was initialized.
        protocol:
            Protocol to use: 'http' or 'https'. If not specified, uses the
            protocol specified when FileService was initialized.
        host_base:
            Live host base url.  If not specified, uses the host base specified
            when FileService was initialized.
        '''

        if directory_name is None:
            url = '{0}://{1}{2}/{3}/{4}'.format(
                protocol or self.protocol,
                account_name or self.account_name,
                host_base or self.host_base,
                share_name,
                file_name,
            )
        else:
            url = '{0}://{1}{2}/{3}/{4}/{5}'.format(
                protocol or self.protocol,
                account_name or self.account_name,
                host_base or self.host_base,
                share_name,
                directory_name,
                file_name,
            )

        return url

    def list_shares(self, prefix=None, marker=None, max_results=None,
                    include=None):
        '''
        The List Shares operation returns a list of the shares under
        the specified account.

        prefix:
            Filters the results to return only shares whose names
            begin with the specified prefix.
        marker:
            A string value that identifies the portion of the list to
            be returned with the next list operation.
        max_results:
            Specifies the maximum number of shares to return.
        include:
            Include this parameter to specify that the share's
            metadata be returned as part of the response body. set this
            parameter to string 'metadata' to get share's metadata.
        '''
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = '/?comp=list'
        request.query = [
            ('prefix', _str_or_none(prefix)),
            ('marker', _str_or_none(marker)),
            ('maxresults', _int_or_none(max_results)),
            ('include', _str_or_none(include))
        ]
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_file_header(
            request, self.authentication)
        response = self._perform_request(request)

        return _ETreeXmlToObject.parse_enum_results_list(
            response, ShareEnumResults, "Shares", Share)

    def create_share(self, share_name, metadata=None,
                     fail_on_exist=False):
        '''
        Creates a new share under the specified account. If the share
        with the same name already exists, the operation fails on the
        service. By default, the exception is swallowed by the client.
        To expose the exception, specify True for fail_on_exists.

        share_name:
            Name of share to create.
        metadata:
            A dict with name_value pairs to associate with the
            share as metadata. Example:{'Category':'test'}
        fail_on_exist:
            Specify whether to throw an exception when the share exists.
            False by default.
        '''
        _validate_not_none('share_name', share_name)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = '/' + _str(share_name) + '?restype=share'
        request.headers = [('x-ms-meta-name-values', metadata)]
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_file_header(
            request, self.authentication)
        if not fail_on_exist:
            try:
                self._perform_request(request)
                return True
            except AzureHttpError as ex:
                _dont_fail_on_exist(ex)
                return False
        else:
            self._perform_request(request)
            return True

    def get_share_properties(self, share_name):
        '''
        Returns all user-defined metadata and system properties for the
        specified share.

        share_name:
            Name of existing share.
        '''
        _validate_not_none('share_name', share_name)
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = '/' + _str(share_name) + '?restype=share'
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_file_header(
            request, self.authentication)
        response = self._perform_request(request)

        return _parse_response_for_dict(response)

    def get_share_metadata(self, share_name):
        '''
        Returns all user-defined metadata for the specified share. The
        metadata will be in returned dictionary['x-ms-meta-(name)'].

        share_name:
            Name of existing share.
        '''
        _validate_not_none('share_name', share_name)
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = '/' + \
            _str(share_name) + '?restype=share&comp=metadata'
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_file_header(
            request, self.authentication)
        response = self._perform_request(request)

        return _parse_response_for_dict_prefix(response, prefixes=['x-ms-meta'])

    def set_share_metadata(self, share_name, metadata=None):
        '''
        Sets one or more user-defined name-value pairs for the specified
        share.

        share_name:
            Name of existing share.
        metadata:
            A dict containing name, value for metadata.
            Example: {'category':'test'}
        '''
        _validate_not_none('share_name', share_name)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = '/' + \
            _str(share_name) + '?restype=share&comp=metadata'
        request.headers = [('x-ms-meta-name-values', metadata)]
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_file_header(
            request, self.authentication)
        self._perform_request(request)

    def delete_share(self, share_name, fail_not_exist=False):
        '''
        Marks the specified share for deletion. If the share
        does not exist, the operation fails on the service. By 
        default, the exception is swallowed by the client.
        To expose the exception, specify True for fail_not_exist.

        share_name:
            Name of share to delete.
        fail_not_exist:
            Specify whether to throw an exception when the share doesn't
            exist. False by default.
        '''
        _validate_not_none('share_name', share_name)
        request = HTTPRequest()
        request.method = 'DELETE'
        request.host = self._get_host()
        request.path = '/' + _str(share_name) + '?restype=share'
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_file_header(
            request, self.authentication)
        if not fail_not_exist:
            try:
                self._perform_request(request)
                return True
            except AzureHttpError as ex:
                _dont_fail_not_exist(ex)
                return False
        else:
            self._perform_request(request)
            return True

    def create_directory(self, share_name, directory_name,
                         fail_on_exist=False):
        '''
        Creates a new directory under the specified share or parent directory. 
        If the directory with the same name already exists, the operation fails
        on the service. By default, the exception is swallowed by the client.
        To expose the exception, specify True for fail_on_exists.

        share_name:
            Name of existing share.
        directory_name:
            Name of directory to create, including the path to the parent 
            directory.
        fail_on_exist:
            specify whether to throw an exception when the directory exists.
            False by default.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('directory_name', directory_name)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = '/' + _str(share_name) + \
            '/' + _str(directory_name) + '?restype=directory'
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_file_header(
            request, self.authentication)
        if not fail_on_exist:
            try:
                self._perform_request(request)
                return True
            except AzureHttpError as ex:
                _dont_fail_on_exist(ex)
                return False
        else:
            self._perform_request(request)
            return True

    def delete_directory(self, share_name, directory_name,
                         fail_not_exist=False):
        '''
        Deletes the specified empty directory. Note that the directory must
        be empty before it can be deleted. Attempting to delete directories 
        that are not empty will fail.

        If the directory does not exist, the operation fails on the
        service. By default, the exception is swallowed by the client.
        To expose the exception, specify True for fail_not_exist.

        share_name:
            Name of existing share.
        directory_name:
            Name of directory to create, including the path to the parent 
            directory.
        fail_not_exist:
            Specify whether to throw an exception when the directory doesn't
            exist.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('directory_name', directory_name)
        request = HTTPRequest()
        request.method = 'DELETE'
        request.host = self._get_host()
        request.path = '/' + _str(share_name) + \
            '/' + _str(directory_name) + '?restype=directory'
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_file_header(
            request, self.authentication)
        if not fail_not_exist:
            try:
                self._perform_request(request)
                return True
            except AzureHttpError as ex:
                _dont_fail_not_exist(ex)
                return False
        else:
            self._perform_request(request)
            return True

    def get_directory_properties(self, share_name, directory_name):
        '''
        Returns all user-defined metadata and system properties for the
        specified directory.

        share_name:
            Name of existing share.
        directory_name:
           The path to an existing directory.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('directory_name', directory_name)
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = '/' + _str(share_name) + \
            '/' + _str(directory_name) + '?restype=directory'
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_file_header(
            request, self.authentication)
        response = self._perform_request(request)

        return _parse_response_for_dict(response)

    def list_directories_and_files(self, share_name, directory_name=None, 
                                   marker=None, max_results=None):
        '''
        Returns a list of files or directories under the specified share or 
        directory. It lists the contents only for a single level of the directory 
        hierarchy.

        share_name:
            Name of existing share.
        directory_name:
            The path to the directory.
        marker:
            A string value that identifies the portion of the list
            to be returned with the next list operation. The operation returns
            a marker value within the response body if the list returned was
            not complete. The marker value may then be used in a subsequent
            call to request the next set of list items. The marker value is
            opaque to the client.
        max_results:
            Specifies the maximum number of files to return,
            including all directory elements. If the request does not specify
            max_results or specifies a value greater than 5,000, the server will
            return up to 5,000 items. Setting max_results to a value less than
            or equal to zero results in error response code 400 (Bad Request).
        '''
        _validate_not_none('share_name', share_name)
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = '/' + _str(share_name)
        if directory_name is not None:
            request.path += '/' + _str(directory_name)
        request.path += '?restype=directory&comp=list'
        request.query = [
            ('marker', _str_or_none(marker)),
            ('maxresults', _int_or_none(max_results)),
        ]
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_file_header(
            request, self.authentication)
        response = self._perform_request(request)

        return _parse_file_enum_results_list(response)

    def get_file_properties(self, share_name, directory_name, file_name):
        '''
        Returns all user-defined metadata, standard HTTP properties, and
        system properties for the file.

        share_name:
            Name of existing share.
        directory_name:
            The path to the directory.
        file_name:
            Name of existing file.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('file_name', file_name)
        request = HTTPRequest()
        request.method = 'HEAD'
        request.host = self._get_host()
        request.path = '/' + _str(share_name)
        if directory_name is not None:
            request.path += '/' + _str(directory_name)
        request.path += '/' + _str(file_name) + ''
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_file_header(
            request, self.authentication)

        response = self._perform_request(request)

        return _parse_response_for_dict(response)

    def resize_file(self, share_name, directory_name, 
                    file_name, content_length):
        '''
        Resizes a file to the specified size.

        share_name:
            Name of existing share.
        directory_name:
            The path to the directory.
        file_name:
            Name of existing file.
        content-length:
            The length to resize the file to. If the specified byte 
            value is less than the current size of the file,
            then all ranges above the specified byte value 
            are cleared.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('file_name', file_name)
        _validate_not_none('content_length', content_length)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = '/' + _str(share_name)
        if directory_name is not None:
            request.path += '/' + _str(directory_name)
        request.path += '/' + _str(file_name) + '?comp=properties'
        request.headers = [
            ('x-ms-content-length', _str_or_none(content_length))]
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_file_header(
            request, self.authentication)
        self._perform_request(request)

    def set_file_properties(self, share_name, directory_name, 
                            file_name, cache_control=None,
                            content_type=None, content_md5=None,
                            content_encoding=None, content_language=None,
                            content_disposition=None):
        '''
        Sets system properties on the file.

        share_name:
            Name of existing share.
        directory_name:
            The path to the directory.
        file_name:
            Name of existing file.
        cache_control:
            Modifies the cache control string for the file.
        content_type:
            Sets the file's content type.
        content_md5:
            Sets the file's MD5 hash.
        content_encoding:
            Sets the file's content encoding.
        content_language:
            Sets the file's content language.
        content_disposition:
            Sets the file's Content-Disposition header.
            The Content-Disposition response header field conveys additional
            information about how to process the response payload, and also can
            be used to attach additional metadata. For example, if set to
            attachment, it indicates that the user-agent should not display the
            response, but instead show a Save As dialog with a filename other
            than the file name specified.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('file_name', file_name)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = '/' + _str(share_name)
        if directory_name is not None:
            request.path += '/' + _str(directory_name)
        request.path += '/' + _str(file_name) + '?comp=properties'
        request.headers = [
            ('x-ms-cache-control', _str_or_none(cache_control)),
            ('x-ms-content-type', _str_or_none(content_type)),
            ('x-ms-content-disposition',
             _str_or_none(content_disposition)),
            ('x-ms-content-md5', _str_or_none(content_md5)),
            ('x-ms-content-encoding',
             _str_or_none(content_encoding)),
            ('x-ms-content-language',
             _str_or_none(content_language))]
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_file_header(
            request, self.authentication)
        self._perform_request(request)

    def get_file_metadata(self, share_name, directory_name, file_name):
        '''
        Returns all user-defined metadata for the specified file.

        share_name:
            Name of existing share.
        directory_name:
            The path to the directory.
        file_name:
            Name of existing file.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('file_name', file_name)
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = '/' + _str(share_name)
        if directory_name is not None:
            request.path += '/' + _str(directory_name)
        request.path += '/' + _str(file_name) + '?comp=metadata'
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_file_header(
            request, self.authentication)
        response = self._perform_request(request)

        return _parse_response_for_dict_prefix(response, prefixes=['x-ms-meta'])

    def set_file_metadata(self, share_name, directory_name, 
                          file_name, metadata=None):
        '''
        Sets user-defined metadata for the specified file as one or more
        name-value pairs.

        share_name:
            Name of existing share.
        directory_name:
            The path to the directory.
        file_name:
            Name of existing file.
        metadata:
            Dict containing name and value pairs.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('file_name', file_name)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = '/' + _str(share_name)
        if directory_name is not None:
            request.path += '/' + _str(directory_name)
        request.path += '/' + _str(file_name) + '?comp=metadata'
        request.headers = [('x-ms-meta-name-values', metadata)]
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_file_header(
            request, self.authentication)
        self._perform_request(request)

    def delete_file(self, share_name, directory_name, file_name):
        '''
        Marks the specified file for deletion. The file is later
        deleted during garbage collection.

        share_name:
            Name of existing share.
        directory_name:
            The path to the directory.
        file_name:
            Name of existing file.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('file_name', file_name)
        request = HTTPRequest()
        request.method = 'DELETE'
        request.host = self._get_host()
        request.path = '/' + _str(share_name)
        if directory_name is not None:
            request.path += '/' + _str(directory_name)
        request.path += '/' + _str(file_name) + ''
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_file_header(
            request, self.authentication)
        self._perform_request(request)

    def create_file(self, share_name, directory_name, file_name,
                    content_length, content_encoding=None, 
                    content_language=None, content_md5=None, cache_control=None,
                    content_type=None, metadata=None):
        '''
        Creates a new block file or range file, or updates the content of an
        existing block file.

        See put_block_file_from_* and put_file_range_from_* for high level
        functions that handle the creation and upload of large files with
        automatic chunking and progress notifications.

        share_name:
            Name of existing share.
        directory_name:
            The path to the directory.
        file_name:
            Name of file to create or update.
        content_length:
            Required. This header specifies the maximum size
            for the file, up to 1 TB.
        content_encoding:
            Set the file's content encoding.
        content_language:
            Set the file's content language.
        content_md5:
            Set the file's MD5 hash.
        cache_control:
            Sets the file's cache control.
        content_type:
            Set the file's content type.
        metadata:
            A dict containing name, value for metadata.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('file_name', file_name)
        _validate_not_none('x_ms_content_length', content_length)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = '/' + _str(share_name)
        if directory_name is not None:
            request.path += '/' + _str(directory_name)
        request.path += '/' + _str(file_name) + ''
        request.headers = [
            ('x-ms-content-type', _str_or_none(content_type)),
            ('x-ms-content-encoding', _str_or_none(content_encoding)),
            ('x-ms-content-language', _str_or_none(content_language)),
            ('x-ms-content-md5', _str_or_none(content_md5)),
            ('x-ms-cache-control', _str_or_none(cache_control)),
            ('x-ms-meta-name-values', metadata),
            ('x-ms-content-length', _str_or_none(content_length)),
            ('x-ms-type', 'file')
        ]
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_file_header(
            request, self.authentication)
        self._perform_request(request)

    def create_file_from_path(self, share_name, directory_name, file_name, 
                           local_file_path, content_type=None,
                           content_encoding=None, content_language=None,
                           content_md5=None, cache_control=None,
                           metadata=None, progress_callback=None,
                           max_connections=1, max_retries=5, retry_wait=1.0):
        '''
        Creates a new azure file from a local file path, or updates the content of an
        existing file, with automatic chunking and progress notifications.

        share_name:
            Name of existing share.
        directory_name:
            The path to the directory.
        file_name:
            Name of file to create or update.
        local_file_path:
            Path of the local file to upload as the file content.
        content_type:
            Set the file's content type.
        content_encoding:
            Set the file's content encoding.
        content_language:
            Set the file's content language.
        content_md5:
            Set the file's MD5 hash.
        cache_control:
            Sets the file's cache control.
        metadata:
            A dict containing name, value for metadata.
        progress_callback:
            Callback for progress with signature function(current, total) where
            current is the number of bytes transfered so far, and total is the
            size of the file, or None if the total size is unknown.
        max_connections:
            Maximum number of parallel connections to use when the file size
            exceeds 64MB.
            Set to 1 to upload the file chunks sequentially.
            Set to 2 or more to upload the file chunks in parallel. This uses
            more system resources but will upload faster.
        max_retries:
            Number of times to retry upload of file chunk if an error occurs.
        retry_wait:
            Sleep time in secs between retries.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('file_name', file_name)
        _validate_not_none('local_file_path', local_file_path)

        count = path.getsize(local_file_path)
        with open(local_file_path, 'rb') as stream:
            self.create_file_from_stream(
                share_name, directory_name, file_name, stream,
                count, content_type, content_encoding,
                content_language, content_md5, cache_control,
                metadata, progress_callback,
                max_connections, max_retries, retry_wait)

    def create_file_from_text(self, share_name, directory_name, file_name, 
                           text, encoding='utf-8', content_type=None,
                           content_encoding=None, content_language=None,
                           content_md5=None, cache_control=None,
                           metadata=None):
        '''
        Creates a new file from str/unicode, or updates the content of an
        existing file.

        share_name:
            Name of existing share.
        directory_name:
            The path to the directory.
        file_name:
            Name of blob to create or update.
        text:
            Text to upload to the blob.
        encoding:
            Encoding to use to convert the text to bytes.
        content_type:
            Set the blob's content type.
        content_encoding:
            Set the blob's content encoding.
        content_language:
            Set the blob's content language.
        content_md5:
            Set the blob's MD5 hash.
        cache_control:
            Sets the blob's cache control.
        metadata:
            A dict containing name, value for metadata.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('file_name', file_name)
        _validate_not_none('text', text)

        if not isinstance(text, bytes):
            _validate_not_none('encoding', encoding)
            text = text.encode(encoding)

        self.create_file_from_bytes(
            share_name, directory_name, file_name, text, 0,
            len(text), content_type, content_encoding,
            content_language, content_md5, cache_control,
            metadata)

    def create_file_from_bytes(
        self, share_name, directory_name, file_name, file,
        index=0, count=None, content_type=None,
        content_encoding=None, content_language=None,
        content_md5=None, cache_control=None, metadata=None,
        progress_callback=None, max_connections=1, max_retries=5,
        retry_wait=1.0):
        '''
        Creates a new page file from an array of bytes, or updates the content
        of an existing page file, with automatic chunking and progress
        notifications.

        share_name:
            Name of existing share.
        directory_name:
            The path to the directory.
        file_name:
            Name of file to create or update.
        file:
            Content of file as an array of bytes.
        index:
            Start index in the array of bytes.
        count:
            Number of bytes to upload. Set to None or negative value to upload
            all bytes starting from index.
        content_type:
            Set the file's content type.
        content_encoding:
            Set the file's content encoding.
        content_language:
            Set the file's content language.
        content_md5:
            Set the file's MD5 hash.
        cache_control:
            Sets the file's cache control.
        metadata:
            A dict containing name, value for metadata.
        progress_callback:
            Callback for progress with signature function(current, total) where
            current is the number of bytes transfered so far, and total is the
            size of the file, or None if the total size is unknown.
        max_connections:
            Maximum number of parallel connections to use when the file size
            exceeds 64MB.
            Set to 1 to upload the file chunks sequentially.
            Set to 2 or more to upload the file chunks in parallel. This uses
            more system resources but will upload faster.
        max_retries:
            Number of times to retry upload of file chunk if an error occurs.
        retry_wait:
            Sleep time in secs between retries.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('file_name', file_name)
        _validate_not_none('file', file)
        _validate_type_bytes('file', file)

        if index < 0:
            raise TypeError(_ERROR_VALUE_NEGATIVE.format('index'))

        if count is None or count < 0:
            count = len(file) - index

        stream = BytesIO(file)
        stream.seek(index)

        self.create_file_from_stream(
            share_name, directory_name, file_name, stream, count,
            content_type, content_encoding, content_language,
            content_md5, cache_control, metadata,
            progress_callback, max_connections, max_retries,
            retry_wait)

    def create_file_from_stream(
        self, share_name, directory_name, file_name, stream, count,
        content_type=None, content_encoding=None, content_language=None,
        content_md5=None, cache_control=None, metadata=None,
        progress_callback=None, max_connections=1, max_retries=5,
        retry_wait=1.0):
        '''
        Creates a new page file from a file/stream, or updates the content of an
        existing page file, with automatic chunking and progress notifications.

        share_name:
            Name of existing share.
        directory_name:
            The path to the directory.
        file_name:
            Name of file to create or update.
        stream:
            Opened file/stream to upload as the file content.
        count:
            Number of bytes to read from the stream. This is required, a page
            file cannot be created if the count is unknown.
        content_type:
            Set the file's content type.
        content_encoding:
            Set the file's content encoding.
        content_language:
            Set the file's content language.
        content_md5:
            Set the file's MD5 hash.
        cache_control:
            Sets the file's cache control.
        metadata:
            A dict containing name, value for metadata.
        progress_callback:
            Callback for progress with signature function(current, total) where
            current is the number of bytes transfered so far, and total is the
            size of the file, or None if the total size is unknown.
        max_connections:
            Maximum number of parallel connections to use when the file size
            exceeds 64MB.
            Set to 1 to upload the file chunks sequentially.
            Set to 2 or more to upload the file chunks in parallel. This uses
            more system resources but will upload faster.
            Note that parallel upload requires the stream to be seekable.
        max_retries:
            Number of times to retry upload of file chunk if an error occurs.
        retry_wait:
            Sleep time in secs between retries.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('file_name', file_name)
        _validate_not_none('stream', stream)
        _validate_not_none('count', count)

        if count < 0:
            raise TypeError(_ERROR_VALUE_NEGATIVE.format('count'))

        self.create_file(
            share_name,
            directory_name,
            file_name,
            count,
            content_type,
            content_encoding,
            content_language,
            content_md5,
            cache_control,
            metadata
        )

        _upload_file_chunks(
            self,
            share_name,
            directory_name,
            file_name,
            count,
            self._FILE_MAX_CHUNK_DATA_SIZE,
            stream,
            max_connections,
            max_retries,
            retry_wait,
            progress_callback,
        )

    def get_file(self, share_name, directory_name, file_name, byte_range=None, 
                 range_get_content_md5=None):
        '''
        Reads or downloads a file from the system, including its metadata and
        properties.

        See get_file_to_* for high level functions that handle the download
        of large files with automatic chunking and progress notifications.

        share_name:
            Name of existing share.
        directory_name:
            The path to the directory.
        file_name:
            Name of existing file.
        byte_range:
            Return only the bytes of the file in the specified range.
        range_get_content_md5:
            When this header is set to true and specified together
            with the Range header, the service returns the MD5 hash for the
            range, as long as the range is less than or equal to 4 MB in size.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('file_name', file_name)
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = '/' + _str(share_name)
        if directory_name is not None:
            request.path += '/' + _str(directory_name)
        request.path += '/' + _str(file_name) + ''
        request.headers = [
            ('x-ms-range', _str_or_none(byte_range)),
            ('x-ms-range-get-content-md5',
             _str_or_none(range_get_content_md5))
        ]
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_file_header(
            request, self.authentication)
        response = self._perform_request(request, None)

        return _create_file_result(response)

    def get_file_to_path(self, share_name, directory_name, file_name, file_path,
                         open_mode='wb', progress_callback=None,
                         max_connections=1, max_retries=5, retry_wait=1.0):
        '''
        Downloads a file to a file path, with automatic chunking and progress
        notifications.

        share_name:
            Name of existing share.
        directory_name:
            The path to the directory.
        file_name:
            Name of existing file.
        file_path:
            Path of file to write to.
        open_mode:
            Mode to use when opening the file.
        progress_callback:
            Callback for progress with signature function(current, total) where
            current is the number of bytes transfered so far, and total is the
            size of the file.
        max_connections:
            Maximum number of parallel connections to use when the file size
            exceeds 64MB.
            Set to 1 to download the file chunks sequentially.
            Set to 2 or more to download the file chunks in parallel. This uses
            more system resources but will download faster.
        max_retries:
            Number of times to retry download of file chunk if an error occurs.
        retry_wait:
            Sleep time in secs between retries.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('file_name', file_name)
        _validate_not_none('file_path', file_path)
        _validate_not_none('open_mode', open_mode)

        with open(file_path, open_mode) as stream:
            self.get_file_to_stream(
                share_name, directory_name, file_name, stream,
                progress_callback, max_connections, max_retries,
                retry_wait)

    def get_file_to_stream(
        self, share_name, directory_name, file_name, stream,
        progress_callback=None, max_connections=1, max_retries=5,
        retry_wait=1.0):
        '''
        Downloads a file to a file/stream, with automatic chunking and progress
        notifications.

        share_name:
            Name of existing share.
        directory_name:
            The path to the directory.
        file_name:
            Name of existing file.
        stream:
            Opened file/stream to write to.
        progress_callback:
            Callback for progress with signature function(current, total) where
            current is the number of bytes transfered so far, and total is the
            size of the file.
        max_connections:
            Maximum number of parallel connections to use when the file size
            exceeds 64MB.
            Set to 1 to download the file chunks sequentially.
            Set to 2 or more to download the file chunks in parallel. This uses
            more system resources but will download faster.
            Note that parallel download requires the stream to be seekable.
        max_retries:
            Number of times to retry download of file chunk if an error occurs.
        retry_wait:
            Sleep time in secs between retries.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('file_name', file_name)
        _validate_not_none('stream', stream)

        props = self.get_file_properties(share_name, directory_name, file_name)
        file_size = int(props['content-length'])

        if file_size < self._FILE_MAX_DATA_SIZE:
            if progress_callback:
                progress_callback(0, file_size)

            data = self.get_file(share_name, directory_name,
                                 file_name)

            stream.write(data)

            if progress_callback:
                progress_callback(file_size, file_size)
        else:
            _download_file_chunks(
                self,
                share_name,
                directory_name,
                file_name,
                file_size,
                self._FILE_MAX_CHUNK_DATA_SIZE,
                stream,
                max_connections,
                max_retries,
                retry_wait,
                progress_callback
            )

    def get_file_to_bytes(self, share_name, directory_name, file_name, progress_callback=None,
                          max_connections=1, max_retries=5, retry_wait=1.0):
        '''
        Downloads a file as an array of bytes, with automatic chunking and
        progress notifications.

        share_name:
            Name of existing share.
        directory_name:
            The path to the directory.
        file_name:
            Name of existing file.
        progress_callback:
            Callback for progress with signature function(current, total) where
            current is the number of bytes transfered so far, and total is the
            size of the file.
        max_connections:
            Maximum number of parallel connections to use when the file size
            exceeds 64MB.
            Set to 1 to download the file chunks sequentially.
            Set to 2 or more to download the file chunks in parallel. This uses
            more system resources but will download faster.
        max_retries:
            Number of times to retry download of file chunk if an error occurs.
        retry_wait:
            Sleep time in secs between retries.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('file_name', file_name)

        stream = BytesIO()
        self.get_file_to_stream(share_name,
                              directory_name,
                              file_name,
                              stream,
                              progress_callback,
                              max_connections,
                              max_retries,
                              retry_wait)

        return stream.getvalue()

    def get_file_to_text(
        self, share_name, directory_name, file_name, encoding='utf-8',
        progress_callback=None, max_connections=1, max_retries=5,
        retry_wait=1.0):
        '''
        Downloads a file as unicode text, with automatic chunking and progress
        notifications.

        share_name:
            Name of existing share.
        directory_name:
            The path to the directory.
        file_name:
            Name of existing file.
        encoding:
            Python encoding to use when decoding the file data.
        progress_callback:
            Callback for progress with signature function(current, total) where
            current is the number of bytes transfered so far, and total is the
            size of the file.
        max_connections:
            Maximum number of parallel connections to use when the file size
            exceeds 64MB.
            Set to 1 to download the file chunks sequentially.
            Set to 2 or more to download the file chunks in parallel. This uses
            more system resources but will download faster.
        max_retries:
            Number of times to retry download of file chunk if an error occurs.
        retry_wait:
            Sleep time in secs between retries.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('file_name', file_name)
        _validate_not_none('encoding', encoding)

        result = self.get_file_to_bytes(share_name,
                                        directory_name,
                                        file_name,
                                        progress_callback,
                                        max_connections,
                                        max_retries,
                                        retry_wait)

        return result.decode(encoding)

    def update_range(self, share_name, directory_name, file_name, data, 
                     byte_range, content_md5=None):
        '''
        Writes the bytes specified by the request body into the specified range.
         
        share_name:
            Name of existing share.
        directory_name:
            The path to the directory.
        file_name:
            Name of existing file.
        data:
            Content of the range.
        byte_range:
            Specifies the range of bytes to be written. Both the start and end 
            of the range must be specified. The range can be up to 4 MB in size.
            The byte range must be specified in the following format: 
            bytes=startByte-endByte (e.g. "bytes=0-1024"). 
        content_md5:
            An MD5 hash of the range content. This hash is used to
            verify the integrity of the range during transport. When this header
            is specified, the storage service compares the hash of the content
            that has arrived with the header value that was sent. If the two
            hashes do not match, the operation will fail with error code 400
            (Bad Request).
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('file_name', file_name)
        _validate_not_none('data', data)
        _validate_not_none('x_ms_range', byte_range)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = '/' + _str(share_name)
        if directory_name is not None:
            request.path += '/' + _str(directory_name)
        request.path += '/' + _str(file_name) + '?comp=range'
        request.headers = [
            ('x-ms-range', _str_or_none(byte_range)),
            ('Content-MD5', _str_or_none(content_md5)),
            ('x-ms-write', 'update'),
        ]
        request.body = _get_request_body_bytes_only('data', data)
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_file_header(
            request, self.authentication)
        self._perform_request(request)

    def clear_range(self, share_name, directory_name, file_name, byte_range):
        '''
        Clears the specified range and releases the space used in storage for 
        that range.
         
        share_name:
            Name of existing share.
        directory_name:
            The path to the directory.
        file_name:
            Name of existing file.
        byte_range:
            Specifies the range of bytes to be cleared. Both the start
            and end of the range must be specified. The range can be up to the 
            value of the file's full size. The byte range must be specified in 
            the following format: bytes=startByte-endByte (e.g. "bytes=0-1024"). 
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('file_name', file_name)
        _validate_not_none('x_ms_range', byte_range)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = '/' + _str(share_name)
        if directory_name is not None:
            request.path += '/' + _str(directory_name)
        request.path += '/' + _str(file_name) + '?comp=range'
        request.headers = [
            ('x-ms-range', _str_or_none(byte_range)),
            ('Content-Length', '0'),
            ('x-ms-write', 'clear'),
        ]
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_file_header(
            request, self.authentication)
        self._perform_request(request)

    def list_ranges(self, share_name, directory_name, file_name, byte_range=None):
        '''
        Retrieves the ranges for a file. If the x-ms-range header is specified 
        on a request, then the service uses the range specified by x-ms-range; 
        otherwise, the range specified by the Range header is used. If both are
        omitted, then all ranges for the file are returned.

        Some File service GET operations support the use of the standard HTTP 
        Range header. Many HTTP clients, including the .NET client library, 
        limit the size of the Range header to a 32-bit integer, and thus its 
        value is limited to a maximum of 4 GB. Since files can be larger than 
        4 GB in size, the File service accepts a custom range header x-ms-range 
        for any operation that takes an HTTP Range header. 

        share_name:
            Name of existing share.
        directory_name:
            The path to the directory.
        file_name:
            Name of existing file.
        byte_range:
            Specifies the range of bytes over which to list ranges, 
            inclusively. Must be in one of these formats:
                bytes=startByte
                bytes=startByte-endByte
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('file_name', file_name)
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = '/' + _str(share_name)
        if directory_name is not None:
            request.path += '/' + _str(directory_name)
        request.path += '/' + _str(file_name) + '?comp=rangelist'
        request.headers = [
            ('x-ms-range', _str_or_none(byte_range))
        ]
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_file_header(
            request, self.authentication)
        response = self._perform_request(request)

        return _ETreeXmlToObject.parse_simple_list(response, RangeList, Range, "file_ranges")
