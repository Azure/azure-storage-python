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
from .._error import (
    _dont_fail_not_exist,
    _dont_fail_on_exist,
    _validate_not_none,
    _validate_type_bytes,
    _ERROR_VALUE_NEGATIVE,
    _ERROR_STORAGE_MISSING_INFO,
    _ERROR_EMULATOR_DOES_NOT_SUPPORT_FILES,
)
from .._common_conversion import (
    _int_or_none,
    _str,
    _str_or_none,
)
from .._serialization import (
    _get_request_body,
    _get_request_body_bytes_only,
    _convert_signed_identifiers_to_xml,
    _convert_service_properties_to_xml,
)
from .._deserialization import (
    _convert_xml_to_service_properties,
    _convert_xml_to_signed_identifiers,
    _get_download_size,
    _parse_metadata,
    _parse_properties,
)
from ..models import Services
from .models import (
    File,
    FileProperties,
)
from .._http import HTTPRequest
from ._chunking import (
    _download_file_chunks,
    _upload_file_chunks,
)
from ..auth import (
    _StorageSharedKeyAuthentication,
    _StorageSASAuthentication,
)
from ..connection import _ServiceParameters
from ..constants import (
    SERVICE_HOST_BASE,
    DEFAULT_PROTOCOL,
    DEV_ACCOUNT_NAME,
)
from ._serialization import (
    _get_path,
    _validate_and_format_range_headers,
)
from ._deserialization import (
    _convert_xml_to_shares,
    _convert_xml_to_directories_and_files,
    _convert_xml_to_ranges,
    _convert_xml_to_share_stats,
    _parse_file,
    _parse_share,
    _parse_directory,
)
from ..sharedaccesssignature import (
    SharedAccessSignature,
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

    def __init__(self, account_name=None, account_key=None, sas_token=None, 
                 protocol=DEFAULT_PROTOCOL, endpoint_suffix=SERVICE_HOST_BASE, 
                 request_session=None, connection_string=None):
        '''
        :param str account_name:
            The storage account name. This is used to authenticate requests 
            signed with an account key and to construct the storage endpoint. It 
            is required unless a connection string is given.
        :param str account_key:
            The storage account key. This is used for shared key authentication. 
        :param str sas_token:
             A shared access signature token to use to authenticate requests 
             instead of the account key. If account key and sas token are both 
             specified, account key will be used to sign.
        :param str protocol:
            The protocol to use for requests. Defaults to https.
        :param str endpoint_suffix:
            The host base component of the url, minus the account name. Defaults 
            to Azure (core.windows.net). Override this to use the China cloud 
            (core.chinacloudapi.cn).
        :param requests.Session request_session:
            The session object to use for http requests.
        :param str connection_string:
            If specified, this will override all other parameters besides 
            request session. See
            http://azure.microsoft.com/en-us/documentation/articles/storage-configure-connection-string/
            for the connection string format.
        '''
        service_params = _ServiceParameters.get_service_parameters(
            'file',
            account_name=account_name, 
            account_key=account_key, 
            sas_token=sas_token, 
            protocol=protocol, 
            endpoint_suffix=endpoint_suffix,
            request_session=request_session,
            connection_string=connection_string)
            
        super(FileService, self).__init__(service_params)

        if self.account_name == DEV_ACCOUNT_NAME:
            raise ValueError(_ERROR_EMULATOR_DOES_NOT_SUPPORT_FILES)

        if self.account_key:
            self.authentication = _StorageSharedKeyAuthentication(
                self.account_name,
                self.account_key,
            )
        elif self.sas_token:
            self.authentication = _StorageSASAuthentication(self.sas_token)
        else:
            raise ValueError(_ERROR_STORAGE_MISSING_INFO)

    def make_file_url(self, share_name, directory_name, file_name, 
                      protocol=None, sas_token=None):
        '''
        Creates the url to access a file.

        share_name:
            Name of share.
        directory_name:
            The path to the directory.
        file_name:
            Name of file.
        protocol:
            Protocol to use: 'http' or 'https'. If not specified, uses the
            protocol specified when FileService was initialized.
        sas_token:
            Shared access signature token created with
            generate_shared_access_signature.
        '''

        if directory_name is None:
            url = '{}://{}/{}/{}'.format(
                protocol or self.protocol,
                self.primary_endpoint,
                share_name,
                file_name,
            )
        else:
            url = '{}://{}/{}/{}/{}'.format(
                protocol or self.protocol,
                self.primary_endpoint,
                share_name,
                directory_name,
                file_name,
            )

        if sas_token:
            url += '?' + sas_token

        return url

    def generate_account_shared_access_signature(self, resource_types, permission, 
                                        expiry, start=None, ip=None, protocol=None):
        '''
        Generates a shared access signature for the file service.
        Use the returned signature with the sas_token parameter of the FileService.

        :param ResourceTypes resource_types:
            Specifies the resource types that are accessible with the account SAS.
        :param AccountPermissions permission:
            The permissions associated with the shared access signature. The 
            user is restricted to operations allowed by the permissions. 
            Required unless an id is given referencing a stored access policy 
            which contains this field. This field must be omitted if it has been 
            specified in an associated stored access policy.
        :param expiry:
            The time at which the shared access signature becomes invalid. 
            Required unless an id is given referencing a stored access policy 
            which contains this field. This field must be omitted if it has 
            been specified in an associated stored access policy. Azure will always 
            convert values to UTC. If a date is passed in without timezone info, it 
            is assumed to be UTC.
        :type expiry: date or str
        :param start:
            The time at which the shared access signature becomes valid. If 
            omitted, start time for this call is assumed to be the time when the 
            storage service receives the request. Azure will always convert values 
            to UTC. If a date is passed in without timezone info, it is assumed to 
            be UTC.
        :type start: date or str
        :param str ip:
            Specifies an IP address or a range of IP addresses from which to accept requests.
            If the IP address from which the request originates does not match the IP address
            or address range specified on the SAS token, the request is not authenticated.
            For example, specifying sip=168.1.5.65 or sip=168.1.5.60-168.1.5.70 on the SAS
            restricts the request to those IP addresses.
        :param str protocol:
            Specifies the protocol permitted for a request made. Possible values are
            both HTTPS and HTTP (https,http) or HTTPS only (https). The default value
            is https,http. Note that HTTP only is not a permitted value.
        '''
        _validate_not_none('self.account_name', self.account_name)
        _validate_not_none('self.account_key', self.account_key)

        sas = SharedAccessSignature(self.account_name, self.account_key)
        return sas.generate_account(Services.FILE, resource_types, permission, 
                                    expiry, start=start, ip=ip, protocol=protocol)

    def generate_share_shared_access_signature(self, share_name, 
                                         permission=None, 
                                         expiry=None,
                                         start=None, 
                                         id=None,
                                         ip=None,
                                         protocol=None,
                                         cache_control=None,
                                         content_disposition=None,
                                         content_encoding=None,
                                         content_language=None,
                                         content_type=None):
        '''
        Generates a shared access signature for the share.
        Use the returned signature with the sas_token parameter of FileService.

        :param str share_name:
            Name of share.
        :param SharePermissions permission:
            The permissions associated with the shared access signature. The 
            user is restricted to operations allowed by the permissions.
            Permissions must be ordered read, create, write, delete, list.
            Required unless an id is given referencing a stored access policy 
            which contains this field. This field must be omitted if it has been 
            specified in an associated stored access policy.
        :param expiry:
            The time at which the shared access signature becomes invalid. 
            Required unless an id is given referencing a stored access policy 
            which contains this field. This field must be omitted if it has 
            been specified in an associated stored access policy. Azure will always 
            convert values to UTC. If a date is passed in without timezone info, it 
            is assumed to be UTC.
        :type expiry: date or str
        :param start:
            The time at which the shared access signature becomes valid. If 
            omitted, start time for this call is assumed to be the time when the 
            storage service receives the request. Azure will always convert values 
            to UTC. If a date is passed in without timezone info, it is assumed to 
            be UTC.
        :type start: date or str
        :param str id:
            A unique value up to 64 characters in length that correlates to a 
            stored access policy. To create a stored access policy, use 
            set_file_service_properties.
        :param str ip:
            Specifies an IP address or a range of IP addresses from which to accept requests.
            If the IP address from which the request originates does not match the IP address
            or address range specified on the SAS token, the request is not authenticated.
            For example, specifying sip=168.1.5.65 or sip=168.1.5.60-168.1.5.70 on the SAS
            restricts the request to those IP addresses.
        :param str protocol:
            Specifies the protocol permitted for a request made. Possible values are
            both HTTPS and HTTP (https,http) or HTTPS only (https). The default value
            is https,http. Note that HTTP only is not a permitted value.
        :param str cache_control:
            Response header value for Cache-Control when resource is accessed
            using this shared access signature.
        :param str content_disposition:
            Response header value for Content-Disposition when resource is accessed
            using this shared access signature.
        :param str content_encoding:
            Response header value for Content-Encoding when resource is accessed
            using this shared access signature.
        :param str content_language:
            Response header value for Content-Language when resource is accessed
            using this shared access signature.
        :param str content_type:
            Response header value for Content-Type when resource is accessed
            using this shared access signature.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('self.account_name', self.account_name)
        _validate_not_none('self.account_key', self.account_key)

        sas = SharedAccessSignature(self.account_name, self.account_key)
        return sas.generate_share(
            share_name,
            permission, 
            expiry,
            start=start, 
            id=id,
            ip=ip,
            protocol=protocol,
            cache_control=cache_control,
            content_disposition=content_disposition,
            content_encoding=content_encoding,
            content_language=content_language,
            content_type=content_type,
        )

    def generate_file_shared_access_signature(self, share_name, 
                                         directory_name=None, 
                                         file_name=None,
                                         permission=None, 
                                         expiry=None,
                                         start=None, 
                                         id=None,
                                         ip=None,
                                         protocol=None,
                                         cache_control=None,
                                         content_disposition=None,
                                         content_encoding=None,
                                         content_language=None,
                                         content_type=None):
        '''
        Generates a shared access signature for the file.
        Use the returned signature with the sas_token parameter of FileService.

        :param str share_name:
            Name of share.
        :param str directory_name:
            Name of directory. SAS tokens cannot be created for directories, so 
            this parameter should only be present if file_name is provided.
        :param str file_name:
            Name of file.
        :param FilePermissions permission:
            The permissions associated with the shared access signature. The 
            user is restricted to operations allowed by the permissions.
            Permissions must be ordered read, create, write, delete, list.
            Required unless an id is given referencing a stored access policy 
            which contains this field. This field must be omitted if it has been 
            specified in an associated stored access policy.
        :param expiry:
            The time at which the shared access signature becomes invalid. 
            Required unless an id is given referencing a stored access policy 
            which contains this field. This field must be omitted if it has 
            been specified in an associated stored access policy. Azure will always 
            convert values to UTC. If a date is passed in without timezone info, it 
            is assumed to be UTC.
        :type expiry: date or str
        :param start:
            The time at which the shared access signature becomes valid. If 
            omitted, start time for this call is assumed to be the time when the 
            storage service receives the request. Azure will always convert values 
            to UTC. If a date is passed in without timezone info, it is assumed to 
            be UTC.
        :type start: date or str
        :param str id:
            A unique value up to 64 characters in length that correlates to a 
            stored access policy. To create a stored access policy, use 
            set_file_service_properties.
        :param str ip:
            Specifies an IP address or a range of IP addresses from which to accept requests.
            If the IP address from which the request originates does not match the IP address
            or address range specified on the SAS token, the request is not authenticated.
            For example, specifying sip=168.1.5.65 or sip=168.1.5.60-168.1.5.70 on the SAS
            restricts the request to those IP addresses.
        :param str protocol:
            Specifies the protocol permitted for a request made. Possible values are
            both HTTPS and HTTP (https,http) or HTTPS only (https). The default value
            is https,http. Note that HTTP only is not a permitted value.
        :param str cache_control:
            Response header value for Cache-Control when resource is accessed
            using this shared access signature.
        :param str content_disposition:
            Response header value for Content-Disposition when resource is accessed
            using this shared access signature.
        :param str content_encoding:
            Response header value for Content-Encoding when resource is accessed
            using this shared access signature.
        :param str content_language:
            Response header value for Content-Language when resource is accessed
            using this shared access signature.
        :param str content_type:
            Response header value for Content-Type when resource is accessed
            using this shared access signature.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('file_name', file_name)
        _validate_not_none('self.account_name', self.account_name)
        _validate_not_none('self.account_key', self.account_key)

        sas = SharedAccessSignature(self.account_name, self.account_key)
        return sas.generate_file(
            share_name,
            directory_name,
            file_name,
            permission, 
            expiry,
            start=start, 
            id=id,
            ip=ip,
            protocol=protocol,
            cache_control=cache_control,
            content_disposition=content_disposition,
            content_encoding=content_encoding,
            content_language=content_language,
            content_type=content_type,
        )

    def set_file_service_properties(self, hour_metrics=None, minute_metrics=None, 
                                    cors=None, timeout=None):
        '''
        Sets the properties of a storage account's File service, including
        Azure Storage Analytics. If an element (ex HourMetrics) is left as None, the 
        existing settings on the service for that functionality are preserved.

        :param Metrics hour_metrics:
            The hour metrics settings provide a summary of request 
            statistics grouped by API in hourly aggregates for files.
        :param Metrics minute_metrics:
            The minute metrics settings provide request statistics 
            for each minute for files.
        :param cors:
            You can include up to five CorsRule elements in the 
            list. If an empty list is specified, all CORS rules will be deleted, 
            and CORS will be disabled for the service.
        :type cors: list of :class:`CorsRule`
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = _get_path()
        request.query = [
            ('restype', 'service'),
            ('comp', 'properties'),
            ('timeout', _int_or_none(timeout)),         
        ]
        request.body = _get_request_body(
            _convert_service_properties_to_xml(None, hour_metrics, minute_metrics, cors))

        self._perform_request(request)

    def get_file_service_properties(self, timeout=None):
        '''
        Gets the properties of a storage account's File service, including
        Azure Storage Analytics.

        timeout:
            Optional. The timeout parameter is expressed in seconds.
        '''
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = _get_path()
        request.query = [
            ('restype', 'service'),
            ('comp', 'properties'),
            ('timeout', _int_or_none(timeout)),         
        ]

        response = self._perform_request(request)
        return _convert_xml_to_service_properties(response.body)

    def list_shares(self, prefix=None, marker=None, max_results=None, 
                    include=None, timeout=None):
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
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = _get_path()
        request.query = [
            ('comp', 'list'),
            ('prefix', _str_or_none(prefix)),
            ('marker', _str_or_none(marker)),
            ('maxresults', _int_or_none(max_results)),
            ('include', _str_or_none(include)),
            ('timeout', _int_or_none(timeout)),
        ]

        response = self._perform_request(request)
        return _convert_xml_to_shares(response)

    def create_share(self, share_name, metadata=None, quota=None,
                     fail_on_exist=False, timeout=None):
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
        quota:
            Specifies the maximum size of the share, in gigabytes. Must be 
            greater than 0, and less than or equal to 5TB (5120).
        fail_on_exist:
            Specify whether to throw an exception when the share exists.
            False by default.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('share_name', share_name)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = _get_path(share_name)
        request.query = [
            ('restype', 'share'),
            ('timeout', _int_or_none(timeout)),
        ]
        request.headers = [
            ('x-ms-meta-name-values', metadata),
            ('x-ms-share-quota', _int_or_none(quota))]

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

    def get_share_properties(self, share_name, timeout=None):
        '''
        Returns all user-defined metadata and system properties for the
        specified share.

        share_name:
            Name of existing share.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('share_name', share_name)
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = _get_path(share_name)
        request.query = [
            ('restype', 'share'),
            ('timeout', _int_or_none(timeout)),
        ]

        response = self._perform_request(request)
        return _parse_share(share_name, response)

    def set_share_properties(self, share_name, quota, timeout=None):
        '''
        Sets service-defined properties for the specified share.

        share_name:
            Name of existing share.
        quota:
            Specifies the maximum size of the share, in gigabytes. Must be 
            greater than 0, and less than or equal to 5 TB (5120 GB).
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('quota', quota)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = _get_path(share_name)
        request.query = [
            ('restype', 'share'),
            ('comp', 'properties'),
            ('timeout', _int_or_none(timeout)),
        ]
        request.headers = [('x-ms-share-quota', _int_or_none(quota))]

        self._perform_request(request)

    def get_share_metadata(self, share_name, timeout=None):
        '''
        Returns all user-defined metadata for the specified share. The
        metadata will be in returned dictionary['x-ms-meta-(name)'].

        share_name:
            Name of existing share.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('share_name', share_name)
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = _get_path(share_name)
        request.query = [
            ('restype', 'share'),
            ('comp', 'metadata'),
            ('timeout', _int_or_none(timeout)),
        ]

        response = self._perform_request(request)
        return _parse_metadata(response)

    def set_share_metadata(self, share_name, metadata=None, timeout=None):
        '''
        Sets one or more user-defined name-value pairs for the specified
        share.

        share_name:
            Name of existing share.
        metadata:
            A dict containing name, value for metadata.
            Example: {'category':'test'}
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('share_name', share_name)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = _get_path(share_name)
        request.query = [
            ('restype', 'share'),
            ('comp', 'metadata'),
            ('timeout', _int_or_none(timeout)),
        ]
        request.headers = [('x-ms-meta-name-values', metadata)]

        self._perform_request(request)

    def get_share_acl(self, share_name, timeout=None):
        '''
        Gets the permissions for the specified share.

        :param str share_name:
            Name of existing share.
        :return: A dictionary of access policies associated with the share.
        :rtype: dict of str to :class:`.AccessPolicy`:
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('share_name', share_name)
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = _get_path(share_name)
        request.query = [
            ('restype', 'share'),
            ('comp', 'acl'),
            ('timeout', _int_or_none(timeout)),
        ]

        response = self._perform_request(request)
        return _convert_xml_to_signed_identifiers(response.body)

    def set_share_acl(self, share_name, signed_identifiers=None, timeout=None):
        '''
        Sets the permissions for the specified share or stored access 
        policies that may be used with Shared Access Signatures.

        :param str share_name:
            Name of existing share.
        :param signed_identifiers:
            A dictionary of access policies to associate with the share. The 
            dictionary may contain up to 5 elements. An empty dictionary 
            will clear the access policies set on the service. 
        :type signed_identifiers: dict of str to :class:`.AccessPolicy`:
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('share_name', share_name)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = _get_path(share_name)
        request.query = [
            ('restype', 'share'),
            ('comp', 'acl'),
            ('timeout', _int_or_none(timeout)),
        ]
        request.body = _get_request_body(
            _convert_signed_identifiers_to_xml(signed_identifiers))

        self._perform_request(request)

    def get_share_stats(self, share_name, timeout=None):
        '''
        Gets statistics related to the share.

        :param str share_name:
            Name of existing share.
        :return: Returns statistics related to the share.
        :rtype: ShareStats
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('share_name', share_name)
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = _get_path(share_name)
        request.query = [
            ('restype', 'share'),
            ('comp', 'stats'),
            ('timeout', _int_or_none(timeout)),
        ]

        response = self._perform_request(request)
        return _convert_xml_to_share_stats(response)

    def delete_share(self, share_name, fail_not_exist=False, timeout=None):
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
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('share_name', share_name)
        request = HTTPRequest()
        request.method = 'DELETE'
        request.host = self._get_host()
        request.path = _get_path(share_name)
        request.query = [
            ('restype', 'share'),
            ('timeout', _int_or_none(timeout)),
        ]

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
                         fail_on_exist=False, timeout=None):
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
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('directory_name', directory_name)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = _get_path(share_name, directory_name)
        request.query = [
            ('restype', 'directory'),
            ('timeout', _int_or_none(timeout)),
        ]

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
                         fail_not_exist=False, timeout=None):
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
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('directory_name', directory_name)
        request = HTTPRequest()
        request.method = 'DELETE'
        request.host = self._get_host()
        request.path = _get_path(share_name, directory_name)
        request.query = [
            ('restype', 'directory'),
            ('timeout', _int_or_none(timeout)),
        ]

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

    def get_directory_properties(self, share_name, directory_name, timeout=None):
        '''
        Returns all user-defined metadata and system properties for the
        specified directory.

        share_name:
            Name of existing share.
        directory_name:
           The path to an existing directory.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('directory_name', directory_name)
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = _get_path(share_name, directory_name)
        request.query = [
            ('restype', 'directory'),
            ('timeout', _int_or_none(timeout)),
        ]

        response = self._perform_request(request)
        return _parse_directory(directory_name, response)

    def get_directory_metadata(self, share_name, directory_name, timeout=None):
        '''
        Returns all user-defined metadata for the specified directory.

        share_name:
            Name of existing share.
        directory_name:
            The path to the directory.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('directory_name', directory_name)
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = _get_path(share_name, directory_name)
        request.query = [
            ('restype', 'directory'),
            ('comp', 'metadata'),
            ('timeout', _int_or_none(timeout)),
        ]

        response = self._perform_request(request)
        return _parse_metadata(response)

    def set_directory_metadata(self, share_name, directory_name, metadata=None, timeout=None):
        '''
        Sets user-defined metadata for the specified directory as one or more
        name-value pairs.

        share_name:
            Name of existing share.
        directory_name:
            The path to the directory.
        metadata:
            Dict containing name and value pairs.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('directory_name', directory_name)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = _get_path(share_name, directory_name)
        request.query = [
            ('restype', 'directory'),
            ('comp', 'metadata'),
            ('timeout', _int_or_none(timeout)),
        ]
        request.headers = [('x-ms-meta-name-values', metadata)]

        self._perform_request(request)

    def list_directories_and_files(self, share_name, directory_name=None, 
                                   marker=None, max_results=None, timeout=None):
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
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('share_name', share_name)
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = _get_path(share_name, directory_name)
        request.query = [
            ('restype', 'directory'),
            ('comp', 'list'),
            ('marker', _str_or_none(marker)),
            ('maxresults', _int_or_none(max_results)),
            ('timeout', _int_or_none(timeout)),
        ]

        response = self._perform_request(request)
        return _convert_xml_to_directories_and_files(response)

    def get_file_properties(self, share_name, directory_name, file_name, timeout=None):
        '''
        Returns all user-defined metadata, standard HTTP properties, and
        system properties for the file. Returns an instance of File with properties and metadata.

        share_name:
            Name of existing share.
        directory_name:
            The path to the directory.
        file_name:
            Name of existing file.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('file_name', file_name)
        request = HTTPRequest()
        request.method = 'HEAD'
        request.host = self._get_host()
        request.path = _get_path(share_name, directory_name, file_name)
        request.query = [('timeout', _int_or_none(timeout))]

        response = self._perform_request(request)
        return _parse_file(file_name, response)

    def exists(self, share_name, directory_name=None, file_name=None, timeout=None):
        '''
        Returns a boolean indicating whether the share exists, the directory 
        exists, or the file exists.

        share_name:
            Name of a share.
        directory_name:
            The path to a directory.
        file_name:
            Name of a file.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('share_name', share_name)
        try:
            if file_name is not None:
                self.get_file_properties(share_name, directory_name, file_name, timeout=timeout)
            elif directory_name is not None:
                self.get_directory_properties(share_name, directory_name, timeout=timeout)
            else:
                self.get_share_properties(share_name, timeout=timeout)
            return True
        except AzureHttpError as ex:
            _dont_fail_not_exist(ex)
            return False

    def resize_file(self, share_name, directory_name, 
                    file_name, content_length, timeout=None):
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
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('file_name', file_name)
        _validate_not_none('content_length', content_length)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = _get_path(share_name, directory_name, file_name)
        request.query = [
            ('comp', 'properties'),
            ('timeout', _int_or_none(timeout)),
        ]
        request.headers = [
            ('x-ms-content-length', _str_or_none(content_length))]

        self._perform_request(request)

    def set_file_properties(self, share_name, directory_name, 
                            file_name, content_settings=None, timeout=None):
        '''
        Sets system properties on the file.

        share_name:
            Name of existing share.
        directory_name:
            The path to the directory.
        file_name:
            Name of existing file.
        content_settings:
            ContentSettings object used to set the file properties.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('file_name', file_name)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = _get_path(share_name, directory_name, file_name)
        request.query = [
            ('comp', 'properties'),
            ('timeout', _int_or_none(timeout)),
        ]
        request.headers = None
        if content_settings is not None:
            request.headers = content_settings.to_headers()

        self._perform_request(request)

    def get_file_metadata(self, share_name, directory_name, file_name, timeout=None):
        '''
        Returns all user-defined metadata for the specified file.

        share_name:
            Name of existing share.
        directory_name:
            The path to the directory.
        file_name:
            Name of existing file.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('file_name', file_name)
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = _get_path(share_name, directory_name, file_name)
        request.query = [
            ('comp', 'metadata'),
            ('timeout', _int_or_none(timeout)),
        ]

        response = self._perform_request(request)
        return _parse_metadata(response)

    def set_file_metadata(self, share_name, directory_name, 
                          file_name, metadata=None, timeout=None):
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
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('file_name', file_name)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = _get_path(share_name, directory_name, file_name)
        request.query = [
            ('comp', 'metadata'),
            ('timeout', _int_or_none(timeout)),
        ]
        request.headers = [('x-ms-meta-name-values', metadata)]

        self._perform_request(request)

    def copy_file(self, share_name, directory_name, file_name, copy_source,
                  metadata=None, timeout=None):
        '''
        Copies a file to a destination within the storage account.

        share_name:
            Name of existing share.
        directory_name:
            The path to the directory.
        file_name:
            Name of existing file.
        copy_source:
            Specifies the URL of the source file or file, up to 2 KB in length. 
            A source file in the same account can be private, but a file in another account
            must be public or accept credentials included in this URL, such as
            a Shared Access Signature.
        metadata:
            Optional. Dict containing name and value pairs.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('file_name', file_name)
        _validate_not_none('copy_source', copy_source)

        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = _get_path(share_name, directory_name, file_name)
        request.query = [('timeout', _int_or_none(timeout))]
        request.headers = [
            ('x-ms-copy-source', _str_or_none(copy_source)),
            ('x-ms-meta-name-values', metadata),
        ]

        response = self._perform_request(request)
        props = _parse_properties(response, FileProperties)
        return props.copy

    def abort_copy_file(self, share_name, directory_name, file_name, copy_id, timeout=None):
        '''
         Aborts a pending copy_file operation, and leaves a destination file
         with zero length and full metadata.

         share_name:
             Name of destination share.
        directory_name:
            The path to the directory.
         file_name:
             Name of destination file.
         copy_id:
            Copy identifier provided in the copy_id of the original
            copy_file operation.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('file_name', file_name)
        _validate_not_none('copy_id', copy_id)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = _get_path(share_name, directory_name, file_name)
        request.query = [
            ('comp', 'copy'),
            ('copyid', _str(copy_id)),
            ('timeout', _int_or_none(timeout)),
        ]
        request.headers = [
            ('x-ms-copy-action', 'abort'),
        ]

        self._perform_request(request)

    def delete_file(self, share_name, directory_name, file_name, timeout=None):
        '''
        Marks the specified file for deletion. The file is later
        deleted during garbage collection.

        share_name:
            Name of existing share.
        directory_name:
            The path to the directory.
        file_name:
            Name of existing file.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('file_name', file_name)
        request = HTTPRequest()
        request.method = 'DELETE'
        request.host = self._get_host()
        request.path = _get_path(share_name, directory_name, file_name)
        request.query = [('timeout', _int_or_none(timeout))]

        self._perform_request(request)

    def create_file(self, share_name, directory_name, file_name,
                    content_length, content_settings=None, metadata=None, 
                    timeout=None):
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
        content_settings:
            ContentSettings object used to set file properties.
        metadata:
            A dict containing name, value for metadata.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('file_name', file_name)
        _validate_not_none('x_ms_content_length', content_length)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = _get_path(share_name, directory_name, file_name)
        request.query = [('timeout', _int_or_none(timeout))]
        request.headers = [
            ('x-ms-meta-name-values', metadata),
            ('x-ms-content-length', _str_or_none(content_length)),
            ('x-ms-type', 'file')
        ]
        if content_settings is not None:
            request.headers += content_settings.to_headers()

        self._perform_request(request)

    def create_file_from_path(self, share_name, directory_name, file_name, 
                           local_file_path, content_settings=None,
                           metadata=None, progress_callback=None,
                           max_connections=1, max_retries=5, retry_wait=1.0, timeout=None):
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
        content_settings:
            ContentSettings object used for setting file properties.
        metadata:
            A dict containing name, value for metadata.
        progress_callback:
            Callback for progress with signature function(current, total) where
            current is the number of bytes transfered so far and total is the
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
        :param int timeout:
            The timeout parameter is expressed in seconds. This method may make 
            multiple calls to the Azure service and the timeout will apply to 
            each call individually.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('file_name', file_name)
        _validate_not_none('local_file_path', local_file_path)

        count = path.getsize(local_file_path)
        with open(local_file_path, 'rb') as stream:
            self.create_file_from_stream(
                share_name, directory_name, file_name, stream,
                count, content_settings, metadata, progress_callback,
                max_connections, max_retries, retry_wait, timeout)

    def create_file_from_text(self, share_name, directory_name, file_name, 
                           text, encoding='utf-8', content_settings=None,
                           metadata=None, timeout=None):
        '''
        Creates a new file from str/unicode, or updates the content of an
        existing file.

        share_name:
            Name of existing share.
        directory_name:
            The path to the directory.
        file_name:
            Name of file to create or update.
        text:
            Text to upload to the file.
        encoding:
            Encoding to use to convert the text to bytes.
        content_settings:
            ContentSettings object used to set file properties.
        metadata:
            A dict containing name, value for metadata.
        :param int timeout:
            The timeout parameter is expressed in seconds. This method may make 
            multiple calls to the Azure service and the timeout will apply to 
            each call individually.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('file_name', file_name)
        _validate_not_none('text', text)

        if not isinstance(text, bytes):
            _validate_not_none('encoding', encoding)
            text = text.encode(encoding)

        self.create_file_from_bytes(
            share_name, directory_name, file_name, text, 0,
            len(text), content_settings, metadata, timeout)

    def create_file_from_bytes(
        self, share_name, directory_name, file_name, file,
        index=0, count=None, content_settings=None, metadata=None,
        progress_callback=None, max_connections=1, max_retries=5,
        retry_wait=1.0, timeout=None):
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
        content_settings:
            ContentSettings object used to set file properties.
        metadata:
            A dict containing name, value for metadata.
        progress_callback:
            Callback for progress with signature function(current, total) where
            current is the number of bytes transfered so far and total is the
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
        :param int timeout:
            The timeout parameter is expressed in seconds. This method may make 
            multiple calls to the Azure service and the timeout will apply to 
            each call individually.
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
            content_settings, metadata, progress_callback,
            max_connections, max_retries, retry_wait, timeout)

    def create_file_from_stream(
        self, share_name, directory_name, file_name, stream, count,
        content_settings=None, metadata=None, progress_callback=None,
        max_connections=1, max_retries=5, retry_wait=1.0, timeout=None):
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
        content_settings:
            ContentSettings object used to set file properties.
        metadata:
            A dict containing name, value for metadata.
        progress_callback:
            Callback for progress with signature function(current, total) where
            current is the number of bytes transfered so far and total is the
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
        :param int timeout:
            The timeout parameter is expressed in seconds. This method may make 
            multiple calls to the Azure service and the timeout will apply to 
            each call individually.
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
            content_settings,
            metadata,
            timeout
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
            timeout
        )

    def _get_file(self, share_name, directory_name, file_name,
                 start_range=None, end_range=None,
                 range_get_content_md5=None, timeout=None):
        '''
        Downloads a file's content, metadata, and properties. You can specify a
        range if you don't need to download the file in its entirety. If no range
        is specified, the full file will be downloaded.

        See get_file_to_* for high level functions that handle the download
        of large files with automatic chunking and progress notifications.

        share_name:
            Name of existing share.
        directory_name:
            The path to the directory.
        file_name:
            Name of existing file.
        start_range:
            Start of byte range to use for downloading a section of the file.
            If no end_range is given, all bytes after the start_range will be downloaded.
        end_range:
            End of byte range to use for downloading a section of the file.
            If end_range is given, start_range must be provided.
            This range will return bytes from the offset start up to offset end. 
        range_get_content_md5:
            When this header is set to True and specified together
            with the Range header, the service returns the MD5 hash for the
            range, as long as the range is less than or equal to 4 MB in size.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('file_name', file_name)
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = _get_path(share_name, directory_name, file_name)
        request.query = [('timeout', _int_or_none(timeout))]
        _validate_and_format_range_headers(
            request,
            start_range,
            end_range,
            start_range_required=False,
            end_range_required=False,
            check_content_md5=range_get_content_md5)

        response = self._perform_request(request, None)
        return _parse_file(file_name, response)

    def get_file_to_path(self, share_name, directory_name, file_name, file_path,
                         open_mode='wb', start_range=None, end_range=None,
                         range_get_content_md5=None, progress_callback=None,
                         max_connections=1, max_retries=5, retry_wait=1.0, timeout=None):
        '''
        Downloads a file to a file path, with automatic chunking and progress
        notifications. Returns an instance of File with properties and metadata.

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
        start_range:
            Start of byte range to use for downloading a section of the file.
            If no end_range is given, all bytes after the start_range will be downloaded.
        end_range:
            End of byte range to use for downloading a section of the file.
            If end_range is given, start_range must be provided.
            This range will return bytes from the offset start up to offset end. 
        range_get_content_md5:
            When this header is set to True and specified together
            with the Range header, the service returns the MD5 hash for the
            range, as long as the range is less than or equal to 4 MB in size.
        progress_callback:
            Callback for progress with signature function(current, total) 
            where current is the number of bytes transfered so far, and total is 
            the size of the file if known.
        max_connections:
            Set to 1 to download the file sequentially.
            Set to 2 or greater if you want to download a file larger than 64MB in chunks.
            If the file size does not exceed 64MB it will be downloaded in one chunk.
        max_retries:
            Number of times to retry download of file chunk if an error occurs.
        retry_wait:
            Sleep time in secs between retries.
        :param int timeout:
            The timeout parameter is expressed in seconds. This method may make 
            multiple calls to the Azure service and the timeout will apply to 
            each call individually.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('file_name', file_name)
        _validate_not_none('file_path', file_path)
        _validate_not_none('open_mode', open_mode)

        with open(file_path, open_mode) as stream:
            file = self.get_file_to_stream(
                share_name, directory_name, file_name, stream,
                start_range, end_range, range_get_content_md5,
                progress_callback, max_connections, max_retries,
                retry_wait, timeout)

        return file

    def get_file_to_stream(
        self, share_name, directory_name, file_name, stream,
        start_range=None, end_range=None, range_get_content_md5=None,
        progress_callback=None, max_connections=1, max_retries=5,
        retry_wait=1.0, timeout=None):
        '''
        Downloads a file to a stream, with automatic chunking and progress
        notifications. Returns an instance of File with properties and metadata.

        share_name:
            Name of existing share.
        directory_name:
            The path to the directory.
        file_name:
            Name of existing file.
        stream:
            Opened file/stream to write to.
        start_range:
            Start of byte range to use for downloading a section of the file.
            If no end_range is given, all bytes after the start_range will be downloaded.
        end_range:
            End of byte range to use for downloading a section of the file.
            If end_range is given, start_range must be provided.
            This range will return bytes from the offset start up to offset end. 
        range_get_content_md5:
            When this header is set to True and specified together
            with the Range header, the service returns the MD5 hash for the
            range, as long as the range is less than or equal to 4 MB in size.
        progress_callback:
            Callback for progress with signature function(current, total) 
            where current is the number of bytes transfered so far, and total is 
            the size of the file if known.
        max_connections:
            Set to 1 to download the file sequentially.
            Set to 2 or greater if you want to download a file larger than 64MB in chunks.
            If the file size does not exceed 64MB it will be downloaded in one chunk.
        max_retries:
            Number of times to retry download of file chunk if an error occurs.
        retry_wait:
            Sleep time in secs between retries.
        :param int timeout:
            The timeout parameter is expressed in seconds. This method may make 
            multiple calls to the Azure service and the timeout will apply to 
            each call individually.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('file_name', file_name)
        _validate_not_none('stream', stream)

        # Only get properties if parallelism will actually be used
        file_size = None
        if max_connections > 1 and range_get_content_md5 is None:
            file = self.get_file_properties(share_name, directory_name, 
                                            file_name, timeout=timeout)
            file_size = file.properties.content_length

            # If file size is large, use parallel download
            if file_size >= self._FILE_MAX_DATA_SIZE:
                _download_file_chunks(
                    self,
                    share_name,
                    directory_name,
                    file_name,
                    file_size,
                    self._FILE_MAX_CHUNK_DATA_SIZE,
                    start_range,
                    end_range,
                    stream,
                    max_connections,
                    max_retries,
                    retry_wait,
                    progress_callback, 
                    timeout
                )
                return file

        # If parallelism is off or the file is small, do a single download
        download_size = _get_download_size(start_range, end_range, file_size)
        if progress_callback:
            progress_callback(0, download_size)

        file = self._get_file(
            share_name,
            directory_name,
            file_name,
            start_range=start_range,
            end_range=end_range,
            range_get_content_md5=range_get_content_md5,
            timeout=timeout)

        if file.content is not None:
            stream.write(file.content)

        if progress_callback:
            download_size = len(file.content)
            progress_callback(download_size, download_size)

        file.content = None # Clear file content since output has been written to user stream
        return file

    def get_file_to_bytes(self, share_name, directory_name, file_name, 
                          start_range=None, end_range=None, range_get_content_md5=None,
                          progress_callback=None, max_connections=1, max_retries=5,
                          retry_wait=1.0, timeout=None):
        '''
        Downloads a file as an array of bytes, with automatic chunking and
        progress notifications. Returns an instance of File with properties, metadata, and content.

        share_name:
            Name of existing share.
        directory_name:
            The path to the directory.
        file_name:
            Name of existing file.
        start_range:
            Start of byte range to use for downloading a section of the file.
            If no end_range is given, all bytes after the start_range will be downloaded.
        end_range:
            End of byte range to use for downloading a section of the file.
            If end_range is given, start_range must be provided.
            This range will return bytes from the offset start up to offset end. 
        range_get_content_md5:
            When this header is set to True and specified together
            with the Range header, the service returns the MD5 hash for the
            range, as long as the range is less than or equal to 4 MB in size.
        progress_callback:
            Callback for progress with signature function(current, total) 
            where current is the number of bytes transfered so far, and total is 
            the size of the file if known.
        max_connections:
            Set to 1 to download the file sequentially.
            Set to 2 or greater if you want to download a file larger than 64MB in chunks.
            If the file size does not exceed 64MB it will be downloaded in one chunk.
        max_retries:
            Number of times to retry download of file chunk if an error occurs.
        retry_wait:
            Sleep time in secs between retries.
        :param int timeout:
            The timeout parameter is expressed in seconds. This method may make 
            multiple calls to the Azure service and the timeout will apply to 
            each call individually.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('file_name', file_name)

        stream = BytesIO()
        file = self.get_file_to_stream(
            share_name,
            directory_name,
            file_name,
            stream,
            start_range,
            end_range,
            range_get_content_md5,
            progress_callback,
            max_connections,
            max_retries,
            retry_wait,
            timeout)

        file.content = stream.getvalue()
        return file

    def get_file_to_text(
        self, share_name, directory_name, file_name, encoding='utf-8',
        start_range=None, end_range=None, range_get_content_md5=None,
        progress_callback=None, max_connections=1, max_retries=5,
        retry_wait=1.0, timeout=None):
        '''
        Downloads a file as unicode text, with automatic chunking and progress
        notifications. Returns an instance of File with properties, metadata, and content.

        share_name:
            Name of existing share.
        directory_name:
            The path to the directory.
        file_name:
            Name of existing file.
        encoding:
            Python encoding to use when decoding the file data.
        start_range:
            Start of byte range to use for downloading a section of the file.
            If no end_range is given, all bytes after the start_range will be downloaded.
        end_range:
            End of byte range to use for downloading a section of the file.
            If end_range is given, start_range must be provided.
            This range will return bytes from the offset start up to offset end.
        range_get_content_md5:
            When this header is set to True and specified together
            with the Range header, the service returns the MD5 hash for the
            range, as long as the range is less than or equal to 4 MB in size.
        progress_callback:
            Callback for progress with signature function(current, total) 
            where current is the number of bytes transfered so far, and total is 
            the size of the file if known.
        max_connections:
            Set to 1 to download the file sequentially.
            Set to 2 or greater if you want to download a file larger than 64MB in chunks.
            If the file size does not exceed 64MB it will be downloaded in one chunk.
        max_retries:
            Number of times to retry download of file chunk if an error occurs.
        retry_wait:
            Sleep time in secs between retries.
        :param int timeout:
            The timeout parameter is expressed in seconds. This method may make 
            multiple calls to the Azure service and the timeout will apply to 
            each call individually.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('file_name', file_name)
        _validate_not_none('encoding', encoding)

        file = self.get_file_to_bytes(
            share_name,
            directory_name,
            file_name,
            start_range,
            end_range,
            range_get_content_md5,
            progress_callback,
            max_connections,
            max_retries,
            retry_wait,
            timeout)

        file.content = file.content.decode(encoding)
        return file

    def update_range(self, share_name, directory_name, file_name, data, 
                     start_range, end_range, content_md5=None, timeout=None):
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
        start_range:
            Start of byte range to use for updating a section of the file.
            The range can be up to 4 MB in size.
        end_range:
            End of byte range to use for updating a section of the file.
            The range can be up to 4 MB in size.
        content_md5:
            An MD5 hash of the range content. This hash is used to
            verify the integrity of the range during transport. When this header
            is specified, the storage service compares the hash of the content
            that has arrived with the header value that was sent. If the two
            hashes do not match, the operation will fail with error code 400
            (Bad Request).
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('file_name', file_name)
        _validate_not_none('data', data)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = _get_path(share_name, directory_name, file_name)
        request.query = [
            ('comp', 'range'),
            ('timeout', _int_or_none(timeout)),
        ]
        request.headers = [
            ('Content-MD5', _str_or_none(content_md5)),
            ('x-ms-write', 'update'),
        ]
        _validate_and_format_range_headers(
            request, start_range, end_range)
        request.body = _get_request_body_bytes_only('data', data)

        self._perform_request(request)

    def clear_range(self, share_name, directory_name, file_name, start_range, end_range, timeout=None):
        '''
        Clears the specified range and releases the space used in storage for 
        that range.
         
        share_name:
            Name of existing share.
        directory_name:
            The path to the directory.
        file_name:
            Name of existing file.
        start_range:
            Start of byte range to use for clearing a section of the file.
            The range can be up to 4 MB in size.
        end_range:
            End of byte range to use for clearing a section of the file.
            The range can be up to 4 MB in size.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('file_name', file_name)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = _get_path(share_name, directory_name, file_name)
        request.query = [
            ('comp', 'range'),
            ('timeout', _int_or_none(timeout)),
        ]
        request.headers = [
            ('Content-Length', '0'),
            ('x-ms-write', 'clear'),
        ]
        _validate_and_format_range_headers(
            request, start_range, end_range)

        self._perform_request(request)

    def list_ranges(self, share_name, directory_name, file_name,
                    start_range=None, end_range=None, timeout=None):
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
        start_range:
            Specifies the start offset of bytes over which to list ranges.
        end_range:
            Specifies the end offset of bytes over which to list ranges.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('share_name', share_name)
        _validate_not_none('file_name', file_name)
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = _get_path(share_name, directory_name, file_name)
        request.query = [
            ('comp', 'rangelist'),
            ('timeout', _int_or_none(timeout)),
        ]
        if start_range is not None:
            _validate_and_format_range_headers(
                request,
                start_range,
                end_range,
                start_range_required=False,
                end_range_required=False)

        response = self._perform_request(request)
        return _convert_xml_to_ranges(response)
