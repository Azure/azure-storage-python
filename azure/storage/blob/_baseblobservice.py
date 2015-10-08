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
from azure.common import (
    AzureHttpError,
)
from .._common_error import (
    _dont_fail_not_exist,
    _dont_fail_on_exist,
    _validate_not_none,
)
from .._common_conversion import (
    _int_or_none,
    _str,
    _str_or_none,
)
from abc import ABCMeta
from .._common_serialization import (
    _get_request_body,
    _parse_response_for_dict,
    _parse_response_for_dict_filter,
    _parse_response_for_dict_prefix,
    _update_request_uri_query_local_storage,
    _ETreeXmlToObject,
)
from .._http import HTTPRequest
from ._chunking import _download_blob_chunks
from ..models import (
    Logging,
    Metrics,
    CorsRule,
    AccessPolicy,
)
from .models import (
    Container,
    ContainerEnumResults,
    LeaseActions,
)
from ..auth import (
    StorageSASAuthentication,
    StorageSharedKeyAuthentication,
    StorageNoAuthentication,
)
from ..connection import StorageConnectionParameters
from ..constants import (
    BLOB_SERVICE_HOST_BASE,
    DEFAULT_HTTP_TIMEOUT,
    DEV_BLOB_HOST,
    X_MS_VERSION,
)
from .._serialization import (
    _convert_signed_identifiers_to_xml,
    _convert_service_properties_to_xml,
)
from .._deserialization import (
    _convert_xml_to_service_properties,
    _convert_xml_to_signed_identifiers,
)
from ._serialization import (
    _create_blob_result,
    _parse_blob_enum_results_list,
    _update_storage_blob_header,
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

class _BaseBlobService(_StorageClient):

    '''
    This is the main class managing Blob resources.
    '''

    __metaclass__ = ABCMeta
    _BLOB_MAX_DATA_SIZE = 64 * 1024 * 1024
    _BLOB_MAX_CHUNK_DATA_SIZE = 4 * 1024 * 1024

    def __init__(self, account_name=None, account_key=None, protocol='https',
                 host_base=BLOB_SERVICE_HOST_BASE, dev_host=DEV_BLOB_HOST,
                 timeout=DEFAULT_HTTP_TIMEOUT, sas_token=None, connection_string=None,
                 request_session=None):
        '''
        account_name:
            your storage account name, required for all operations.
        account_key:
            your storage account key, required for all operations.
        protocol:
            Optional. Protocol. Defaults to https.
        host_base:
            Optional. Live host base url. Defaults to Azure url. Override this
            for on-premise.
        dev_host:
            Optional. Dev host url. Defaults to localhost.
        timeout:
            Optional. Timeout for the http request, in seconds.
        sas_token:
            Optional. Token to use to authenticate with shared access signature.
        connection_string:
            Optional. If specified, the first four parameters (account_name,
            account_key, protocol, host_base) may be overridden
            by values specified in the connection_string. The next three parameters
            (dev_host, timeout, sas_token) cannot be specified with a
            connection_string. See 
            http://azure.microsoft.com/en-us/documentation/articles/storage-configure-connection-string/
            for the connection string format.
        request_session:
            Optional. Session object to use for http requests. If this is
            specified, it replaces the default use of httplib.
        '''
        if connection_string is not None:
            connection_params = StorageConnectionParameters(connection_string)
            account_name = connection_params.account_name
            account_key = connection_params.account_key
            protocol = connection_params.protocol.lower()
            host_base = connection_params.host_base_blob
            
        super(_BaseBlobService, self).__init__(
            account_name, account_key, protocol, host_base, dev_host, timeout, sas_token, request_session)

        if self.account_key:
            self.authentication = StorageSharedKeyAuthentication(
                self.account_name,
                self.account_key,
            )
        elif self.sas_token:
            self.authentication = StorageSASAuthentication(self.sas_token)
        else:
            self.authentication = StorageNoAuthentication()

    def make_blob_url(self, container_name, blob_name, account_name=None,
                      protocol=None, host_base=None, sas_token=None):
        '''
        Creates the url to access a blob.

        container_name:
            Name of container.
        blob_name:
            Name of blob.
        account_name:
            Name of the storage account. If not specified, uses the account
            specified when _BaseBlobService was initialized.
        protocol:
            Protocol to use: 'http' or 'https'. If not specified, uses the
            protocol specified when _BaseBlobService was initialized.
        host_base:
            Live host base url.  If not specified, uses the host base specified
            when _BaseBlobService was initialized.
        sas_token:
            Shared access signature token created with
            generate_shared_access_signature.
        '''

        url = '{0}://{1}{2}/{3}/{4}'.format(
            protocol or self.protocol,
            account_name or self.account_name,
            host_base or self.host_base,
            container_name,
            blob_name,
        )

        if sas_token:
            url += '?' + sas_token

        return url

    def generate_shared_access_signature(self,
                                         container_name,
                                         blob_name=None,
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
        Generates a shared access signature for the container or blob.
        Use the returned signature with the sas_token parameter of _BaseBlobService.

        :param str container_name:
            Name of container.
        :param str blob_name:
            Name of blob.
        :param str permission:
            The permissions associated with the shared access signature. The 
            user is restricted to operations allowed by the permissions.
            Permissions must be ordered read, write, delete, list.
            Required unless an id is given referencing a stored access policy 
            which contains this field. This field must be omitted if it has been 
            specified in an associated stored access policy.
            See :class:`.ContainerSharedAccessPermissions` and 
            :class:`.BlobSharedAccessPermissions`
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
            set_blob_service_properties.
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
        _validate_not_none('container_name', container_name)
        _validate_not_none('self.account_name', self.account_name)
        _validate_not_none('self.account_key', self.account_key)

        if blob_name:
            resource_type = ResourceType.RESOURCE_BLOB
            resource_path = container_name + '/' + blob_name
        else:
            resource_type = ResourceType.RESOURCE_CONTAINER
            resource_path = container_name

        sas = SharedAccessSignature(self.account_name, self.account_key)
        return sas.generate_signed_query_string(
            'blob',
            resource_path,
            resource_type,
            permission, 
            expiry,
            start, 
            id,
            ip,
            protocol,
            cache_control,
            content_disposition,
            content_encoding,
            content_language,
            content_type,
        )

    def list_containers(self, prefix=None, marker=None, maxresults=None,
                        include=None):
        '''
        The List Containers operation returns a list of the containers under
        the specified account.

        prefix:
            Optional. Filters the results to return only containers whose names
            begin with the specified prefix.
        marker:
            Optional. A string value that identifies the portion of the list to
            be returned with the next list operation.
        maxresults:
            Optional. Specifies the maximum number of containers to return.
        include:
            Optional. Include this parameter to specify that the container's
            metadata be returned as part of the response body. set this
            parameter to string 'metadata' to get container's metadata.
        '''
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = '/?comp=list'
        request.query = [
            ('prefix', _str_or_none(prefix)),
            ('marker', _str_or_none(marker)),
            ('maxresults', _int_or_none(maxresults)),
            ('include', _str_or_none(include))
        ]
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_blob_header(
            request, self.authentication)
        response = self._perform_request(request)

        return _ETreeXmlToObject.parse_enum_results_list(
            response, ContainerEnumResults, "Containers", Container)

    def create_container(self, container_name, x_ms_meta_name_values=None,
                         x_ms_blob_public_access=None, fail_on_exist=False):
        '''
        Creates a new container under the specified account. If the container
        with the same name already exists, the operation fails.

        container_name:
            Name of container to create.
        x_ms_meta_name_values:
            Optional. A dict with name_value pairs to associate with the
            container as metadata. Example:{'Category':'test'}
        x_ms_blob_public_access:
            Optional. Possible values include: container, blob
        fail_on_exist:
            specify whether to throw an exception when the container exists.
        '''
        _validate_not_none('container_name', container_name)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = '/' + _str(container_name) + '?restype=container'
        request.headers = [
            ('x-ms-meta-name-values', x_ms_meta_name_values),
            ('x-ms-blob-public-access', _str_or_none(x_ms_blob_public_access))
        ]
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_blob_header(
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

    def get_container_properties(self, container_name, x_ms_lease_id=None):
        '''
        Returns all user-defined metadata and system properties for the
        specified container.

        container_name:
            Name of existing container.
        x_ms_lease_id:
            If specified, get_container_properties only succeeds if the
            container's lease is active and matches this ID.
        '''
        _validate_not_none('container_name', container_name)
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = '/' + _str(container_name) + '?restype=container'
        request.headers = [('x-ms-lease-id', _str_or_none(x_ms_lease_id))]
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_blob_header(
            request, self.authentication)
        response = self._perform_request(request)

        return _parse_response_for_dict(response)

    def get_container_metadata(self, container_name, x_ms_lease_id=None):
        '''
        Returns all user-defined metadata for the specified container. The
        metadata will be in returned dictionary['x-ms-meta-(name)'].

        container_name:
            Name of existing container.
        x_ms_lease_id:
            If specified, get_container_metadata only succeeds if the
            container's lease is active and matches this ID.
        '''
        _validate_not_none('container_name', container_name)
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = '/' + \
            _str(container_name) + '?restype=container&comp=metadata'
        request.headers = [('x-ms-lease-id', _str_or_none(x_ms_lease_id))]
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_blob_header(
            request, self.authentication)
        response = self._perform_request(request)

        return _parse_response_for_dict_prefix(response, prefixes=['x-ms-meta'])

    def set_container_metadata(self, container_name, x_ms_meta_name_values=None,
                               x_ms_lease_id=None, if_modified_since=None):
        '''
        Sets one or more user-defined name-value pairs for the specified
        container.

        container_name:
            Name of existing container.
        x_ms_meta_name_values:
            A dict containing name, value for metadata.
            Example: {'category':'test'}
        x_ms_lease_id:
            If specified, set_container_metadata only succeeds if the
            container's lease is active and matches this ID.
        if_modified_since:
            Optional. Datetime string.
        '''
        _validate_not_none('container_name', container_name)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = '/' + \
            _str(container_name) + '?restype=container&comp=metadata'
        request.headers = [
            ('x-ms-meta-name-values', x_ms_meta_name_values),
            ('If-Modified-Since', _str_or_none(if_modified_since)),
            ('x-ms-lease-id', _str_or_none(x_ms_lease_id)),
        ]
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_blob_header(
            request, self.authentication)
        self._perform_request(request)

    def get_container_acl(self, container_name, x_ms_lease_id=None):
        '''
        Gets the permissions for the specified container.

        :param str container_name:
            Name of existing container.
        :param str x_ms_lease_id:
            If specified, get_container_acl only succeeds if the
            container's lease is active and matches this ID.
        :return: A dictionary of access policies associated with the container.
        :rtype: dict of str to :class:`.AccessPolicy`:
        '''
        _validate_not_none('container_name', container_name)
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = '/' + \
            _str(container_name) + '?restype=container&comp=acl'
        request.headers = [('x-ms-lease-id', _str_or_none(x_ms_lease_id))]
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_blob_header(
            request, self.authentication)
        response = self._perform_request(request)

        return _convert_xml_to_signed_identifiers(response.body)

    def set_container_acl(self, container_name, signed_identifiers=None,
                          x_ms_blob_public_access=None, x_ms_lease_id=None,
                          if_modified_since=None, if_unmodified_since=None):
        '''
        Sets the permissions for the specified container or stored access 
        policies that may be used with Shared Access Signatures.

        :param str container_name:
            Name of existing container.
        :param signed_identifiers:
            A dictionary of access policies to associate with the container. The 
            dictionary may contain up to 5 elements. An empty dictionary 
            will clear the access policies set on the service. 
        :type signed_identifiers: dict of str to :class:`.AccessPolicy`:
        :param str x_ms_blob_public_access:
            Possible values include: container, blob
        :param str x_ms_lease_id:
            If specified, set_container_acl only succeeds if the
            container's lease is active and matches this ID.
        :param str if_modified_since:
            Datetime string.
        :param str if_unmodified_since:
            DateTime string.
        '''
        _validate_not_none('container_name', container_name)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = '/' + \
            _str(container_name) + '?restype=container&comp=acl'
        request.headers = [
            ('x-ms-blob-public-access', _str_or_none(x_ms_blob_public_access)),
            ('If-Modified-Since', _str_or_none(if_modified_since)),
            ('If-Unmodified-Since', _str_or_none(if_unmodified_since)),
            ('x-ms-lease-id', _str_or_none(x_ms_lease_id)),
        ]
        request.body = _get_request_body(
            _convert_signed_identifiers_to_xml(signed_identifiers))
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_blob_header(
            request, self.authentication)
        self._perform_request(request)

    def delete_container(self, container_name, fail_not_exist=False,
                         x_ms_lease_id=None, if_modified_since=None,
                         if_unmodified_since=None):
        '''
        Marks the specified container for deletion.

        container_name:
            Name of container to delete.
        fail_not_exist:
            Specify whether to throw an exception when the container doesn't
            exist.
        x_ms_lease_id:
            Required if the container has an active lease.
        if_modified_since:
            Optional. Datetime string.
        if_unmodified_since:
            Optional. DateTime string.
        '''
        _validate_not_none('container_name', container_name)
        request = HTTPRequest()
        request.method = 'DELETE'
        request.host = self._get_host()
        request.path = '/' + _str(container_name) + '?restype=container'
        request.headers = [
            ('x-ms-lease-id', _str_or_none(x_ms_lease_id)),
            ('If-Modified-Since', _str_or_none(if_modified_since)),
            ('If-Unmodified-Since', _str_or_none(if_unmodified_since)),          
        ]
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_blob_header(
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

    def _lease_container_impl(self, container_name, x_ms_lease_action,
                              x_ms_lease_id, x_ms_lease_duration,
                              x_ms_lease_break_period,
                              x_ms_proposed_lease_id,
                              if_modified_since,
                              if_unmodified_since):
        '''
        Establishes and manages a lock on a container for delete operations.
        The lock duration can be 15 to 60 seconds, or can be infinite.

        container_name:
            Name of existing container.
        x_ms_lease_action:
            Possible LeaseActions values: acquire|renew|release|break|change
        x_ms_lease_id:
            Required if the container has an active lease.
        x_ms_lease_duration:
            Specifies the duration of the lease, in seconds, or negative one
            (-1) for a lease that never expires. A non-infinite lease can be
            between 15 and 60 seconds. A lease duration cannot be changed
            using renew or change. For backwards compatibility, the default is
            60, and the value is only used on an acquire operation.
        x_ms_lease_break_period:
            Optional. For a break operation, this is the proposed duration of
            seconds that the lease should continue before it is broken, between
            0 and 60 seconds. This break period is only used if it is shorter
            than the time remaining on the lease. If longer, the time remaining
            on the lease is used. A new lease will not be available before the
            break period has expired, but the lease may be held for longer than
            the break period. If this header does not appear with a break
            operation, a fixed-duration lease breaks after the remaining lease
            period elapses, and an infinite lease breaks immediately.
        x_ms_proposed_lease_id:
            Optional for acquire, required for change. Proposed lease ID, in a
            GUID string format.
        if_modified_since:
            Datetime string.
        if_unmodified_since:
            DateTime string.
        '''
        _validate_not_none('container_name', container_name)
        _validate_not_none('x_ms_lease_action', x_ms_lease_action)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = '/' + \
            _str(container_name) + '?restype=container&comp=lease'
        request.headers = [
            ('x-ms-lease-id', _str_or_none(x_ms_lease_id)),
            ('x-ms-lease-action', _str_or_none(x_ms_lease_action)),
            ('x-ms-lease-duration', _str_or_none(x_ms_lease_duration)),
            ('x-ms-lease-break-period', _str_or_none(x_ms_lease_break_period)),
            ('x-ms-proposed-lease-id', _str_or_none(x_ms_proposed_lease_id)),
            ('If-Modified-Since', _str_or_none(if_modified_since)),
            ('If-Unmodified-Since', _str_or_none(if_unmodified_since)),
        ]
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_blob_header(
            request, self.authentication)
        response = self._perform_request(request)

        return _parse_response_for_dict_filter(
            response,
            filter=['x-ms-lease-id', 'x-ms-lease-time'])

    def acquire_container_lease(self, container_name,
                                x_ms_lease_duration=-1,
                                x_ms_proposed_lease_id=None,
                                if_modified_since=None,
                                if_unmodified_since=None):
        '''
        Acquires a lock on a container for delete operations.
        The lock duration can be 15 to 60 seconds or infinite.

        container_name:
            Name of existing container.
        x_ms_lease_duration:
            Optional. Specifies the duration of the lease, in seconds, or negative one
            (-1) for a lease that never expires. A non-infinite lease can be
            between 15 and 60 seconds. A lease duration cannot be changed
            using renew or change. Default is -1 (infinite lease).
        x_ms_proposed_lease_id:
            Optional. Proposed lease ID, in a GUID string format.
        if_modified_since:
            Optional. Datetime string.
        if_unmodified_since:
            Optional. DateTime string.
        '''
        _validate_not_none('x_ms_lease_duration', x_ms_lease_duration)
        if x_ms_lease_duration is not -1 and\
           (x_ms_lease_duration < 15 or x_ms_lease_duration > 60):
            raise ValueError("x_ms_lease_duration param needs to be between 15 and 60 or -1.")

        return self._lease_container_impl(container_name, 
                                          LeaseActions.Acquire,
                                          None, # x_ms_lease_id
                                          x_ms_lease_duration,
                                          None, # x_ms_lease_break_period
                                          x_ms_proposed_lease_id,
                                          if_modified_since,
                                          if_unmodified_since)

    def renew_container_lease(self, container_name, x_ms_lease_id,
                              if_modified_since=None,
                              if_unmodified_since=None):
        '''
        Renews a lock on a container for delete operations.

        container_name:
            Name of existing container.
        x_ms_lease_id:
            Lease ID for active lease.
        if_modified_since:
            Optional. Datetime string.
        if_unmodified_since:
            Optional. DateTime string.
        '''
        _validate_not_none('x_ms_lease_id', x_ms_lease_id)

        return self._lease_container_impl(container_name, 
                                          LeaseActions.Renew,
                                          x_ms_lease_id,
                                          None, # x_ms_lease_duration
                                          None, # x_ms_lease_break_period
                                          None, # x_ms_proposed_lease_id
                                          if_modified_since,
                                          if_unmodified_since)

    def release_container_lease(self, container_name, x_ms_lease_id,
                                if_modified_since=None,
                                if_unmodified_since=None):
        '''
        Releases a lock on a container for delete operations.
        The lock duration can be 15 to 60 seconds, or can be infinite.

        container_name:
            Name of existing container.
        x_ms_lease_id:
            Lease ID for active lease.
        if_modified_since:
            Optional. Datetime string.
        if_unmodified_since:
            Optional. DateTime string.
        if_match:
            Optional. An ETag value.
        if_none_match:
            Optional. An ETag value.
        '''
        _validate_not_none('x_ms_lease_id', x_ms_lease_id)

        return self._lease_container_impl(container_name, 
                                          LeaseActions.Release,
                                          x_ms_lease_id,
                                          None, # x_ms_lease_duration
                                          None, # x_ms_lease_break_period
                                          None, # x_ms_proposed_lease_id
                                          if_modified_since,
                                          if_unmodified_since)

    def break_container_lease(self, container_name, x_ms_lease_id,
                              x_ms_lease_break_period=None,
                              if_modified_since=None,
                              if_unmodified_since=None):
        '''
        Breaks a lock on a container for delete operations.
        The lock duration can be 15 to 60 seconds, or can be infinite.

        container_name:
            Name of existing container.
        x_ms_lease_id:
            Lease ID for active lease.
        x_ms_lease_break_period:
            Optional. This is the proposed duration of seconds that the lease
            should continue before it is broken, between 0 and 60 seconds. This
            break period is only used if it is shorter than the time remaining
            on the lease. If longer, the time remaining on the lease is used.
            A new lease will not be available before the break period has
            expired, but the lease may be held for longer than the break
            period. If this header does not appear with a break
            operation, a fixed-duration lease breaks after the remaining lease
            period elapses, and an infinite lease breaks immediately.
        if_modified_since:
            Optional. Datetime string.
        if_unmodified_since:
            Optional. DateTime string.
        '''
        _validate_not_none('x_ms_lease_id', x_ms_lease_id)
        if (x_ms_lease_break_period is not None) and (x_ms_lease_break_period < 0 or x_ms_lease_break_period > 60):
            raise ValueError("x_ms_lease_break_period param needs to be between 0 and 60.")
        
        return self._lease_container_impl(container_name, 
                                          LeaseActions.Break,
                                          x_ms_lease_id,
                                          None, # x_ms_lease_duration
                                          x_ms_lease_break_period,
                                          None, # x_ms_proposed_lease_id
                                          if_modified_since,
                                          if_unmodified_since)

    def change_container_lease(self, container_name, x_ms_lease_id,
                               x_ms_proposed_lease_id,
                               if_modified_since=None,
                               if_unmodified_since=None):
        '''
        Changes a lock on a container for delete operations.
        The lock duration can be 15 to 60 seconds, or can be infinite.

        container_name:
            Name of existing container.
        x_ms_lease_id:
            Lease ID for active lease.
        x_ms_proposed_lease_id:
            Proposed lease ID, in a GUID string format.
            period elapses, and an infinite lease breaks immediately.
        if_modified_since:
            Optional. Datetime string.
        if_unmodified_since:
            Optional. DateTime string.
        '''
        _validate_not_none('x_ms_lease_id', x_ms_lease_id)

        return self._lease_container_impl(container_name, 
                                          LeaseActions.Change,
                                          x_ms_lease_id,
                                          None, # x_ms_lease_duration
                                          None, # x_ms_lease_break_period
                                          x_ms_proposed_lease_id,
                                          if_modified_since,
                                          if_unmodified_since)

    def list_blobs(self, container_name, prefix=None, marker=None,
                   maxresults=None, include=None, delimiter=None):
        '''
        Returns the list of blobs under the specified container.

        container_name:
            Name of existing container.
        prefix:
            Optional. Filters the results to return only blobs whose names
            begin with the specified prefix.
        marker:
            Optional. A string value that identifies the portion of the list
            to be returned with the next list operation. The operation returns
            a marker value within the response body if the list returned was
            not complete. The marker value may then be used in a subsequent
            call to request the next set of list items. The marker value is
            opaque to the client.
        maxresults:
            Optional. Specifies the maximum number of blobs to return,
            including all BlobPrefix elements. If the request does not specify
            maxresults or specifies a value greater than 5,000, the server will
            return up to 5,000 items. Setting maxresults to a value less than
            or equal to zero results in error response code 400 (Bad Request).
        include:
            Optional. Specifies one or more datasets to include in the
            response. To specify more than one of these options on the URI,
            you must separate each option with a comma. Valid values are:
                snapshots:
                    Specifies that snapshots should be included in the
                    enumeration. Snapshots are listed from oldest to newest in
                    the response.
                metadata:
                    Specifies that blob metadata be returned in the response.
                uncommittedblobs:
                    Specifies that blobs for which blocks have been uploaded,
                    but which have not been committed using Put Block List
                    (REST API), be included in the response.
                copy:
                    Version 2012-02-12 and newer. Specifies that metadata
                    related to any current or previous Copy Blob operation
                    should be included in the response.
        delimiter:
            Optional. When the request includes this parameter, the operation
            returns a BlobPrefix element in the response body that acts as a
            placeholder for all blobs whose names begin with the same
            substring up to the appearance of the delimiter character. The
            delimiter may be a single character or a string.
        '''
        _validate_not_none('container_name', container_name)
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = '/' + \
            _str(container_name) + '?restype=container&comp=list'
        request.query = [
            ('prefix', _str_or_none(prefix)),
            ('delimiter', _str_or_none(delimiter)),
            ('marker', _str_or_none(marker)),
            ('maxresults', _int_or_none(maxresults)),
            ('include', _str_or_none(include))
        ]
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_blob_header(
            request, self.authentication)
        response = self._perform_request(request)

        return _parse_blob_enum_results_list(response)

    def set_blob_service_properties(self, logging=None, hour_metrics=None, 
                                    minute_metrics=None, cors=None, target_version=None,
                                    timeout=None):
        '''
        Sets the properties of a storage account's Blob service, including
        Azure Storage Analytics. If an element (ex Logging) is left as None, the 
        existing settings on the service for that functionality are preserved.

        :param Logging logging:
            Groups the Azure Analytics Logging settings.
        :param Metrics hour_metrics:
            The hour metrics settings provide a summary of request 
            statistics grouped by API in hourly aggregates for blobs.
        :param Metrics minute_metrics:
            The minute metrics settings provide request statistics 
            for each minute for blobs.
        :param cors:
            You can include up to five CorsRule elements in the 
            list. If an empty list is specified, all CORS rules will be deleted, 
            and CORS will be disabled for the service.
        :type cors: list of :class:`CorsRule`
        :param string target_version:
            Indicates the default version to use for requests if an incoming 
            request's version is not specified. 
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = '/?restype=service&comp=properties'
        request.query = [('timeout', _int_or_none(timeout))]
        request.body = _get_request_body(
            _convert_service_properties_to_xml(logging, hour_metrics, minute_metrics, cors, target_version))
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_blob_header(
            request, self.authentication)
        self._perform_request(request)

    def get_blob_service_properties(self, timeout=None):
        '''
        Gets the properties of a storage account's Blob service, including
        Azure Storage Analytics.

        timeout:
            Optional. The timeout parameter is expressed in seconds.
        '''
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = '/?restype=service&comp=properties'
        request.query = [('timeout', _int_or_none(timeout))]
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_blob_header(
            request, self.authentication)
        response = self._perform_request(request)

        return _convert_xml_to_service_properties(response.body)

    def get_blob_properties(self, container_name, blob_name,
                            snapshot=None, x_ms_lease_id=None,
                            if_modified_since=None,
                            if_unmodified_since=None,
                            if_match=None, if_none_match=None):
        '''
        Returns all user-defined metadata, standard HTTP properties, and
        system properties for the blob.

        container_name:
            Name of existing container.
        blob_name:
            Name of existing blob.
        snapshot:
            Optional. The snapshot parameter is an opaque DateTime value that,
            when present, specifies the blob snapshot to retrieve.
        x_ms_lease_id:
            Required if the blob has an active lease.
        if_modified_since:
            Optional. Datetime string.
        if_unmodified_since:
            Optional. DateTime string.
        if_match:
            Optional. An ETag value.
        if_none_match:
            Optional. An ETag value.
        '''
        _validate_not_none('container_name', container_name)
        _validate_not_none('blob_name', blob_name)
        request = HTTPRequest()
        request.method = 'HEAD'
        request.host = self._get_host()
        request.path = '/' + _str(container_name) + '/' + _str(blob_name) + ''
        request.headers = [
            ('x-ms-lease-id', _str_or_none(x_ms_lease_id)),
            ('If-Modified-Since', _str_or_none(if_modified_since)),
            ('If-Unmodified-Since', _str_or_none(if_unmodified_since)),
            ('If-Match', _str_or_none(if_match)),
            ('If-None-Match', _str_or_none(if_none_match)),
        ]
        request.query = [('snapshot', _str_or_none(snapshot))]
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_blob_header(
            request, self.authentication)

        response = self._perform_request(request)

        return _parse_response_for_dict(response)

    def set_blob_properties(self, container_name, blob_name,
                            x_ms_blob_cache_control=None,
                            x_ms_blob_content_type=None,
                            x_ms_blob_content_md5=None,
                            x_ms_blob_content_encoding=None,
                            x_ms_blob_content_language=None,
                            x_ms_lease_id=None,
                            x_ms_blob_content_disposition=None,
                            if_modified_since=None,
                            if_unmodified_since=None,
                            if_match=None,
                            if_none_match=None):
        '''
        Sets system properties on the blob.

        container_name:
            Name of existing container.
        blob_name:
            Name of existing blob.
        x_ms_blob_cache_control:
            Optional. Modifies the cache control string for the blob.
        x_ms_blob_content_type:
            Optional. Sets the blob's content type.
        x_ms_blob_content_md5:
            Optional. Sets the blob's MD5 hash.
        x_ms_blob_content_encoding:
            Optional. Sets the blob's content encoding.
        x_ms_blob_content_language:
            Optional. Sets the blob's content language.
        x_ms_lease_id:
            Required if the blob has an active lease.
        x_ms_blob_content_disposition:
            Optional. Sets the blob's Content-Disposition header.
            The Content-Disposition response header field conveys additional
            information about how to process the response payload, and also can
            be used to attach additional metadata. For example, if set to
            attachment, it indicates that the user-agent should not display the
            response, but instead show a Save As dialog with a filename other
            than the blob name specified.
        if_modified_since:
            Optional. Datetime string.
        if_unmodified_since:
            Optional. DateTime string.
        if_match:
            Optional. An ETag value.
        if_none_match:
            Optional. An ETag value.
        '''
        _validate_not_none('container_name', container_name)
        _validate_not_none('blob_name', blob_name)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = '/' + \
            _str(container_name) + '/' + _str(blob_name) + '?comp=properties'
        request.headers = [
            ('x-ms-blob-cache-control', _str_or_none(x_ms_blob_cache_control)),
            ('x-ms-blob-content-type', _str_or_none(x_ms_blob_content_type)),
            ('x-ms-blob-content-disposition',
             _str_or_none(x_ms_blob_content_disposition)),
            ('x-ms-blob-content-md5', _str_or_none(x_ms_blob_content_md5)),
            ('x-ms-blob-content-encoding',
             _str_or_none(x_ms_blob_content_encoding)),
            ('x-ms-blob-content-language',
             _str_or_none(x_ms_blob_content_language)),
            ('If-Modified-Since', _str_or_none(if_modified_since)),
            ('If-Unmodified-Since', _str_or_none(if_unmodified_since)),
            ('If-Match', _str_or_none(if_match)),
            ('If-None-Match', _str_or_none(if_none_match)),
            ('x-ms-lease-id', _str_or_none(x_ms_lease_id))
        ]
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_blob_header(
            request, self.authentication)
        self._perform_request(request)

    def get_blob(self, container_name, blob_name, snapshot=None,
                 x_ms_range=None, x_ms_lease_id=None,
                 x_ms_range_get_content_md5=None,
                 if_modified_since=None, if_unmodified_since=None,
                 if_match=None, if_none_match=None):
        '''
        Reads or downloads a blob from the system, including its metadata and
        properties.

        See get_blob_to_* for high level functions that handle the download
        of large blobs with automatic chunking and progress notifications.

        container_name:
            Name of existing container.
        blob_name:
            Name of existing blob.
        snapshot:
            Optional. The snapshot parameter is an opaque DateTime value that,
            when present, specifies the blob snapshot to retrieve.
        x_ms_range:
            Optional. Return only the bytes of the blob in the specified range.
        x_ms_lease_id:
            Required if the blob has an active lease.
        x_ms_range_get_content_md5:
            Optional. When this header is set to true and specified together
            with the Range header, the service returns the MD5 hash for the
            range, as long as the range is less than or equal to 4 MB in size.
        if_modified_since:
            Optional. Datetime string.
        if_unmodified_since:
            Optional. DateTime string.
        if_match:
            Optional. An ETag value.
        if_none_match:
            Optional. An ETag value.
        '''
        _validate_not_none('container_name', container_name)
        _validate_not_none('blob_name', blob_name)
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = '/' + _str(container_name) + '/' + _str(blob_name) + ''
        request.headers = [
            ('x-ms-range', _str_or_none(x_ms_range)),
            ('x-ms-lease-id', _str_or_none(x_ms_lease_id)),
            ('x-ms-range-get-content-md5',
             _str_or_none(x_ms_range_get_content_md5)),
            ('If-Modified-Since', _str_or_none(if_modified_since)),
            ('If-Unmodified-Since', _str_or_none(if_unmodified_since)),
            ('If-Match', _str_or_none(if_match)),
            ('If-None-Match', _str_or_none(if_none_match)),
        ]
        request.query = [('snapshot', _str_or_none(snapshot))]
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_blob_header(
            request, self.authentication)
        response = self._perform_request(request, None)

        return _create_blob_result(response)

    def get_blob_to_path(self, container_name, blob_name, file_path,
                         open_mode='wb', snapshot=None, x_ms_lease_id=None,
                         progress_callback=None,
                         max_connections=1, max_retries=5, retry_wait=1.0,
                         if_modified_since=None, if_unmodified_since=None,
                         if_match=None, if_none_match=None):
        '''
        Downloads a blob to a file path, with automatic chunking and progress
        notifications.

        container_name:
            Name of existing container.
        blob_name:
            Name of existing blob.
        file_path:
            Path of file to write to.
        open_mode:
            Mode to use when opening the file.
        snapshot:
            Optional. The snapshot parameter is an opaque DateTime value that,
            when present, specifies the blob snapshot to retrieve.
        x_ms_lease_id:
            Required if the blob has an active lease.
        progress_callback:
            Callback for progress with signature function(current, total) where
            current is the number of bytes transfered so far, and total is the
            size of the blob.
        max_connections:
            Maximum number of parallel connections to use when the blob size
            exceeds 64MB.
            Set to 1 to download the blob chunks sequentially.
            Set to 2 or more to download the blob chunks in parallel. This uses
            more system resources but will download faster.
        max_retries:
            Number of times to retry download of blob chunk if an error occurs.
        retry_wait:
            Sleep time in secs between retries.
        if_modified_since:
            Optional. Datetime string.
        if_unmodified_since:
            Optional. DateTime string.
        if_match:
            Optional. An ETag value.
        if_none_match:
            Optional. An ETag value.
        '''
        _validate_not_none('container_name', container_name)
        _validate_not_none('blob_name', blob_name)
        _validate_not_none('file_path', file_path)
        _validate_not_none('open_mode', open_mode)

        with open(file_path, open_mode) as stream:
            self.get_blob_to_file(container_name,
                                  blob_name,
                                  stream,
                                  snapshot,
                                  x_ms_lease_id,
                                  progress_callback,
                                  max_connections,
                                  max_retries,
                                  retry_wait,
                                  if_modified_since,
                                  if_unmodified_since,
                                  if_match,
                                  if_none_match)

    def get_blob_to_file(self, container_name, blob_name, stream,
                         snapshot=None, x_ms_lease_id=None,
                         progress_callback=None,
                         max_connections=1, max_retries=5, retry_wait=1.0,
                         if_modified_since=None, if_unmodified_since=None,
                         if_match=None, if_none_match=None):
        '''
        Downloads a blob to a file/stream, with automatic chunking and progress
        notifications.

        container_name:
            Name of existing container.
        blob_name:
            Name of existing blob.
        stream:
            Opened file/stream to write to.
        snapshot:
            Optional. The snapshot parameter is an opaque DateTime value that,
            when present, specifies the blob snapshot to retrieve.
        x_ms_lease_id:
            Required if the blob has an active lease.
        progress_callback:
            Callback for progress with signature function(current, total) where
            current is the number of bytes transfered so far, and total is the
            size of the blob.
        max_connections:
            Maximum number of parallel connections to use when the blob size
            exceeds 64MB.
            Set to 1 to download the blob chunks sequentially.
            Set to 2 or more to download the blob chunks in parallel. This uses
            more system resources but will download faster.
            Note that parallel download requires the stream to be seekable.
        max_retries:
            Number of times to retry download of blob chunk if an error occurs.
        retry_wait:
            Sleep time in secs between retries.
        if_modified_since:
            Optional. Datetime string.
        if_unmodified_since:
            Optional. DateTime string.
        if_match:
            Optional. An ETag value.
        if_none_match:
            Optional. An ETag value.
        '''
        _validate_not_none('container_name', container_name)
        _validate_not_none('blob_name', blob_name)
        _validate_not_none('stream', stream)

        props = self.get_blob_properties(container_name, blob_name)
        blob_size = int(props['content-length'])

        if blob_size < self._BLOB_MAX_DATA_SIZE:
            if progress_callback:
                progress_callback(0, blob_size)

            data = self.get_blob(container_name,
                                 blob_name,
                                 snapshot,
                                 x_ms_lease_id=x_ms_lease_id,
                                 if_modified_since=if_modified_since,
                                 if_unmodified_since=if_unmodified_since,
                                 if_match=if_match,
                                 if_none_match=if_none_match)

            stream.write(data)

            if progress_callback:
                progress_callback(blob_size, blob_size)
        else:
            _download_blob_chunks(
                self,
                container_name,
                blob_name,
                blob_size,
                self._BLOB_MAX_CHUNK_DATA_SIZE,
                stream,
                max_connections,
                max_retries,
                retry_wait,
                progress_callback
            )

    def get_blob_to_bytes(self, container_name, blob_name, snapshot=None,
                          x_ms_lease_id=None, progress_callback=None,
                          max_connections=1, max_retries=5, retry_wait=1.0,
                          if_modified_since=None, if_unmodified_since=None,
                          if_match=None, if_none_match=None):
        '''
        Downloads a blob as an array of bytes, with automatic chunking and
        progress notifications.

        container_name:
            Name of existing container.
        blob_name:
            Name of existing blob.
        snapshot:
            Optional. The snapshot parameter is an opaque DateTime value that,
            when present, specifies the blob snapshot to retrieve.
        x_ms_lease_id:
            Required if the blob has an active lease.
        progress_callback:
            Callback for progress with signature function(current, total) where
            current is the number of bytes transfered so far, and total is the
            size of the blob.
        max_connections:
            Maximum number of parallel connections to use when the blob size
            exceeds 64MB.
            Set to 1 to download the blob chunks sequentially.
            Set to 2 or more to download the blob chunks in parallel. This uses
            more system resources but will download faster.
        max_retries:
            Number of times to retry download of blob chunk if an error occurs.
        retry_wait:
            Sleep time in secs between retries.
        if_modified_since:
            Optional. Datetime string.
        if_unmodified_since:
            Optional. DateTime string.
        if_match:
            Optional. An ETag value.
        if_none_match:
            Optional. An ETag value.
        '''
        _validate_not_none('container_name', container_name)
        _validate_not_none('blob_name', blob_name)

        stream = BytesIO()
        self.get_blob_to_file(container_name,
                              blob_name,
                              stream,
                              snapshot,
                              x_ms_lease_id,
                              progress_callback,
                              max_connections,
                              max_retries,
                              retry_wait,
                              if_modified_since,
                              if_unmodified_since,
                              if_match,
                              if_none_match)

        return stream.getvalue()

    def get_blob_to_text(self, container_name, blob_name, text_encoding='utf-8',
                         snapshot=None, x_ms_lease_id=None,
                         progress_callback=None,
                         max_connections=1, max_retries=5, retry_wait=1.0,
                         if_modified_since=None, if_unmodified_since=None,
                         if_match=None, if_none_match=None):
        '''
        Downloads a blob as unicode text, with automatic chunking and progress
        notifications.

        container_name:
            Name of existing container.
        blob_name:
            Name of existing blob.
        text_encoding:
            Encoding to use when decoding the blob data.
        snapshot:
            Optional. The snapshot parameter is an opaque DateTime value that,
            when present, specifies the blob snapshot to retrieve.
        x_ms_lease_id:
            Required if the blob has an active lease.
        progress_callback:
            Callback for progress with signature function(current, total) where
            current is the number of bytes transfered so far, and total is the
            size of the blob.
        max_connections:
            Maximum number of parallel connections to use when the blob size
            exceeds 64MB.
            Set to 1 to download the blob chunks sequentially.
            Set to 2 or more to download the blob chunks in parallel. This uses
            more system resources but will download faster.
        max_retries:
            Number of times to retry download of blob chunk if an error occurs.
        retry_wait:
            Sleep time in secs between retries.
        if_modified_since:
            Optional. Datetime string.
        if_unmodified_since:
            Optional. DateTime string.
        if_match:
            Optional. An ETag value.
        if_none_match:
            Optional. An ETag value.
        '''
        _validate_not_none('container_name', container_name)
        _validate_not_none('blob_name', blob_name)
        _validate_not_none('text_encoding', text_encoding)

        result = self.get_blob_to_bytes(container_name,
                                        blob_name,
                                        snapshot,
                                        x_ms_lease_id,
                                        progress_callback,
                                        max_connections,
                                        max_retries,
                                        retry_wait,
                                        if_modified_since,
                                        if_unmodified_since,
                                        if_match,
                                        if_none_match)

        return result.decode(text_encoding)

    def get_blob_metadata(self, container_name, blob_name, snapshot=None,
                          x_ms_lease_id=None, if_modified_since=None,
                          if_unmodified_since=None,
                          if_match=None, if_none_match=None):
        '''
        Returns all user-defined metadata for the specified blob or snapshot.

        container_name:
            Name of existing container.
        blob_name:
            Name of existing blob.
        snapshot:
            Optional. The snapshot parameter is an opaque DateTime value that,
            when present, specifies the blob snapshot to retrieve.
        x_ms_lease_id:
            Required if the blob has an active lease.
        if_modified_since:
            Optional. Datetime string.
        if_unmodified_since:
            Optional. DateTime string.
        if_match:
            Optional. An ETag value.
        if_none_match:
            Optional. An ETag value.
        '''
        _validate_not_none('container_name', container_name)
        _validate_not_none('blob_name', blob_name)
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = '/' + \
            _str(container_name) + '/' + _str(blob_name) + '?comp=metadata'
        request.headers = [
            ('x-ms-lease-id', _str_or_none(x_ms_lease_id)),
            ('If-Modified-Since', _str_or_none(if_modified_since)),
            ('If-Unmodified-Since', _str_or_none(if_unmodified_since)),
            ('If-Match', _str_or_none(if_match)),
            ('If-None-Match', _str_or_none(if_none_match)),
        ]
        request.query = [('snapshot', _str_or_none(snapshot))]
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_blob_header(
            request, self.authentication)
        response = self._perform_request(request)

        return _parse_response_for_dict_prefix(response, prefixes=['x-ms-meta'])

    def set_blob_metadata(self, container_name, blob_name,
                          x_ms_meta_name_values=None, x_ms_lease_id=None,
                          if_modified_since=None, if_unmodified_since=None,
                          if_match=None, if_none_match=None):
        '''
        Sets user-defined metadata for the specified blob as one or more
        name-value pairs.

        container_name:
            Name of existing container.
        blob_name:
            Name of existing blob.
        x_ms_meta_name_values:
            Dict containing name and value pairs.
        x_ms_lease_id:
            Required if the blob has an active lease.
        if_modified_since:
            Optional. Datetime string.
        if_unmodified_since:
            Optional. DateTime string.
        if_match:
            Optional. An ETag value.
        if_none_match:
            Optional. An ETag value
        '''
        _validate_not_none('container_name', container_name)
        _validate_not_none('blob_name', blob_name)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = '/' + \
            _str(container_name) + '/' + _str(blob_name) + '?comp=metadata'
        request.headers = [
            ('x-ms-meta-name-values', x_ms_meta_name_values),
            ('If-Modified-Since', _str_or_none(if_modified_since)),
            ('If-Unmodified-Since', _str_or_none(if_unmodified_since)),
            ('If-Match', _str_or_none(if_match)),
            ('If-None-Match', _str_or_none(if_none_match)),
            ('x-ms-lease-id', _str_or_none(x_ms_lease_id))
        ]
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_blob_header(
            request, self.authentication)
        self._perform_request(request)

    def _lease_blob_impl(self, container_name, blob_name,
                         x_ms_lease_action, x_ms_lease_id,
                         x_ms_lease_duration, x_ms_lease_break_period,
                         x_ms_proposed_lease_id, if_modified_since,
                         if_unmodified_since, if_match, if_none_match):
        '''
        Establishes and manages a lock on a blob for write and delete
        operations.

        container_name:
            Name of existing container.
        blob_name:
            Name of existing blob.
        x_ms_lease_action:
            Possible LeaseActions acquire|renew|release|break|change
        x_ms_lease_id:
            Required if the blob has an active lease.
        x_ms_lease_duration:
            Specifies the duration of the lease, in seconds, or negative one
            (-1) for a lease that never expires. A non-infinite lease can be
            between 15 and 60 seconds. A lease duration cannot be changed
            using renew or change.
        x_ms_lease_break_period:
            Optional. For a break operation, this is the proposed duration of
            seconds that the lease should continue before it is broken, between
            0 and 60 seconds. This break period is only used if it is shorter
            than the time remaining on the lease. If longer, the time remaining
            on the lease is used. A new lease will not be available before the
            break period has expired, but the lease may be held for longer than
            the break period. If this header does not appear with a break
            operation, a fixed-duration lease breaks after the remaining lease
            period elapses, and an infinite lease breaks immediately.
        x_ms_proposed_lease_id:
            Optional for acquire, required for change. Proposed lease ID, in a
            GUID string format.
        if_modified_since:
            Datetime string.
        if_unmodified_since:
            DateTime string.
        if_match:
            Snapshot the blob only if its ETag value matches the
            value specified.
        if_none_match:
            An ETag value.
        '''
        _validate_not_none('container_name', container_name)
        _validate_not_none('blob_name', blob_name)
        _validate_not_none('x_ms_lease_action', x_ms_lease_action)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = '/' + \
            _str(container_name) + '/' + _str(blob_name) + '?comp=lease'
        request.headers = [
            ('x-ms-lease-id', _str_or_none(x_ms_lease_id)),
            ('x-ms-lease-action', _str_or_none(x_ms_lease_action)),
            ('x-ms-lease-duration', _str_or_none(x_ms_lease_duration)),
            ('x-ms-lease-break-period', _str_or_none(x_ms_lease_break_period)),
            ('x-ms-proposed-lease-id', _str_or_none(x_ms_proposed_lease_id)),
            ('If-Modified-Since', _str_or_none(if_modified_since)),
            ('If-Unmodified-Since', _str_or_none(if_unmodified_since)),
            ('If-Match', _str_or_none(if_match)),
            ('If-None-Match', _str_or_none(if_none_match)),
        ]
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_blob_header(
            request, self.authentication)
        response = self._perform_request(request)

        return _parse_response_for_dict_filter(
            response,
            filter=['x-ms-lease-id', 'x-ms-lease-time'])

    def acquire_blob_lease(self, container_name, blob_name,
                           x_ms_lease_duration=-1,
                           x_ms_proposed_lease_id=None,
                           if_modified_since=None,
                           if_unmodified_since=None,
                           if_match=None,
                           if_none_match=None):
        '''
        Acquires a lock on a blob for write and delete operations.

        container_name:
            Name of existing container.
        blob_name:
            Name of existing blob.
        x_ms_lease_duration:
            Optional. Specifies the duration of the lease, in seconds, or negative one
            (-1) for a lease that never expires. A non-infinite lease can be
            between 15 and 60 seconds. A lease duration cannot be changed
            using renew or change. Default is -1 (infinite lease).
        x_ms_proposed_lease_id:
            Optional. Proposed lease ID, in a GUID string format.
        if_modified_since:
            Optional. Datetime string.
        if_unmodified_since:
            Optional. DateTime string.
        if_match:
            Optional. An ETag value.
        if_none_match:
            Optional. An ETag value.
        '''
        _validate_not_none('x_ms_lease_duration', x_ms_lease_duration)

        if x_ms_lease_duration is not -1 and\
           (x_ms_lease_duration < 15 or x_ms_lease_duration > 60):
            raise ValueError("x_ms_lease_duration param needs to be between 15 and 60 or -1.")
        return self._lease_blob_impl(container_name,
                                     blob_name,
                                     LeaseActions.Acquire,
                                     None, # x_ms_lease_id
                                     x_ms_lease_duration,
                                     None, # x_ms_lease_break_period
                                     x_ms_proposed_lease_id,
                                     if_modified_since,
                                     if_unmodified_since,
                                     if_match,
                                     if_none_match)

    def renew_blob_lease(self, container_name, blob_name,
                         x_ms_lease_id, if_modified_since=None,
                         if_unmodified_since=None, if_match=None,
                         if_none_match=None):
        '''
        Renews a lock on a blob for write and delete operations.

        container_name:
            Name of existing container.
        blob_name:
            Name of existing blob.
        x_ms_lease_id:
            Lease ID for active lease.
        if_modified_since:
            Optional. Datetime string.
        if_unmodified_since:
            Optional. DateTime string.
        if_match:
            Optional. An ETag value.
        if_none_match:
            Optional. An ETag value.
        '''
        _validate_not_none('x_ms_lease_id', x_ms_lease_id)

        return self._lease_blob_impl(container_name,
                                     blob_name,
                                     LeaseActions.Renew,
                                     x_ms_lease_id,
                                     None, # x_ms_lease_duration
                                     None, # x_ms_lease_break_period
                                     None, # x_ms_proposed_lease_id
                                     if_modified_since,
                                     if_unmodified_since,
                                     if_match,
                                     if_none_match)

    def release_blob_lease(self, container_name, blob_name,
                           x_ms_lease_id, if_modified_since=None,
                           if_unmodified_since=None, if_match=None,
                           if_none_match=None):
        '''
        Releases a lock on a blob for write and delete operations.

        container_name:
            Name of existing container.
        blob_name:
            Name of existing blob.
        x_ms_lease_id:
            Lease ID for active lease.
        if_modified_since:
            Optional. Datetime string.
        if_unmodified_since:
            Optional. DateTime string.
        if_match:
            Optional. An ETag value.
        if_none_match:
            Optional. An ETag value.
        '''
        _validate_not_none('x_ms_lease_id', x_ms_lease_id)

        return self._lease_blob_impl(container_name,
                                     blob_name,
                                     LeaseActions.Release,
                                     x_ms_lease_id,
                                     None, # x_ms_lease_duration
                                     None, # x_ms_lease_break_period
                                     None, # x_ms_proposed_lease_id
                                     if_modified_since,
                                     if_unmodified_since,
                                     if_match,
                                     if_none_match)

    def break_blob_lease(self, container_name, blob_name,
                         x_ms_lease_id,
                         x_ms_lease_break_period=None,
                         if_modified_since=None,
                         if_unmodified_since=None,
                         if_match=None,
                         if_none_match=None):
        '''
        Breaks a lock on a blob for write and delete operations.

        container_name:
            Name of existing container.
        blob_name:
            Name of existing blob.
        x_ms_lease_id:
            Lease ID for active lease.
        x_ms_lease_break_period:
            Optional. For a break operation, this is the proposed duration of
            seconds that the lease should continue before it is broken, between
            0 and 60 seconds. This break period is only used if it is shorter
            than the time remaining on the lease. If longer, the time remaining
            on the lease is used. A new lease will not be available before the
            break period has expired, but the lease may be held for longer than
            the break period. If this header does not appear with a break
            operation, a fixed-duration lease breaks after the remaining lease
            period elapses, and an infinite lease breaks immediately.
        if_modified_since:
            Optional. Datetime string.
        if_unmodified_since:
            Optional. DateTime string.
        if_match:
            Optional. An ETag value.
        if_none_match:
            Optional. An ETag value.
        '''
        _validate_not_none('x_ms_lease_id', x_ms_lease_id)
        if (x_ms_lease_break_period is not None) and (x_ms_lease_break_period < 0 or x_ms_lease_break_period > 60):
            raise ValueError("x_ms_lease_break_period param needs to be between 0 and 60.")

        return self._lease_blob_impl(container_name,
                                     blob_name,
                                     LeaseActions.Break,
                                     x_ms_lease_id,
                                     None, # x_ms_lease_duration
                                     x_ms_lease_break_period,
                                     None, # x_ms_proposed_lease_id
                                     if_modified_since,
                                     if_unmodified_since,
                                     if_match,
                                     if_none_match)

    def change_blob_lease(self, container_name, blob_name,
                         x_ms_lease_id,
                         x_ms_proposed_lease_id,
                         if_modified_since=None,
                         if_unmodified_since=None,
                         if_match=None,
                         if_none_match=None):
        '''
        Changes a lock on a blob for write and delete operations.

        container_name:
            Name of existing container.
        blob_name:
            Name of existing blob.
        x_ms_lease_id:
            Required if the blob has an active lease.
        x_ms_proposed_lease_id:
            Proposed lease ID, in a GUID string format.
        if_modified_since:
            Optional. Datetime string.
        if_unmodified_since:
            Optional. DateTime string.
        if_match:
            Optional. An ETag value.
        if_none_match:
            Optional. An ETag value.
        '''
        return self._lease_blob_impl(container_name,
                                     blob_name,
                                     LeaseActions.Change,
                                     x_ms_lease_id,
                                     None, # x_ms_lease_duration
                                     None, # x_ms_lease_break_period
                                     x_ms_proposed_lease_id,
                                     if_modified_since,
                                     if_unmodified_since,
                                     if_match,
                                     if_none_match)

    def snapshot_blob(self, container_name, blob_name,
                      x_ms_meta_name_values=None, if_modified_since=None,
                      if_unmodified_since=None, if_match=None,
                      if_none_match=None, x_ms_lease_id=None):
        '''
        Creates a read-only snapshot of a blob.

        container_name:
            Name of existing container.
        blob_name:
            Name of existing blob.
        x_ms_meta_name_values:
            Optional. Dict containing name and value pairs.
        if_modified_since:
            Optional. Datetime string.
        if_unmodified_since:
            DateTime string.
        if_match:
            Optional. An ETag value.
        if_none_match:
            Optional. An ETag value
        x_ms_lease_id:
            Required if the blob has an active lease.
        '''
        _validate_not_none('container_name', container_name)
        _validate_not_none('blob_name', blob_name)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = '/' + \
            _str(container_name) + '/' + _str(blob_name) + '?comp=snapshot'
        request.headers = [
            ('x-ms-meta-name-values', x_ms_meta_name_values),
            ('If-Modified-Since', _str_or_none(if_modified_since)),
            ('If-Unmodified-Since', _str_or_none(if_unmodified_since)),
            ('If-Match', _str_or_none(if_match)),
            ('If-None-Match', _str_or_none(if_none_match)),
            ('x-ms-lease-id', _str_or_none(x_ms_lease_id))
        ]
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_blob_header(
            request, self.authentication)
        response = self._perform_request(request)

        return _parse_response_for_dict_filter(
            response,
            filter=['x-ms-snapshot', 'etag', 'last-modified'])

    def copy_blob(self, container_name, blob_name, x_ms_copy_source,
                  x_ms_meta_name_values=None,
                  x_ms_source_if_modified_since=None,
                  x_ms_source_if_unmodified_since=None,
                  x_ms_source_if_match=None, x_ms_source_if_none_match=None,
                  if_modified_since=None, if_unmodified_since=None,
                  if_match=None, if_none_match=None, x_ms_lease_id=None,
                  x_ms_source_lease_id=None):
        '''
        Copies a blob to a destination within the storage account.

        container_name:
            Name of existing container.
        blob_name:
            Name of existing blob.
        x_ms_copy_source:
            URL up to 2 KB in length that specifies a blob. A source blob in
            the same account can be private, but a blob in another account
            must be public or accept credentials included in this URL, such as
            a Shared Access Signature. Examples:
            https://myaccount.blob.core.windows.net/mycontainer/myblob
            https://myaccount.blob.core.windows.net/mycontainer/myblob?snapshot=<DateTime>
        x_ms_meta_name_values:
            Optional. Dict containing name and value pairs.
        x_ms_source_if_modified_since:
            Optional. An ETag value. Specify this conditional header to copy
            the source blob only if its ETag matches the value specified.
        x_ms_source_if_unmodified_since:
            Optional. An ETag value. Specify this conditional header to copy
            the blob only if its ETag does not match the value specified.
        x_ms_source_if_match:
            Optional. A DateTime value. Specify this conditional header to
            copy the blob only if the source blob has been modified since the
            specified date/time.
        x_ms_source_if_none_match:
            Optional. An ETag value. Specify this conditional header to copy
            the source blob only if its ETag matches the value specified.
        if_modified_since:
            Optional. Datetime string.
        if_unmodified_since:
            DateTime string.
        if_match:
            Optional. An ETag value.
        if_none_match:
            Optional. An ETag value
        x_ms_lease_id:
            Required if the blob has an active lease.
        x_ms_source_lease_id:
            Optional. Specify this to perform the Copy Blob operation only if
            the lease ID given matches the active lease ID of the source blob.
        '''
        _validate_not_none('container_name', container_name)
        _validate_not_none('blob_name', blob_name)
        _validate_not_none('x_ms_copy_source', x_ms_copy_source)

        if x_ms_copy_source.startswith('/'):
            # Backwards compatibility for earlier versions of the SDK where
            # the copy source can be in the following formats:
            # - Blob in named container:
            #     /accountName/containerName/blobName
            # - Snapshot in named container:
            #     /accountName/containerName/blobName?snapshot=<DateTime>
            # - Blob in root container:
            #     /accountName/blobName
            # - Snapshot in root container:
            #     /accountName/blobName?snapshot=<DateTime>
            account, _, source =\
                x_ms_copy_source.partition('/')[2].partition('/')
            x_ms_copy_source = self.protocol + '://' + \
                account + self.host_base + '/' + source

        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = '/' + _str(container_name) + '/' + _str(blob_name) + ''
        request.headers = [
            ('x-ms-copy-source', _str_or_none(x_ms_copy_source)),
            ('x-ms-meta-name-values', x_ms_meta_name_values),
            ('x-ms-source-if-modified-since',
             _str_or_none(x_ms_source_if_modified_since)),
            ('x-ms-source-if-unmodified-since',
             _str_or_none(x_ms_source_if_unmodified_since)),
            ('x-ms-source-if-match', _str_or_none(x_ms_source_if_match)),
            ('x-ms-source-if-none-match',
             _str_or_none(x_ms_source_if_none_match)),
            ('If-Modified-Since', _str_or_none(if_modified_since)),
            ('If-Unmodified-Since', _str_or_none(if_unmodified_since)),
            ('If-Match', _str_or_none(if_match)),
            ('If-None-Match', _str_or_none(if_none_match)),
            ('x-ms-lease-id', _str_or_none(x_ms_lease_id)),
            ('x-ms-source-lease-id', _str_or_none(x_ms_source_lease_id))
        ]

        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_blob_header(
            request, self.authentication)
        response = self._perform_request(request)

        return _parse_response_for_dict(response)

    def abort_copy_blob(self, container_name, blob_name, x_ms_copy_id,
                        x_ms_lease_id=None):
        '''
         Aborts a pending copy_blob operation, and leaves a destination blob
         with zero length and full metadata.

         container_name:
             Name of destination container.
         blob_name:
             Name of destination blob.
         x_ms_copy_id:
            Copy identifier provided in the x-ms-copy-id of the original
            copy_blob operation.
         x_ms_lease_id:
            Required if the destination blob has an active infinite lease.
        '''
        _validate_not_none('container_name', container_name)
        _validate_not_none('blob_name', blob_name)
        _validate_not_none('x_ms_copy_id', x_ms_copy_id)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = '/' + _str(container_name) + '/' + \
            _str(blob_name) + '?comp=copy&copyid=' + \
            _str(x_ms_copy_id)
        request.headers = [
            ('x-ms-lease-id', _str_or_none(x_ms_lease_id)),
            ('x-ms-copy-action', 'abort'),
        ]
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_blob_header(
            request, self.authentication)
        self._perform_request(request)

    def delete_blob(self, container_name, blob_name, snapshot=None,
                    timeout=None, x_ms_lease_id=None,
                    x_ms_delete_snapshots=None,
                    if_modified_since=None, if_unmodified_since=None,
                    if_match=None, if_none_match=None):
        '''
        Marks the specified blob or snapshot for deletion. The blob is later
        deleted during garbage collection.

        To mark a specific snapshot for deletion provide the date/time of the
        snapshot via the snapshot parameter.

        container_name:
            Name of existing container.
        blob_name:
            Name of existing blob.
        snapshot:
            Optional. The snapshot parameter is an opaque DateTime value that,
            when present, specifies the blob snapshot to delete.
        timeout:
            Optional. The timeout parameter is expressed in seconds.
            The Blob service returns an error when the timeout interval elapses
            while processing the request.
        x_ms_lease_id:
            Required if the blob has an active lease.
        x_ms_delete_snapshots:
            Required if the blob has associated snapshots. Specify one of the
            following two options:
                include:
                    Delete the base blob and all of its snapshots.
                only:
                    Delete only the blob's snapshots and not the blob itself.
            This header should be specified only for a request against the base
            blob resource. If this header is specified on a request to delete
            an individual snapshot, the Blob service returns status code 400
            (Bad Request). If this header is not specified on the request and
            the blob has associated snapshots, the Blob service returns status
            code 409 (Conflict).
        if_modified_since:
            Optional. Datetime string.
        if_unmodified_since:
            Optional. DateTime string.
        if_match:
            Optional. An ETag value.
        if_none_match:
            Optional. An ETag value.
        '''
        _validate_not_none('container_name', container_name)
        _validate_not_none('blob_name', blob_name)
        request = HTTPRequest()
        request.method = 'DELETE'
        request.host = self._get_host()
        request.path = '/' + _str(container_name) + '/' + _str(blob_name) + ''
        request.headers = [
            ('x-ms-lease-id', _str_or_none(x_ms_lease_id)),
            ('x-ms-delete-snapshots', _str_or_none(x_ms_delete_snapshots)),
            ('If-Modified-Since', _str_or_none(if_modified_since)),
            ('If-Unmodified-Since', _str_or_none(if_unmodified_since)),
            ('If-Match', _str_or_none(if_match)),
            ('If-None-Match', _str_or_none(if_none_match)),
        ]
        request.query = [
            ('snapshot', _str_or_none(snapshot)),
            ('timeout', _int_or_none(timeout))
        ]
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_blob_header(
            request, self.authentication)
        self._perform_request(request)
