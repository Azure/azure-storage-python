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
    AzureConflictHttpError,
    AzureHttpError,
)
from ..constants import (
    SERVICE_HOST_BASE,
    DEFAULT_PROTOCOL,
)
from .._error import (
    _dont_fail_not_exist,
    _dont_fail_on_exist,
    _validate_not_none,
    _ERROR_CONFLICT,
    _ERROR_STORAGE_MISSING_INFO,
)
from .._serialization import (
    _get_request_body,
)
from .._common_conversion import (
    _int_or_none,
    _str,
    _str_or_none,
)
from .._http import (
    HTTPRequest,
)
from ..models import (
    Services,
    ListGenerator,
)
from .models import (
    QueueMessageFormat,
)
from ..auth import (
    _StorageSASAuthentication,
    _StorageSharedKeyAuthentication,
)
from ..connection import (
    _ServiceParameters,
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
    _convert_queue_message_xml,
    _get_path,
)
from ._deserialization import (
    _convert_xml_to_queues,
    _convert_xml_to_queue_messages,
    _parse_queue_message_from_headers,
    _parse_metadata_and_message_count,
)
from ..sharedaccesssignature import (
    SharedAccessSignature,
)
from ..storageclient import _StorageClient


_HTTP_RESPONSE_NO_CONTENT = 204

class QueueService(_StorageClient):

    '''
    This is the main class managing queue resources.

    :ivar encode_function: A function used to encode queue messages. Takes as 
    a parameter the data passed to the put_message API and returns the encoded 
    message. Defaults to take text and xml encode, but bytes and other 
    encodings can be used. For example, base64 may be preferable for developing 
    across multiple Azure Storage libraries in different languages. See the 
    `.QueueMessageFormat` class for xml, base64 and no encoding methods as well 
    as binary equivalents.
    :vartype encode_function: function(data)
    :ivar decode_function: A function used to encode decode messages. Takes as 
    a parameter the data returned by the get_messages and peek_messages APIs and 
    returns the decoded message. Defaults to return text and xml decode, but 
    bytes and other decodings can be used. For example, base64 may be preferable 
    for developing across multiple Azure Storage libraries in different languages. 
    See the `.QueueMessageFormat` class for xml, base64 and no decoding methods 
    as well as binary equivalents.
    :vartype decode_function: function(data)
    '''

    def __init__(self, account_name=None, account_key=None, sas_token=None, 
                 is_emulated=False, protocol=DEFAULT_PROTOCOL, endpoint_suffix=SERVICE_HOST_BASE,
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
        :param bool is_emulated:
            Whether to use the emulator. Defaults to False. If specified, will 
            override all other parameters besides connection string and request 
            session.
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
            'queue',
            account_name=account_name, 
            account_key=account_key, 
            sas_token=sas_token, 
            is_emulated=is_emulated, 
            protocol=protocol, 
            endpoint_suffix=endpoint_suffix,
            request_session=request_session,
            connection_string=connection_string)
            
        super(QueueService, self).__init__(service_params)

        if self.account_key:
            self.authentication = _StorageSharedKeyAuthentication(
                self.account_name,
                self.account_key,
            )
        elif self.sas_token:
            self.authentication = _StorageSASAuthentication(self.sas_token)
        else:
            raise ValueError(_ERROR_STORAGE_MISSING_INFO)

        self.encode_function = QueueMessageFormat.text_xmlencode
        self.decode_function = QueueMessageFormat.text_xmldecode

    def generate_account_shared_access_signature(self, resource_types, permission, 
                                        expiry, start=None, ip=None, protocol=None):
        '''
        Generates a shared access signature for the queue service.
        Use the returned signature with the sas_token parameter of QueueService.

        :param azure.storage.models.ResourceTypes resource_types:
            Specifies the resource types that are accessible with the account SAS.
        :param azure.storage.models.AccountPermissions permission:
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
        return sas.generate_account(Services.QUEUE, resource_types, permission, 
                                    expiry, start=start, ip=ip, protocol=protocol)

    def generate_queue_shared_access_signature(self, queue_name,
                                         permission=None, 
                                         expiry=None,                                       
                                         start=None,
                                         id=None,
                                         ip=None, protocol=None,):
        '''
        Generates a shared access signature for the queue.
        Use the returned signature with the sas_token parameter of QueueService.

        :param str queue_name:
            Name of queue.
        :param str permission:
            The permissions associated with the shared access signature. The 
            user is restricted to operations allowed by the permissions.
            Permissions must be ordered read, add, update, process.
            Required unless an id is given referencing a stored access policy 
            which contains this field. This field must be omitted if it has been 
            specified in an associated stored access policy.
            See :class:`.QueueSharedAccessPermissions`
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
        '''
        _validate_not_none('queue_name', queue_name)
        _validate_not_none('self.account_name', self.account_name)
        _validate_not_none('self.account_key', self.account_key)

        sas = SharedAccessSignature(self.account_name, self.account_key)
        return sas.generate_queue(
            queue_name,
            permission=permission, 
            expiry=expiry,
            start=start, 
            id=id,
            ip=ip,
            protocol=protocol,
        )

    def get_queue_service_properties(self, timeout=None):
        '''
        Gets the properties of a storage account's Queue Service, including
        Azure Storage Analytics.

        :param int timeout:
            The timeout parameter is expressed in seconds.
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

    def list_queues(self, prefix=None, marker=None, max_results=None,
                    include=None, timeout=None):
        '''
        Lists all of the queues in a given storage account.

        :param str prefix:
            Filters the results to return only queues with names that begin
            with the specified prefix.
        :param str marker:
            A string value that identifies the portion of the list
            to be returned with the next list operation. The operation returns
            a next_marker value within the response body if the list returned was
            not complete. The marker value may then be used in a subsequent
            call to request the next set of list items. The marker value is
            opaque to the client.
        :param int max_results:
            Specifies the maximum number of queues to return. If maxresults is
            not specified, the server will return up to 5,000 items.
        :param str include:
            Include this parameter to specify that the container's
            metadata be returned as part of the response body.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        kwargs = {'prefix': prefix, 'marker': marker, 'max_results': max_results, 
                'include': include, 'timeout': timeout}
        resp = self._list_queues(**kwargs)

        return ListGenerator(resp, self._list_queues, (), kwargs)

    def _list_queues(self, prefix=None, marker=None, max_results=None,
                    include=None, timeout=None):
        '''
        Lists all of the queues in a given storage account.

        :param str prefix:
            Filters the results to return only queues with names that begin
            with the specified prefix.
        :param str marker:
            A string value that identifies the portion of the list to be
            returned with the next list operation. The operation returns a
            NextMarker element within the response body if the list returned
            was not complete. This value may then be used as a query parameter
            in a subsequent call to request the next portion of the list of
            queues. The marker value is opaque to the client.
        :param int max_results:
            Specifies the maximum number of queues to return. If maxresults is
            not specified, the server will return up to 5,000 items.
        :param str include:
            Include this parameter to specify that the container's
            metadata be returned as part of the response body.
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
            ('timeout', _int_or_none(timeout))
        ]
        response = self._perform_request(request)

        return _convert_xml_to_queues(response)

    def create_queue(self, queue_name, metadata=None,
                     fail_on_exist=False, timeout=None):
        '''
        Creates a queue under the given account.

        :param str queue_name:
            name of the queue.
        :param dict metadata:
            A dict containing name-value pairs to associate with the
            queue as metadata.
        :param bool fail_on_exist:
            Specify whether throw exception when queue exists.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('queue_name', queue_name)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = _get_path(queue_name)
        request.query = [('timeout', _int_or_none(timeout))]
        request.headers = [('x-ms-meta-name-values', metadata)]
        if not fail_on_exist:
            try:
                response = self._perform_request(request)
                if response.status == _HTTP_RESPONSE_NO_CONTENT:
                    return False
                return True
            except AzureHttpError as ex:
                _dont_fail_on_exist(ex)
                return False
        else:
            response = self._perform_request(request)
            if response.status == _HTTP_RESPONSE_NO_CONTENT:
                raise AzureConflictHttpError(
                    _ERROR_CONFLICT.format(response.message), response.status)
            return True

    def delete_queue(self, queue_name, fail_not_exist=False, timeout=None):
        '''
        Permanently deletes the specified queue.

        :param str queue_name:
            Name of the queue.
        :param bool fail_not_exist:
            Specify whether throw exception when queue doesn't exist.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('queue_name', queue_name)
        request = HTTPRequest()
        request.method = 'DELETE'
        request.host = self._get_host()
        request.path = _get_path(queue_name)
        request.query = [('timeout', _int_or_none(timeout))]
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

    def get_queue_metadata(self, queue_name, timeout=None):
        '''
        Retrieves user-defined metadata and queue properties on the specified
        queue. Metadata is associated with the queue as name-values pairs.

        :param str queue_name:
            Name of the queue.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('queue_name', queue_name)
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = _get_path(queue_name)
        request.query = [
            ('comp', 'metadata'),
            ('timeout', _int_or_none(timeout)),
        ]
        response = self._perform_request(request)

        return _parse_metadata_and_message_count(response)

    def set_queue_metadata(self, queue_name, metadata=None, timeout=None):
        '''
        Sets user-defined metadata on the specified queue. Metadata is
        associated with the queue as name-value pairs.

        :param str queue_name:
            Name of the queue.
        :param dict metadata:
            A dict containing name-value pairs to associate with the
            queue as metadata.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('queue_name', queue_name)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = _get_path(queue_name)
        request.query = [
            ('comp', 'metadata'),
            ('timeout', _int_or_none(timeout)),
        ]
        request.headers = [('x-ms-meta-name-values', metadata)]
        self._perform_request(request)

    def exists(self, queue_name, timeout=None):
        '''
        Returns a boolean indicating whether the queue exists.

        :param str queue_name:
            Name of queue to check for existence.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        try:
            self.get_queue_metadata(queue_name, timeout=timeout)
            return True
        except AzureHttpError as ex:
            _dont_fail_not_exist(ex)
            return False

    def get_queue_acl(self, queue_name, timeout=None):
        '''
        Returns details about any stored access policies specified on the
        queue that may be used with Shared Access Signatures.

        :param str queue_name:
            Name of existing queue.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        :return: A dictionary of access policies associated with the queue.
        :rtype: dict of str to :class:`azure.storage.models.AccessPolicy`:
        '''
        _validate_not_none('queue_name', queue_name)
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = _get_path(queue_name)
        request.query = [
            ('comp', 'acl'),
            ('timeout', _int_or_none(timeout)),
        ]
        response = self._perform_request(request)

        return _convert_xml_to_signed_identifiers(response.body)

    def set_queue_acl(self, queue_name, signed_identifiers=None, timeout=None):
        '''
        Sets stored access policies for the queue that may be used with
        Shared Access Signatures.

        :param str queue_name:
            Name of existing queue.
        :param signed_identifiers:
            A dictionary of access policies to associate with the queue. The 
            dictionary may contain up to 5 elements. An empty dictionary  
            will clear the access policies set on the service. 
        :type signed_identifiers: dict of str to :class:`azure.storage.models.AccessPolicy`:
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('queue_name', queue_name)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = _get_path(queue_name)
        request.query = [
            ('comp', 'acl'),
            ('timeout', _int_or_none(timeout)),
        ]
        request.body = _get_request_body(
            _convert_signed_identifiers_to_xml(signed_identifiers))
        self._perform_request(request)

    def put_message(self, queue_name, content, visibility_timeout=None,
                    time_to_live=None, timeout=None):
        '''
        Adds a new message to the back of the message queue. A visibility
        timeout can also be specified to make the message invisible until the
        visibility timeout expires. A message must be in a format that can be
        included in an XML request with UTF-8 encoding. The encoded message can
        be up to 64KB in size.

        :param str queue_name:
            Name of the queue.
        :param obj content:
            Message content. Allowed type is determined by the encode_function 
            set on the service. Default is str.
        :param int visibility_timeout:
            If not specified, the default value is 0. Specifies the
            new visibility timeout value, in seconds, relative to server time.
            The new value must be larger than or equal to 0, and cannot be
            larger than 7 days. The visibility timeout of a message cannot be
            set to a value later than the expiry time. visibility_timeout
            should be set to a value smaller than the time-to-live value.
        :param int time_to_live:
            Specifies the time-to-live interval for the message, in
            seconds. The maximum time-to-live allowed is 7 days. If this
            parameter is omitted, the default time-to-live is 7 days.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('queue_name', queue_name)
        _validate_not_none('content', content)
        request = HTTPRequest()
        request.method = 'POST'
        request.host = self._get_host()
        request.path = _get_path(queue_name, True)
        request.query = [
            ('visibilitytimeout', _str_or_none(visibility_timeout)),
            ('messagettl', _str_or_none(time_to_live)),
            ('timeout', _int_or_none(timeout))
        ]
        request.body = _get_request_body(_convert_queue_message_xml(content, self.encode_function))
        self._perform_request(request)

    def get_messages(self, queue_name, num_messages=None,
                     visibility_timeout=None, timeout=None):
        '''
        Retrieves one or more messages from the front of the queue.

        :param str queue_name:
            Name of the queue.
        :param int num_messages:
            A nonzero integer value that specifies the number of
            messages to retrieve from the queue, up to a maximum of 32. If
            fewer are visible, the visible messages are returned. By default,
            a single message is retrieved from the queue with this operation.
        :param int visibility_timeout:
            Specifies the new visibility timeout value, in seconds, relative
            to server time. The new value must be larger than or equal to 1
            second, and cannot be larger than 7 days, or larger than 2 hours
            on REST protocol versions prior to version 2011-08-18. The
            visibility timeout of a message can be set to a value later than
            the expiry time.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        :return: A list of QueueMessage objects.
        :rtype: list of `azure.storage.queue.models.QueueMessage`s
        '''
        _validate_not_none('queue_name', queue_name)
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = _get_path(queue_name, True)
        request.query = [
            ('numofmessages', _str_or_none(num_messages)),
            ('visibilitytimeout', _str_or_none(visibility_timeout)),
            ('timeout', _int_or_none(timeout))
        ]
        response = self._perform_request(request)

        return _convert_xml_to_queue_messages(response, self.decode_function)

    def peek_messages(self, queue_name, num_messages=None, timeout=None):
        '''
        Retrieves one or more messages from the front of the queue, but does
        not alter the visibility of the message.

        :param str queue_name:
            Name of the queue.
        :param int num_messages:
            A nonzero integer value that specifies the number of
            messages to peek from the queue, up to a maximum of 32. By default,
            a single message is peeked from the queue with this operation.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        :return: A list of QueueMessage objects.
        :rtype: list of `azure.storage.queue.models.QueueMessage`s
        '''
        _validate_not_none('queue_name', queue_name)
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = _get_path(queue_name, True)
        request.query = [
            ('peekonly', 'true'),
            ('numofmessages', _str_or_none(num_messages)),
            ('timeout', _int_or_none(timeout))]
        response = self._perform_request(request)

        return _convert_xml_to_queue_messages(response, self.decode_function)

    def delete_message(self, queue_name, message_id, pop_receipt, timeout=None):
        '''
        Deletes the specified message.

        :param str queue_name:
            Name of the queue.
        :param str message_id:
            Message to delete.
        :param str pop_receipt:
            A valid pop receipt value returned from an earlier call
            to the Get Messages or Update Message operation.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('queue_name', queue_name)
        _validate_not_none('message_id', message_id)
        _validate_not_none('pop_receipt', pop_receipt)
        request = HTTPRequest()
        request.method = 'DELETE'
        request.host = self._get_host()
        request.path = _get_path(queue_name, True, message_id)
        request.query = [
            ('popreceipt', _str_or_none(pop_receipt)),
            ('timeout', _int_or_none(timeout))]
        self._perform_request(request)

    def clear_messages(self, queue_name, timeout=None):
        '''
        Deletes all messages from the specified queue.

        :param str queue_name:
            Name of the queue.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('queue_name', queue_name)
        request = HTTPRequest()
        request.method = 'DELETE'
        request.host = self._get_host()
        request.path = _get_path(queue_name, True)
        request.query = [('timeout', _int_or_none(timeout))]
        self._perform_request(request)

    def update_message(self, queue_name, message_id, pop_receipt, visibility_timeout, 
                       content=None, timeout=None):
        '''
        Updates the visibility timeout of a message. You can also use this
        operation to update the contents of a message.

        :param str queue_name:
            Name of the queue.
        :param str message_id:
            Message to update.
        :param str pop_receipt:
            A valid pop receipt value returned from an earlier call
            to the Get Messages or Update Message operation.
        :param int visibility_timeout:
            Specifies the new visibility timeout value, in seconds,
            relative to server time. The new value must be larger than or equal
            to 0, and cannot be larger than 7 days. The visibility timeout of a
            message cannot be set to a value later than the expiry time. A
            message can be updated until it has been deleted or has expired.
        :param obj content:
            Message content. Allowed type is determined by the encode_function 
            set on the service. Default is str.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('queue_name', queue_name)
        _validate_not_none('message_id', message_id)
        _validate_not_none('pop_receipt', pop_receipt)
        _validate_not_none('visibility_timeout', visibility_timeout)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = _get_path(queue_name, True, message_id)
        request.query = [
            ('popreceipt', _str_or_none(pop_receipt)),
            ('visibilitytimeout', _int_or_none(visibility_timeout)),
            ('timeout', _int_or_none(timeout))
        ]

        if content is not None:
            request.body = _get_request_body(_convert_queue_message_xml(content, self.encode_function))

        response = self._perform_request(request)
        return _parse_queue_message_from_headers(response)

    def set_queue_service_properties(self, logging=None, hour_metrics=None, 
                                    minute_metrics=None, cors=None, timeout=None):
        '''
        Sets the properties of a storage account's Queue service, including
        Azure Storage Analytics. If an element (ex Logging) is left as None, the 
        existing settings on the service for that functionality are preserved.

        :param azure.storage.models.Logging logging:
            Groups the Azure Analytics Logging settings.
        :param azure.storage.models.Metrics hour_metrics:
            The hour metrics settings provide a summary of request 
            statistics grouped by API in hourly aggregates for blobs.
        :param azure.storage.models.Metrics minute_metrics:
            The minute metrics settings provide request statistics 
            for each minute for blobs.
        :param cors:
            You can include up to five CorsRule elements in the 
            list. If an empty list is specified, all CORS rules will be deleted, 
            and CORS will be disabled for the service.
        :type cors: list of :class:`azure.storage.models.CorsRule`
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
            _convert_service_properties_to_xml(logging, hour_metrics, minute_metrics, cors))
        self._perform_request(request)
