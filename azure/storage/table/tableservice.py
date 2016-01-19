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
from contextlib import contextmanager
from azure.common import (
    AzureHttpError,
)
from .._common_conversion import (
    _int_or_none,
    _str,
    _str_or_none,
)
from .._error import (
    _dont_fail_not_exist,
    _dont_fail_on_exist,
    _validate_not_none,
    _ERROR_STORAGE_MISSING_INFO,
)
from .._serialization import (
    _get_request_body,
    _update_request,
    _convert_signed_identifiers_to_xml,
    _convert_service_properties_to_xml,
)
from .._http import HTTPRequest
from ..models import (
    Services,
    ListGenerator,
)
from .models import TablePayloadFormat
from ..auth import (
    _StorageSASAuthentication,
    _StorageTableSharedKeyAuthentication,
)
from ..connection import (
    _ServiceParameters,
)
from .._deserialization import (
    _convert_xml_to_service_properties,
    _convert_xml_to_signed_identifiers,
)
from ._serialization import (
    _convert_table_to_json,
    _convert_batch_to_json,
    _update_storage_table_header,
    _get_entity_path,
    _DEFAULT_ACCEPT_HEADER,
    _DEFAULT_CONTENT_TYPE_HEADER,
    _DEFAULT_PREFER_HEADER,
)
from ._deserialization import (
    _convert_json_response_to_entity,
    _convert_json_response_to_tables,
    _convert_json_response_to_entities,
    _parse_batch_response,
    _extract_etag,
)
from ..constants import (
    SERVICE_HOST_BASE,
    DEFAULT_PROTOCOL,
)
from ._request import (
    _get_entity,
    _insert_entity,
    _update_entity,
    _merge_entity,
    _delete_entity,
    _insert_or_replace_entity,
    _insert_or_merge_entity,
)
from ..sharedaccesssignature import (
    SharedAccessSignature,
)
from ..storageclient import _StorageClient
from .tablebatch import TableBatch

class TableService(_StorageClient):

    '''
    This is the main class managing Table resources.
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
            'table',
            account_name=account_name, 
            account_key=account_key, 
            sas_token=sas_token, 
            is_emulated=is_emulated, 
            protocol=protocol, 
            endpoint_suffix=endpoint_suffix,
            request_session=request_session,
            connection_string=connection_string)
            
        super(TableService, self).__init__(service_params)

        if self.account_key:
            self.authentication = _StorageTableSharedKeyAuthentication(
                self.account_name,
                self.account_key,
            )
        elif self.sas_token:
            self.authentication = _StorageSASAuthentication(self.sas_token)
        else:
            raise ValueError(_ERROR_STORAGE_MISSING_INFO)


    def generate_account_shared_access_signature(self, resource_types, permission, 
                                        expiry, start=None, ip=None, protocol=None):
        '''
        Generates a shared access signature for the table service.
        Use the returned signature with the sas_token parameter of TableService.

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
        return sas.generate_account(Services.TABLE, resource_types, permission, 
                                    expiry, start=start, ip=ip, protocol=protocol)


    def generate_table_shared_access_signature(self, table_name, permission=None, 
                                        expiry=None, start=None, id=None,
                                        ip=None, protocol=None,
                                        start_pk=None, start_rk=None, 
                                        end_pk=None, end_rk=None):
        '''
        Generates a shared access signature for the table.
        Use the returned signature with the sas_token parameter of TableService.

        :param str table_name:
            Name of table.
        :param TablePermissions permission:
            The permissions associated with the shared access signature. The 
            user is restricted to operations allowed by the permissions. 
            Required unless an id is given referencing a stored access policy 
            which contains this field. This field must be omitted if it has been 
            specified in an associated stored access policy.
            Permissions must be ordered query, add, update, delete if passed as 
            a string.
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
        :param str start_pk
            The minimum partition key accessible with this shared access 
            signature. startpk must accompany startrk. Key values are inclusive. 
            If omitted, there is no lower bound on the table entities that can 
            be accessed.
        :param str start_rk
            The minimum row key accessible with this shared access signature. 
            startpk must accompany startrk. Key values are inclusive. If 
            omitted, there is no lower bound on the table entities that can be 
            accessed.
        :param str end_pk
            The maximum partition key accessible with this shared access 
            signature. endpk must accompany endrk. Key values are inclusive. If 
            omitted, there is no upper bound on the table entities that can be 
            accessed.
        :param str end_rk
            The maximum row key accessible with this shared access signature. 
            endpk must accompany endrk. Key values are inclusive. If omitted, 
            there is no upper bound on the table entities that can be accessed.
        '''
        _validate_not_none('table_name', table_name)
        _validate_not_none('self.account_name', self.account_name)
        _validate_not_none('self.account_key', self.account_key)

        sas = SharedAccessSignature(self.account_name, self.account_key)
        return sas.generate_table(
            table_name,
            permission=permission, 
            expiry=expiry,
            start=start, 
            id=id,
            ip=ip,
            protocol=protocol,
            start_pk=start_pk,
            start_rk=start_rk,
            end_pk=end_pk,
            end_rk=end_rk,
        )

    def get_table_service_properties(self, timeout=None):
        '''
        Gets the properties of a storage account's Table service, including
        Azure Storage Analytics.

        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = '/'
        request.query = [
            ('restype', 'service'),
            ('comp', 'properties'),
            ('timeout', _int_or_none(timeout)),
        ]

        response = self._perform_request(request)
        return _convert_xml_to_service_properties(response.body)

    def set_table_service_properties(self, logging=None, hour_metrics=None, 
                                    minute_metrics=None, cors=None, timeout=None):
        '''
        Sets the properties of a storage account's Table service, including
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
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = '/'
        request.query = [
            ('restype', 'service'),
            ('comp', 'properties'),
            ('timeout', _int_or_none(timeout)),
        ]
        request.body = _get_request_body(
            _convert_service_properties_to_xml(logging, hour_metrics, minute_metrics, cors))

        self._perform_request(request)

    def list_tables(self, max_results=None, marker=None, timeout=None):
        '''
        Returns a list of tables under the specified account.

        max_results:
            Optional. Maximum number of tables to return.
        marker:
            A string value that identifies the portion of the query to be
            returned with the next query operation. The operation returns a
            next_marker element within the response body if the list returned
            was not complete. This value may then be used as a query parameter
            in a subsequent call to request the next portion of the list of
            queues. The marker value is opaque to the client.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        kwargs = {'max_results': max_results, 'marker': marker, 'timeout': timeout}
        resp = self._list_tables(**kwargs)

        return ListGenerator(resp, self._list_tables, (), kwargs)

    def _list_tables(self, max_results=None, marker=None, timeout=None):
        '''
        Returns a list of tables under the specified account.

        max_results:
            Optional. Maximum number of tables to return.
        marker:
            A string value that identifies the portion of the query to be
            returned with the next query operation. The operation returns a
            next_marker element within the response body if the list returned
            was not complete. This value may then be used as a query parameter
            in a subsequent call to request the next portion of the list of
            queues. The marker value is opaque to the client.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = '/Tables'
        request.headers = [('Accept', TablePayloadFormat.JSON_NO_METADATA)]
        request.query = [
            ('$top', _int_or_none(max_results)),
            ('NextTableName', _str_or_none(marker)),
            ('timeout', _int_or_none(timeout)),
        ]

        response = self._perform_request(request)
        return _convert_json_response_to_tables(response)

    def create_table(self, table, fail_on_exist=False, timeout=None):
        '''
        Creates a new table in the storage account.

        table:
            Name of the table to create. Table name may contain only
            alphanumeric characters and cannot begin with a numeric character.
            It is case-insensitive and must be from 3 to 63 characters long.
        fail_on_exist:
            Specify whether throw exception when table exists.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('table', table)
        request = HTTPRequest()
        request.method = 'POST'
        request.host = self._get_host()
        request.path = '/Tables'
        request.query = [('timeout', _int_or_none(timeout))]
        request.headers = [_DEFAULT_CONTENT_TYPE_HEADER,
                           _DEFAULT_PREFER_HEADER,
                           _DEFAULT_ACCEPT_HEADER]
        request.body = _get_request_body(_convert_table_to_json(table))

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

    def exists(self, table_name, timeout=None):
        '''
        Returns a boolean indicating whether the table exists.

        table_name:
            Name of table to check for existence.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('table_name', table_name)
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = '/Tables' + "('" + table_name + "')"
        request.headers = [('Accept', TablePayloadFormat.JSON_NO_METADATA)]
        request.query = [('timeout', _int_or_none(timeout))]

        try:
            self._perform_request(request)
            return True
        except AzureHttpError as ex:
            _dont_fail_not_exist(ex)
            return False

    def delete_table(self, table_name, fail_not_exist=False, timeout=None):
        '''
        table_name:
            Name of the table to delete.
        fail_not_exist:
            Specify whether throw exception when table doesn't exist.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('table_name', table_name)
        request = HTTPRequest()
        request.method = 'DELETE'
        request.host = self._get_host()
        request.path = '/Tables(\'' + _str(table_name) + '\')'
        request.query = [('timeout', _int_or_none(timeout))]
        request.headers = [_DEFAULT_ACCEPT_HEADER]

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

    def get_table_acl(self, table_name, timeout=None):
        '''
        Returns details about any stored access policies specified on the
        table that may be used with Shared Access Signatures.

        :param str table_name:
            Name of existing table.
        :return: A dictionary of access policies associated with the table.
        :rtype: dict of str to :class:`.AccessPolicy`:
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('table_name', table_name)
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = '/' + _str(table_name)
        request.query = [
            ('comp', 'acl'),
            ('timeout', _int_or_none(timeout)),
        ]

        response = self._perform_request(request)
        return _convert_xml_to_signed_identifiers(response.body)

    def set_table_acl(self, table_name, signed_identifiers=None, timeout=None):
        '''
        Sets stored access policies for the table that may be used with
        Shared Access Signatures.

        :param str table_name:
            Name of existing table.
        :param signed_identifiers:
            A dictionary of access policies to associate with the table. The 
            dictionary may contain up to 5 elements. An empty dictionary 
            will clear the access policies set on the service. 
        :type signed_identifiers: dict of str to :class:`.AccessPolicy`:
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('table_name', table_name)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = '/' + _str(table_name)
        request.query = [
            ('comp', 'acl'),
            ('timeout', _int_or_none(timeout)),
        ]
        request.body = _get_request_body(
            _convert_signed_identifiers_to_xml(signed_identifiers))

        self._perform_request(request)

    def query_entities(self, table_name, filter=None, select=None, top=None,
                       marker=None, accept=TablePayloadFormat.JSON_MINIMAL_METADATA,
                       property_resolver=None, timeout=None):
        '''
        Get entities in a table; includes the $filter and $select options.

        table_name:
            Table to query.
        filter:
            Optional. Filter as described at
            http://msdn.microsoft.com/en-us/library/windowsazure/dd894031.aspx
        select:
            Optional. Property names to select from the entities.
        top:
            Optional. Maximum number of entities to return.
        marker:
            A value that identifies the portion of the list
            to be returned with the next list operation. The operation returns
            a next_marker value within the response body if the list returned was
            not complete. The marker value may then be used in a subsequent
            call to request the next set of list items. The marker value is
            opaque to the client.
        accept:
            Required. Specifies the accepted content type of the response 
            payload. See TablePayloadFormat for possible values.
        property_resolver:
            Optional. A function which given the partition key, row key, 
            property name, property value, and the property EdmType if 
            returned by the service, returns the EdmType of the property.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        args = (table_name,)
        kwargs = {'filter': filter, 'select': select, 'top': top, 'marker': marker, 
                  'accept': accept, 'property_resolver': property_resolver, 'timeout': timeout}
        resp = self._query_entities(*args, **kwargs)

        return ListGenerator(resp, self._query_entities, args, kwargs)

    def _query_entities(self, table_name, filter=None, select=None, top=None,
                       marker=None, accept=TablePayloadFormat.JSON_MINIMAL_METADATA,
                       property_resolver=None, timeout=None):
        '''
        Get entities in a table; includes the $filter and $select options.

        table_name:
            Table to query.
        filter:
            Optional. Filter as described at
            http://msdn.microsoft.com/en-us/library/windowsazure/dd894031.aspx
        select:
            Optional. Property names to select from the entities.
        top:
            Optional. Maximum number of entities to return.
        marker:
            A value that identifies the portion of the query
            to be returned with the next query operation. The operation returns
            a next_marker value within the response body if the list returned was
            not complete. The marker value may then be used in a subsequent
            call to request the next set of query items. The marker value is
            opaque to the client.
        accept:
            Required. Specifies the accepted content type of the response 
            payload. See TablePayloadFormat for possible values.
        property_resolver:
            Optional. A function which given the partition key, row key, 
            property name, property value, and the property EdmType if 
            returned by the service, returns the EdmType of the property.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('table_name', table_name)
        _validate_not_none('accept', accept)
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = '/' + _str(table_name) + '()'
        request.headers = [('Accept', _str(accept))]
        request.query = [
            ('$filter', _str_or_none(filter)),
            ('$select', _str_or_none(select)),
            ('$top', _int_or_none(top)),
            ('NextPartitionKey', _str_or_none(marker.next_partition_key)),
            ('NextRowKey', _str_or_none(marker.next_row_key)),
            ('timeout', _int_or_none(timeout)),
        ]

        response = self._perform_request(request)
        return _convert_json_response_to_entities(response, property_resolver)

    def commit_batch(self, table_name, batch, timeout=None):
        '''
        Commits a batch request.

        table_name:
            Table name.
        batch:
            a Batch object
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('table_name', table_name)

        # Construct the batch request
        request = HTTPRequest()
        request.method = 'POST'
        request.host = self._get_host()
        request.path = '/' + '$batch'
        request.query = [('timeout', _int_or_none(timeout))]

        # Update the batch operation requests with table and client specific info
        for row_key, batch_request in batch._requests:
            batch_request.host = self._get_host()
            if batch_request.method == 'POST':
                batch_request.path = '/' + _str(table_name)
            else:
                batch_request.path = _get_entity_path(table_name, batch._partition_key, row_key)
            _update_request(batch_request)

        # Construct the batch body
        request.body, boundary = _convert_batch_to_json(batch._requests)
        request.headers = [('Content-Type', boundary)]

        # Perform the batch request and return the response
        response = self._perform_request(request)
        responses = _parse_batch_response(response.body)
        return responses

    @contextmanager
    def batch(self, table_name, timeout=None):
        '''
        Creates a batch object which can be used as a context manager.
        Commits the batch on exit.

        table_name:
            Table name.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        batch = TableBatch()
        yield batch
        self.commit_batch(table_name, batch, timeout=timeout)

    def get_entity(self, table_name, partition_key, row_key, select=None,
                   accept=TablePayloadFormat.JSON_MINIMAL_METADATA,
                   property_resolver=None, timeout=None):
        '''
        Get an entity in a table; includes the $select options.

        table_name:
            Table name.
        partition_key:
            PartitionKey of the entity.
        row_key:
            RowKey of the entity.
        select:
            Optional. Property names to select.
        accept:
            Required. Specifies the accepted content type of the response 
            payload. See TablePayloadFormat for possible values.
        property_resolver:
            Optional. A function which given the partition key, row key, 
            property name, property value, and the property EdmType if 
            returned by the service, returns the EdmType of the property.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('table_name', table_name)
        request = _get_entity(partition_key, row_key, select, accept)
        request.host = self._get_host()
        request.path = _get_entity_path(table_name, partition_key, row_key)
        request.query += [('timeout', _int_or_none(timeout))]

        response = self._perform_request(request)
        return _convert_json_response_to_entity(response, property_resolver)

    def insert_entity(self, table_name, entity, timeout=None):
        '''
        Inserts a new entity into a table.

        table_name:
            Table name.
        entity:
            Required. The entity object to insert. Could be a dict format or
            entity object. Must contain a PartitionKey and a RowKey.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('table_name', table_name)
        request = _insert_entity(entity)
        request.host = self._get_host()
        request.path = '/' + _str(table_name)
        request.query += [('timeout', _int_or_none(timeout))]

        response = self._perform_request(request)
        return _extract_etag(response)

    def update_entity(self, table_name, entity, if_match='*', timeout=None):
        '''
        Updates an existing entity in a table. The Update Entity operation
        replaces the entire entity and can be used to remove properties.

        table_name:
            Table name.
        entity:
            Required. The entity object to insert. Could be a dict format or
            entity object. Must contain a PartitionKey and a RowKey.
        if_match:
            Required. The client may specify the ETag for the entity on the 
            request in order to compare to the ETag maintained by the service 
            for the purpose of optimistic concurrency. The update operation 
            will be performed only if the ETag sent by the client matches the 
            value maintained by the server, indicating that the entity has 
            not been modified since it was retrieved by the client. To force 
            an unconditional update, set If-Match to the wildcard character (*).
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('table_name', table_name)
        request = _update_entity(entity, if_match)
        request.host = self._get_host()
        request.path = _get_entity_path(table_name, entity['PartitionKey'], entity['RowKey'])
        request.query += [('timeout', _int_or_none(timeout))]

        response = self._perform_request(request)
        return _extract_etag(response)

    def merge_entity(self, table_name, entity, if_match='*', timeout=None):
        '''
        Updates an existing entity by updating the entity's properties. This
        operation does not replace the existing entity as the Update Entity
        operation does.

        table_name:
            Table name.
        entity:
            Required. The entity object to insert. Can be a dict format or
            entity object. Must contain a PartitionKey and a RowKey.
        if_match:
            Required. The client may specify the ETag for the entity on the 
            request in order to compare to the ETag maintained by the service 
            for the purpose of optimistic concurrency. The merge operation 
            will be performed only if the ETag sent by the client matches the 
            value maintained by the server, indicating that the entity has 
            not been modified since it was retrieved by the client. To force 
            an unconditional merge, set If-Match to the wildcard character (*).
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('table_name', table_name)
        request = _merge_entity(entity, if_match)
        request.host = self._get_host()
        request.query += [('timeout', _int_or_none(timeout))]
        request.path = _get_entity_path(table_name, entity['PartitionKey'], entity['RowKey'])

        response = self._perform_request(request)
        return _extract_etag(response)

    def delete_entity(self, table_name, partition_key, row_key,
                      if_match='*', timeout=None):
        '''
        Deletes an existing entity in a table.

        table_name:
            Table name.
        partition_key:
            PartitionKey of the entity.
        row_key:
            RowKey of the entity.
        if_match:
            Required. The client may specify the ETag for the entity on the 
            request in order to compare to the ETag maintained by the service 
            for the purpose of optimistic concurrency. The delete operation 
            will be performed only if the ETag sent by the client matches the 
            value maintained by the server, indicating that the entity has 
            not been modified since it was retrieved by the client. To force 
            an unconditional delete, set If-Match to the wildcard character (*).
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('table_name', table_name)
        request = _delete_entity(partition_key, row_key, if_match)
        request.host = self._get_host()
        request.query += [('timeout', _int_or_none(timeout))]
        request.path = _get_entity_path(table_name, partition_key, row_key)

        self._perform_request(request)

    def insert_or_replace_entity(self, table_name, entity, timeout=None):
        '''
        Replaces an existing entity or inserts a new entity if it does not
        exist in the table. Because this operation can insert or update an
        entity, it is also known as an "upsert" operation.

        table_name:
            Table name.
        entity:
            Required. The entity object to insert. Could be a dict format or
            entity object. Must contain a PartitionKey and a RowKey.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('table_name', table_name)
        request = _insert_or_replace_entity(entity)
        request.host = self._get_host()
        request.query += [('timeout', _int_or_none(timeout))]
        request.path = _get_entity_path(table_name, entity['PartitionKey'], entity['RowKey'])

        response = self._perform_request(request)
        return _extract_etag(response)

    def insert_or_merge_entity(self, table_name, entity, timeout=None):
        '''
        Merges an existing entity or inserts a new entity if it does not exist
        in the table. Because this operation can insert or update an entity,
        it is also known as an "upsert" operation.

        table_name:
            Table name.
        entity:
            Required. The entity object to insert. Could be a dict format or
            entity object. Must contain a PartitionKey and a RowKey.
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        _validate_not_none('table_name', table_name)
        request = _insert_or_merge_entity(entity)
        request.host = self._get_host()
        request.query += [('timeout', _int_or_none(timeout))]
        request.path = _get_entity_path(table_name, entity['PartitionKey'], entity['RowKey'])

        response = self._perform_request(request)
        return _extract_etag(response)

    def _perform_request_worker(self, request):
        _update_storage_table_header(request)
        return super(TableService, self)._perform_request_worker(request)