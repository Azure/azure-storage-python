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
    _update_request_uri_local_storage,
    _extract_etag,
)
from .._http import HTTPRequest
from ..models import Services
from .models import TablePayloadFormat
from ..auth import (
    StorageSASAuthentication,
    StorageTableSharedKeyAuthentication,
)
from ..connection import (
    StorageConnectionParameters,
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
)
from ..constants import (
    TABLE_SERVICE_HOST_BASE,
    DEV_TABLE_HOST,
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

    def __init__(self, account_name=None, account_key=None, protocol='https',
                 host_base=TABLE_SERVICE_HOST_BASE, dev_host=DEV_TABLE_HOST,
                 sas_token=None, connection_string=None, request_session=None):
        '''
        account_name:
            your storage account name, required for all operations.
        account_key:
            your storage account key, required for all operations.
        protocol:
            Optional. Protocol. Defaults to http.
        host_base:
            Optional. Live host base url. Defaults to Azure url. Override this
            for on-premise.
        dev_host:
            Optional. Dev host url. Defaults to localhost.
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
            Optional. Session object to use for http requests.
        '''
        if connection_string is not None:
            connection_params = StorageConnectionParameters(connection_string)
            account_name = connection_params.account_name
            account_key = connection_params.account_key
            protocol = connection_params.protocol.lower()
            host_base = connection_params.host_base_table
            
        super(TableService, self).__init__(
            account_name, account_key, protocol, host_base, dev_host, sas_token, request_session)

        if self.account_key:
            self.authentication = StorageTableSharedKeyAuthentication(
                self.account_name,
                self.account_key,
            )
        elif self.sas_token:
            self.authentication = StorageSASAuthentication(self.sas_token)
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
        request.path = _update_request_uri_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_table_header(request)
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
        request.path = _update_request_uri_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_table_header(request)
        self._perform_request(request)

    def query_tables(self, table_name=None, top=None, next_table_name=None, timeout=None):
        '''
        Returns a list of tables under the specified account.

        table_name:
            Optional.  The specific table to query.
        top:
            Optional. Maximum number of tables to return.
        next_table_name:
            Optional. When top is used, the next table name is stored in
            result.x_ms_continuation['NextTableName']
        :param int timeout:
            The timeout parameter is expressed in seconds.
        '''
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        if table_name is not None:
            uri_part_table_name = "('" + table_name + "')"
        else:
            uri_part_table_name = ""
        request.path = '/Tables' + uri_part_table_name
        request.headers = [('Accept', TablePayloadFormat.JSON_NO_METADATA)]
        request.query = [
            ('$top', _int_or_none(top)),
            ('NextTableName', _str_or_none(next_table_name)),
            ('timeout', _int_or_none(timeout)),
        ]
        request.path = _update_request_uri_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_table_header(request)
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
        request.path = _update_request_uri_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_table_header(request)
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
        request.path = _update_request_uri_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_table_header(request)
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
        request.path = _update_request_uri_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_table_header(request)
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
        request.path = _update_request_uri_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_table_header(request)
        self._perform_request(request)

    def query_entities(self, table_name, filter=None, select=None, top=None,
                       next_partition_key=None, next_row_key=None,
                       accept=TablePayloadFormat.JSON_MINIMAL_METADATA,
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
        next_partition_key:
            Optional. When top is used, the next partition key is stored in
            result.x_ms_continuation['NextPartitionKey']
        next_row_key:
            Optional. When top is used, the next partition key is stored in
            result.x_ms_continuation['NextRowKey']
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
            ('NextPartitionKey', _str_or_none(next_partition_key)),
            ('NextRowKey', _str_or_none(next_row_key)),
            ('timeout', _int_or_none(timeout)),
        ]
        request.path = _update_request_uri_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_table_header(request)
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
        request.path = _update_request_uri_local_storage(
            request, self.use_local_storage)

        # Update the batch operation requests with table and client specific info
        for row_key, batch_request in batch._requests:
            batch_request.host = self._get_host()
            if batch_request.method == 'POST':
                batch_request.path = '/' + _str(table_name)
            else:
                batch_request.path = _get_entity_path(table_name, batch._partition_key, row_key)
            batch_request.path = _update_request_uri_local_storage(
                batch_request, self.use_local_storage)

        # Construct the batch body
        request.body, boundary = _convert_batch_to_json(batch._requests)
        request.headers = [('Content-Type', boundary)]

        # Add the table and storage specific headers, including content length
        request.headers = _update_storage_table_header(request)

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
        request.path = _update_request_uri_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_table_header(request)

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
        request.path = _update_request_uri_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_table_header(request)

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
        request.path = _update_request_uri_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_table_header(request)

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
        request.path = _update_request_uri_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_table_header(request)

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
        request.path = _update_request_uri_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_table_header(request)

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
        request.path = _update_request_uri_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_table_header(request)

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
        request.path = _update_request_uri_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_table_header(request)

        response = self._perform_request(request)
        return _extract_etag(response)

    def _perform_request_worker(self, request):
        self.authentication.sign_request(request)
        return self._httpclient.perform_request(request)
