﻿#-------------------------------------------------------------------------
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
from .._common_conversion import (
    _int_or_none,
    _str,
    _str_or_none,
)
from .._common_error import (
    _dont_fail_not_exist,
    _dont_fail_on_exist,
    _validate_not_none,
    _ERROR_STORAGE_MISSING_INFO,
)
from .._common_serialization import (
    _convert_class_to_xml,
    _get_request_body,
    _parse_response_for_dict,
    _parse_response_for_dict_filter,
    _update_request_uri_query_local_storage,
    _ETreeXmlToObject,
)
from .._http import HTTPRequest
from .._http.batchclient import _BatchClient
from ..models import (
    SignedIdentifiers,
    StorageServiceProperties,
)
from .models import (
    TableSharedAccessPermissions,
)
from ..auth import (
    StorageSASAuthentication,
    StorageTableSharedKeyAuthentication,
)
from ..connection import (
    StorageConnectionParameters,
)
from .._serialization import (
    _convert_signed_identifiers_to_xml,
)
from ._serialization import (
    _convert_entity_to_xml,
    _convert_etree_element_to_entity,
    _convert_etree_element_to_table,
    _convert_response_to_entity,
    _convert_table_to_xml,
    _update_storage_table_header,
)
from ..constants import (
    TABLE_SERVICE_HOST_BASE,
    DEFAULT_HTTP_TIMEOUT,
    DEV_TABLE_HOST,
    X_MS_VERSION,
)
from ..sharedaccesssignature import (
    SharedAccessSignature,
)
from ..storageclient import _StorageClient


class TableService(_StorageClient):

    '''
    This is the main class managing Table resources.
    '''

    def __init__(self, account_name=None, account_key=None, protocol='https',
                 host_base=TABLE_SERVICE_HOST_BASE, dev_host=DEV_TABLE_HOST,
                 timeout=DEFAULT_HTTP_TIMEOUT, sas_token=None, connection_string=None,
                 request_session=None):
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
            Optional. Session object to use for http requests.
        '''
        if connection_string is not None:
            connection_params = StorageConnectionParameters(connection_string)
            account_name = connection_params.account_name
            account_key = connection_params.account_key
            protocol = connection_params.protocol.lower()
            host_base = connection_params.host_base_table
            
        super(TableService, self).__init__(
            account_name, account_key, protocol, host_base, dev_host, timeout, sas_token, request_session)

        if self.account_key:
            self.authentication = StorageTableSharedKeyAuthentication(
                self.account_name,
                self.account_key,
            )
        elif self.sas_token:
            self.authentication = StorageSASAuthentication(self.sas_token)
        else:
            raise ValueError(_ERROR_STORAGE_MISSING_INFO)

    def generate_shared_access_signature(self, table_name,
                                         shared_access_policy=None,
                                         sas_version=X_MS_VERSION):
        '''
        Generates a shared access signature for the table.
        Use the returned signature with the sas_token parameter of TableService.

        table_name:
            Required. Name of table.
        shared_access_policy:
            Instance of SharedAccessPolicy class.
        sas_version:
            x-ms-version for storage service, or None to get a signed query
            string compatible with pre 2012-02-12 clients, where the version
            is not included in the query string.
        '''
        _validate_not_none('table_name', table_name)
        _validate_not_none('shared_access_policy', shared_access_policy)
        _validate_not_none('self.account_name', self.account_name)
        _validate_not_none('self.account_key', self.account_key)

        sas = SharedAccessSignature(self.account_name, self.account_key)
        return sas.generate_signed_query_string(
            table_name,
            None,
            shared_access_policy,
            sas_version,
            table_name=table_name,
        )

    def begin_batch(self):
        if self._batchclient is None:
            self._batchclient = _BatchClient(
                service_instance=self,
                authentication=self.authentication,
                request_session=self._httpclient.request_session,
                timeout=self._httpclient.timeout,
                user_agent=self._httpclient.user_agent,
            )
        return self._batchclient.begin_batch()

    def commit_batch(self):
        try:
            ret = self._batchclient.commit_batch()
        finally:
            self._batchclient = None
        return ret

    def cancel_batch(self):
        self._batchclient = None

    def get_table_service_properties(self):
        '''
        Gets the properties of a storage account's Table service, including
        Windows Azure Storage Analytics.
        '''
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = '/?restype=service&comp=properties'
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_table_header(request)
        response = self._perform_request(request)

        return _ETreeXmlToObject.parse_response(
            response, StorageServiceProperties)

    def set_table_service_properties(self, storage_service_properties):
        '''
        Sets the properties of a storage account's Table Service, including
        Windows Azure Storage Analytics.

        storage_service_properties:
            StorageServiceProperties object.
        '''
        _validate_not_none('storage_service_properties',
                           storage_service_properties)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = '/?restype=service&comp=properties'
        request.body = _get_request_body(
            _convert_class_to_xml(storage_service_properties))
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_table_header(request)
        response = self._perform_request(request)

        return _parse_response_for_dict(response)

    def query_tables(self, table_name=None, top=None, next_table_name=None):
        '''
        Returns a list of tables under the specified account.

        table_name:
            Optional.  The specific table to query.
        top:
            Optional. Maximum number of tables to return.
        next_table_name:
            Optional. When top is used, the next table name is stored in
            result.x_ms_continuation['NextTableName']
        '''
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        if table_name is not None:
            uri_part_table_name = "('" + table_name + "')"
        else:
            uri_part_table_name = ""
        request.path = '/Tables' + uri_part_table_name + ''
        request.query = [
            ('$top', _int_or_none(top)),
            ('NextTableName', _str_or_none(next_table_name))
        ]
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_table_header(request)
        response = self._perform_request(request)

        return _ETreeXmlToObject.convert_response_to_feeds(
            response, _convert_etree_element_to_table)

    def create_table(self, table, fail_on_exist=False):
        '''
        Creates a new table in the storage account.

        table:
            Name of the table to create. Table name may contain only
            alphanumeric characters and cannot begin with a numeric character.
            It is case-insensitive and must be from 3 to 63 characters long.
        fail_on_exist:
            Specify whether throw exception when table exists.
        '''
        _validate_not_none('table', table)
        request = HTTPRequest()
        request.method = 'POST'
        request.host = self._get_host()
        request.path = '/Tables'
        request.body = _get_request_body(_convert_table_to_xml(table))
        request.path, request.query = _update_request_uri_query_local_storage(
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

    def delete_table(self, table_name, fail_not_exist=False):
        '''
        table_name:
            Name of the table to delete.
        fail_not_exist:
            Specify whether throw exception when table doesn't exist.
        '''
        _validate_not_none('table_name', table_name)
        request = HTTPRequest()
        request.method = 'DELETE'
        request.host = self._get_host()
        request.path = '/Tables(\'' + _str(table_name) + '\')'
        request.path, request.query = _update_request_uri_query_local_storage(
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

    def get_table_acl(self, table_name):
        '''
        Returns details about any stored access policies specified on the
        table that may be used with Shared Access Signatures.

        table_name:
            Name of existing table.
        '''
        _validate_not_none('table_name', table_name)
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = '/' + _str(table_name) + '?comp=acl'
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_table_header(request)
        response = self._perform_request(request)

        return _ETreeXmlToObject.parse_response(response, SignedIdentifiers)

    def set_table_acl(self, table_name, signed_identifiers=None):
        '''
        Sets stored access policies for the table that may be used with
        Shared Access Signatures.

        table_name:
            Name of existing table.
        signed_identifiers:
            SignedIdentifers instance
        '''
        _validate_not_none('table_name', table_name)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = '/' + _str(table_name) + '?comp=acl'
        request.body = _get_request_body(
            _convert_signed_identifiers_to_xml(signed_identifiers))
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_table_header(request, content_type=None)
        self._perform_request(request)

    def get_entity(self, table_name, partition_key, row_key, select=''):
        '''
        Get an entity in a table; includes the $select options.

        partition_key:
            PartitionKey of the entity.
        row_key:
            RowKey of the entity.
        select:
            Property names to select.
        '''
        _validate_not_none('table_name', table_name)
        _validate_not_none('partition_key', partition_key)
        _validate_not_none('row_key', row_key)
        _validate_not_none('select', select)
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = '/' + _str(table_name) + \
            '(PartitionKey=\'' + _str(partition_key) + \
            '\',RowKey=\'' + \
            _str(row_key) + '\')?$select=' + \
            _str(select) + ''
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_table_header(request)
        response = self._perform_request(request)

        return _convert_response_to_entity(response)

    def query_entities(self, table_name, filter=None, select=None, top=None,
                       next_partition_key=None, next_row_key=None):
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
        '''
        _validate_not_none('table_name', table_name)
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = '/' + _str(table_name) + '()'
        request.query = [
            ('$filter', _str_or_none(filter)),
            ('$select', _str_or_none(select)),
            ('$top', _int_or_none(top)),
            ('NextPartitionKey', _str_or_none(next_partition_key)),
            ('NextRowKey', _str_or_none(next_row_key))
        ]
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_table_header(request)
        response = self._perform_request(request)

        return _ETreeXmlToObject.convert_response_to_feeds(
            response, _convert_etree_element_to_entity)

    def insert_entity(self, table_name, entity,
                      content_type='application/atom+xml'):
        '''
        Inserts a new entity into a table.

        table_name:
            Table name.
        entity:
            Required. The entity object to insert. Could be a dict format or
            entity object.
        content_type:
            Required. Must be set to application/atom+xml
        '''
        _validate_not_none('table_name', table_name)
        _validate_not_none('entity', entity)
        _validate_not_none('content_type', content_type)
        request = HTTPRequest()
        request.method = 'POST'
        request.host = self._get_host()
        request.path = '/' + _str(table_name) + ''
        request.headers = [('Content-Type', _str_or_none(content_type))]
        request.body = _get_request_body(_convert_entity_to_xml(entity))
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_table_header(request)
        response = self._perform_request(request)

        return _convert_response_to_entity(response)

    def update_entity(self, table_name, partition_key, row_key, entity,
                      content_type='application/atom+xml', if_match='*'):
        '''
        Updates an existing entity in a table. The Update Entity operation
        replaces the entire entity and can be used to remove properties.

        table_name:
            Table name.
        partition_key:
            PartitionKey of the entity.
        row_key:
            RowKey of the entity.
        entity:
            Required. The entity object to insert. Could be a dict format or
            entity object.
        content_type:
            Required. Must be set to application/atom+xml
        if_match:
            Optional. Specifies the condition for which the merge should be
            performed. To force an unconditional merge, set to the wildcard
            character (*).
        '''
        _validate_not_none('table_name', table_name)
        _validate_not_none('partition_key', partition_key)
        _validate_not_none('row_key', row_key)
        _validate_not_none('entity', entity)
        _validate_not_none('content_type', content_type)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = '/' + \
            _str(table_name) + '(PartitionKey=\'' + \
            _str(partition_key) + '\',RowKey=\'' + _str(row_key) + '\')'
        request.headers = [
            ('Content-Type', _str_or_none(content_type)),
            ('If-Match', _str_or_none(if_match))
        ]
        request.body = _get_request_body(_convert_entity_to_xml(entity))
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_table_header(request)
        response = self._perform_request(request)

        return _parse_response_for_dict_filter(response, filter=['etag'])

    def merge_entity(self, table_name, partition_key, row_key, entity,
                     content_type='application/atom+xml', if_match='*'):
        '''
        Updates an existing entity by updating the entity's properties. This
        operation does not replace the existing entity as the Update Entity
        operation does.

        table_name:
            Table name.
        partition_key:
            PartitionKey of the entity.
        row_key:
            RowKey of the entity.
        entity:
            Required. The entity object to insert. Can be a dict format or
            entity object.
        content_type:
            Required. Must be set to application/atom+xml
        if_match:
            Optional. Specifies the condition for which the merge should be
            performed. To force an unconditional merge, set to the wildcard
            character (*).
        '''
        _validate_not_none('table_name', table_name)
        _validate_not_none('partition_key', partition_key)
        _validate_not_none('row_key', row_key)
        _validate_not_none('entity', entity)
        _validate_not_none('content_type', content_type)
        request = HTTPRequest()
        request.method = 'MERGE'
        request.host = self._get_host()
        request.path = '/' + \
            _str(table_name) + '(PartitionKey=\'' + \
            _str(partition_key) + '\',RowKey=\'' + _str(row_key) + '\')'
        request.headers = [
            ('Content-Type', _str_or_none(content_type)),
            ('If-Match', _str_or_none(if_match))
        ]
        request.body = _get_request_body(_convert_entity_to_xml(entity))
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_table_header(request)
        response = self._perform_request(request)

        return _parse_response_for_dict_filter(response, filter=['etag'])

    def delete_entity(self, table_name, partition_key, row_key,
                      content_type='application/atom+xml', if_match='*'):
        '''
        Deletes an existing entity in a table.

        table_name:
            Table name.
        partition_key:
            PartitionKey of the entity.
        row_key:
            RowKey of the entity.
        content_type:
            Required. Must be set to application/atom+xml
        if_match:
            Optional. Specifies the condition for which the delete should be
            performed. To force an unconditional delete, set to the wildcard
            character (*).
        '''
        _validate_not_none('table_name', table_name)
        _validate_not_none('partition_key', partition_key)
        _validate_not_none('row_key', row_key)
        _validate_not_none('content_type', content_type)
        _validate_not_none('if_match', if_match)
        request = HTTPRequest()
        request.method = 'DELETE'
        request.host = self._get_host()
        request.path = '/' + \
            _str(table_name) + '(PartitionKey=\'' + \
            _str(partition_key) + '\',RowKey=\'' + _str(row_key) + '\')'
        request.headers = [
            ('Content-Type', _str_or_none(content_type)),
            ('If-Match', _str_or_none(if_match))
        ]
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_table_header(request)
        self._perform_request(request)

    def insert_or_replace_entity(self, table_name, partition_key, row_key,
                                 entity, content_type='application/atom+xml'):
        '''
        Replaces an existing entity or inserts a new entity if it does not
        exist in the table. Because this operation can insert or update an
        entity, it is also known as an "upsert" operation.

        table_name:
            Table name.
        partition_key:
            PartitionKey of the entity.
        row_key:
            RowKey of the entity.
        entity:
            Required. The entity object to insert. Could be a dict format or
            entity object.
        content_type:
            Required. Must be set to application/atom+xml
        '''
        _validate_not_none('table_name', table_name)
        _validate_not_none('partition_key', partition_key)
        _validate_not_none('row_key', row_key)
        _validate_not_none('entity', entity)
        _validate_not_none('content_type', content_type)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = '/' + \
            _str(table_name) + '(PartitionKey=\'' + \
            _str(partition_key) + '\',RowKey=\'' + _str(row_key) + '\')'
        request.headers = [('Content-Type', _str_or_none(content_type))]
        request.body = _get_request_body(_convert_entity_to_xml(entity))
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_table_header(request)
        response = self._perform_request(request)

        return _parse_response_for_dict_filter(response, filter=['etag'])

    def insert_or_merge_entity(self, table_name, partition_key, row_key,
                               entity, content_type='application/atom+xml'):
        '''
        Merges an existing entity or inserts a new entity if it does not exist
        in the table. Because this operation can insert or update an entity,
        it is also known as an "upsert" operation.

        table_name:
            Table name.
        partition_key:
            PartitionKey of the entity.
        row_key:
            RowKey of the entity.
        entity:
            Required. The entity object to insert. Could be a dict format or
            entity object.
        content_type:
            Required. Must be set to application/atom+xml
        '''
        _validate_not_none('table_name', table_name)
        _validate_not_none('partition_key', partition_key)
        _validate_not_none('row_key', row_key)
        _validate_not_none('entity', entity)
        _validate_not_none('content_type', content_type)
        request = HTTPRequest()
        request.method = 'MERGE'
        request.host = self._get_host()
        request.path = '/' + \
            _str(table_name) + '(PartitionKey=\'' + \
            _str(partition_key) + '\',RowKey=\'' + _str(row_key) + '\')'
        request.headers = [('Content-Type', _str_or_none(content_type))]
        request.body = _get_request_body(_convert_entity_to_xml(entity))
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_table_header(request)
        response = self._perform_request(request)

        return _parse_response_for_dict_filter(response, filter=['etag'])

    def _perform_request_worker(self, request):
        self.authentication.sign_request(request)
        return self._httpclient.perform_request(request)
