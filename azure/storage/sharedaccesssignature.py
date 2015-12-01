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
from datetime import date

from ._common_conversion import _sign_string
from ._serialization import (
    url_quote,
    _to_utc_datetime,
)
from .constants import X_MS_VERSION


class ResourceType(object):
    RESOURCE_BLOB = 'b'
    RESOURCE_CONTAINER = 'c'
    RESOURCE_FILE = 'f'
    RESOURCE_SHARE = 's'


class QueryStringConstants(object):
    SIGNED_SIGNATURE = 'sig'
    SIGNED_PERMISSION = 'sp'
    SIGNED_START = 'st'
    SIGNED_EXPIRY = 'se'
    SIGNED_RESOURCE = 'sr'
    SIGNED_IDENTIFIER = 'si'
    SIGNED_IP = 'sip'
    SIGNED_PROTOCOL = 'spr'
    SIGNED_VERSION = 'sv'
    SIGNED_CACHE_CONTROL = 'rscc'
    SIGNED_CONTENT_DISPOSITION = 'rscd'
    SIGNED_CONTENT_ENCODING = 'rsce'
    SIGNED_CONTENT_LANGUAGE = 'rscl'
    SIGNED_CONTENT_TYPE = 'rsct'
    TABLE_NAME = 'tn'
    START_PK = 'spk'
    START_RK = 'srk'
    END_PK = 'epk'
    END_RK = 'erk'


class SharedAccessSignature(object):

    '''
    The main class used to do the signing and generating the signature.

    account_name:
        the storage account name used to generate shared access signature
    account_key:
        the access key to genenerate share access signature
    '''

    def __init__(self, account_name, account_key):
        self.account_name = account_name
        self.account_key = account_key

    def generate_signed_query_string(self, service, path, resource_type,
                                    permission=None,                                   
                                    expiry=None,
                                    start=None, 
                                    id=None,
                                    ip=None, protocol=None,
                                    cache_control=None, content_disposition=None,
                                    content_encoding=None, content_language=None,
                                    content_type=None, table_name=None, 
                                    start_pk=None, start_rk=None, 
                                    end_pk=None, end_rk=None):
        '''
        Generates the query string for path, resource type and shared access
        parameters.

        :param str service:
            The service to generate the sas for.
        :param str path:
            The path to the resource.
        :param str resource_type:
            'b' for blob, 'c' for container, None for queue/table
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
        :param str table_name:
            Name of table.
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
        '''
        query_dict = self._generate_signed_query_dict(
            service,
            path,
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
            table_name,
            start_pk,
            start_rk,
            end_pk,
            end_rk,
        )
        return '&'.join(['{0}={1}'.format(n, url_quote(v)) for n, v in query_dict.items() if v is not None])

    def _generate_signed_query_dict(self, service, path, resource_type,
                                    permission=None, expiry=None, start=None,
                                    id=None, ip=None, protocol=None,
                                    cache_control=None, content_disposition=None,
                                    content_encoding=None, content_language=None,
                                    content_type=None, table_name=None,
                                    start_pk=None, start_rk=None, 
                                    end_pk=None, end_rk=None):
        query_dict = {}

        def add_query(name, val):
            if val:
                query_dict[name] = val

        if isinstance(start, date):
            start = _to_utc_datetime(start)

        if isinstance(expiry, date):
            expiry = _to_utc_datetime(expiry)

        add_query(QueryStringConstants.SIGNED_START, start)
        add_query(QueryStringConstants.SIGNED_EXPIRY, expiry)
        add_query(QueryStringConstants.SIGNED_PERMISSION, permission)
        add_query(QueryStringConstants.SIGNED_IDENTIFIER, id)

        add_query(QueryStringConstants.SIGNED_IP, ip)
        add_query(QueryStringConstants.SIGNED_PROTOCOL, protocol)
        add_query(QueryStringConstants.SIGNED_VERSION, X_MS_VERSION)
        add_query(QueryStringConstants.SIGNED_RESOURCE, resource_type)
        add_query(QueryStringConstants.SIGNED_CACHE_CONTROL, cache_control)
        add_query(QueryStringConstants.SIGNED_CONTENT_DISPOSITION, content_disposition)
        add_query(QueryStringConstants.SIGNED_CONTENT_ENCODING, content_encoding)
        add_query(QueryStringConstants.SIGNED_CONTENT_LANGUAGE, content_language)
        add_query(QueryStringConstants.SIGNED_CONTENT_TYPE, content_type)

        add_query(QueryStringConstants.TABLE_NAME, table_name)
        add_query(QueryStringConstants.START_PK, start_pk)
        add_query(QueryStringConstants.START_RK, start_rk)
        add_query(QueryStringConstants.END_PK, end_pk)
        add_query(QueryStringConstants.END_RK, end_rk)

        query_dict[QueryStringConstants.SIGNED_SIGNATURE] = self._generate_signature(
            service, path, resource_type, permission, expiry, start, id, ip, protocol,
            cache_control, content_disposition, content_encoding, content_language,
            content_type, table_name, start_pk, start_rk, end_pk, end_rk)

        return query_dict

    def _generate_signature(self, service, path, resource_type, permission=None, 
                            expiry=None, start=None, id=None, ip=None, protocol=None,
                            cache_control=None, content_disposition=None,
                            content_encoding=None, content_language=None,
                            content_type=None, table_name=None, start_pk=None, 
                            start_rk=None, end_pk=None, end_rk=None):
        ''' Generates signature for a given path and shared access policy. '''

        def get_value_to_append(value):
            return_value = value or ''
            return return_value + '\n'

        if path[0] != '/':
            path = '/' + path

        canonicalized_resource = '/' + service + '/' + self.account_name + path

        # Form the string to sign from shared_access_policy and canonicalized
        # resource. The order of values is important.
        string_to_sign = \
            (get_value_to_append(permission) +
             get_value_to_append(start) +
             get_value_to_append(expiry) +
             get_value_to_append(canonicalized_resource) +
             get_value_to_append(id) +
             get_value_to_append(ip) +
             get_value_to_append(protocol) +
             get_value_to_append(X_MS_VERSION))

        if resource_type:
            string_to_sign += \
                (get_value_to_append(cache_control) +
                get_value_to_append(content_disposition) +
                get_value_to_append(content_encoding) +
                get_value_to_append(content_language) +
                get_value_to_append(content_type))

        if table_name:
            string_to_sign += \
                (get_value_to_append(start_pk) +
                get_value_to_append(start_rk) +
                get_value_to_append(end_pk) +
                get_value_to_append(end_rk))

        if string_to_sign[-1] == '\n':
            string_to_sign = string_to_sign[:-1]

        return _sign_string(self.account_key, string_to_sign)
