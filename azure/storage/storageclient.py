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
import os
import sys
import copy
import requests

from abc import ABCMeta
from azure.common import (
    AzureException,
)
from ._constants import (
    _USER_AGENT_STRING,
    _SOCKET_TIMEOUT
)
from ._http import HTTPError
from ._http.httpclient import _HTTPClient
from ._serialization import (
    _storage_error_handler,
    _update_request,
    _add_date_header,
)
from ._error import (
    _ERROR_STORAGE_MISSING_INFO,
)

class StorageClient(object):

    '''
    This is the base class for service objects. Service objects are used to do 
    all requests to Storage. This class cannot be instantiated directly.

    :ivar str account_name:
        The storage account name. This is used to authenticate requests 
        signed with an account key and to construct the storage endpoint. It 
        is required unless a connection string is given, or if a custom 
        domain is used with anonymous authentication.
    :ivar str account_key:
        The storage account key. This is used for shared key authentication. 
        If neither account key or sas token is specified, anonymous access 
        will be used.
    :ivar str sas_token:
        A shared access signature token to use to authenticate requests 
        instead of the account key. If account key and sas token are both 
        specified, account key will be used to sign. If neither are 
        specified, anonymous access will be used.
    :ivar str primary_endpoint:
        The endpoint to send storage requests to.
    :ivar str secondary_endpoint:
        The secondary endpoint to read storage data from. This will only be a 
        valid endpoint if the storage account used is RA-GRS and thus allows 
        reading from secondary.
    :ivar function(request) request_callback:
        A function called immediately before each request is sent. This function 
        takes as a parameter the request object and returns nothing. It may be 
        used to added custom headers or log request data.
    :ivar function() response_callback:
        A function called immediately after each response is received. This 
        function takes as a parameter the response object and returns nothing. 
        It may be used to log response data.
    :param str protocol:
        The protocol to use for requests. Defaults to https.
    :param requests.Session request_session:
        The session object to use for http requests.
    '''

    __metaclass__ = ABCMeta

    def __init__(self, connection_params):
        '''
        :param obj connection_params: The parameters to use to construct the client.
        '''
        self.account_name = connection_params.account_name
        self.account_key = connection_params.account_key
        self.sas_token = connection_params.sas_token

        self.primary_endpoint = connection_params.primary_endpoint
        self.secondary_endpoint = connection_params.secondary_endpoint

        protocol = connection_params.protocol
        request_session = connection_params.request_session or requests.Session()
        self._httpclient = _HTTPClient(
            protocol=protocol,
            session=request_session,
            timeout=_SOCKET_TIMEOUT,
        )

        self._filter = self._perform_request_worker

        self.request_callback = None
        self.response_callback = None

    @property
    def protocol(self):
        return self._httpclient.protocol

    @protocol.setter
    def protocol(self, value):
        self._httpclient.protocol = value

    @property
    def request_session(self):
        return self._httpclient.session

    @request_session.setter
    def request_session(self, value):
        self._httpclient.session = value

    def set_proxy(self, host, port, user=None, password=None):
        '''
        Sets the proxy server host and port for the HTTP CONNECT Tunnelling.

        :param str host: Address of the proxy. Ex: '192.168.0.100'
        :param int port: Port of the proxy. Ex: 6000
        :param str user: User for proxy authorization.
        :param str password: Password for proxy authorization.
        '''
        self._httpclient.set_proxy(host, port, user, password)

    def _get_host(self):
        return self.primary_endpoint

    def _perform_request_worker(self, request):
        _update_request(request)

        if self.request_callback:
            self.request_callback(request)

        # Add date and auth after the callback so date doesn't get too old and 
        # authentication is still correct if signed headers are added in the request 
        # callback
        _add_date_header(request)
        self.authentication.sign_request(request)
        return self._httpclient.perform_request(request)

    def _perform_request(self, request, encoding='utf-8'):
        '''
        Sends the request and return response. Catches HTTPError and hands it
        to error handler
        '''
        try:
            response = self._filter(request)
        except Exception as ex:
            if sys.version_info >= (3,):
                # Automatic chaining in Python 3 means we keep the trace
                raise AzureException
            else:
                # There isn't a good solution in 2 for keeping the stack trace 
                # in general, or that will not result in an error in 3
                # However, we can keep the previous error type and message
                # TODO: In the future we will log the trace
                raise AzureException('{}: {}'.format(ex.__class__.__name__, ex.args[0]))

        if self.response_callback:
            self.response_callback(response)

        # Parse and wrap HTTP errors in AzureHttpError which inherits from AzureException
        if response.status >= 300:
            # This exception will be caught by the general error handler
            # and raised as an azure http exception
            _storage_error_handler(HTTPError(response.status, response.message, response.headers, response.body))

        return response
