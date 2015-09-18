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
import os
import sys

import requests

from .constants import (
    AZURE_STORAGE_ACCOUNT,
    AZURE_STORAGE_ACCESS_KEY,
    DEFAULT_HTTP_TIMEOUT,
    DEV_ACCOUNT_NAME,
    DEV_ACCOUNT_KEY,
    EMULATED,
    _USER_AGENT_STRING,
)
from ._http import HTTPError
from ._http.httpclient import _HTTPClient
from ._serialization import _storage_error_handler
from ._common_error import (
    _ERROR_STORAGE_MISSING_INFO,
)


class _StorageClient(object):

    '''
    This is the base class for BlobManager, TableManager and QueueManager.
    '''

    def __init__(self, account_name=None, account_key=None, protocol='https',
                 host_base='', dev_host='', timeout=DEFAULT_HTTP_TIMEOUT,
                 sas_token=None, request_session=None):
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
        request_session:
            Optional. Session object to use for http requests.
        '''
        self.account_name = account_name
        self.account_key = account_key
        self.requestid = None
        self.protocol = protocol.lower()
        self.host_base = host_base
        self.dev_host = dev_host
        self.sas_token = sas_token

        # the app is not run in azure emulator or use default development
        # storage account and key if app is run in emulator.
        self.use_local_storage = False

        # check whether it is run in emulator.
        if EMULATED in os.environ:
            self.is_emulated = os.environ[EMULATED].lower() != 'false'
        else:
            self.is_emulated = False

        # get account_name and account key. If they are not set when
        # constructing, get the account and key from environment variables if
        # the app is not run in azure emulator or use default development
        # storage account and key if app is run in emulator.
        if not self.account_name and not self.account_key:
            if self.is_emulated:
                self.account_name = DEV_ACCOUNT_NAME
                self.account_key = DEV_ACCOUNT_KEY
                self.protocol = 'http'
                self.use_local_storage = True
            else:
                self.account_name = os.environ.get(AZURE_STORAGE_ACCOUNT)
                self.account_key = os.environ.get(AZURE_STORAGE_ACCESS_KEY)

        if not self.account_name:
            raise ValueError(_ERROR_STORAGE_MISSING_INFO)

        self._httpclient = _HTTPClient(
            service_instance=self,
            protocol=self.protocol,
            timeout=timeout,
            request_session=request_session or requests.Session(),
            user_agent=_USER_AGENT_STRING,
        )
        self._batchclient = None
        self._filter = self._perform_request_worker

    def with_filter(self, filter):
        '''
        Returns a new service which will process requests with the specified
        filter.  Filtering operations can include logging, automatic retrying,
        etc...  The filter is a lambda which receives the HTTPRequest and
        another lambda.  The filter can perform any pre-processing on the
        request, pass it off to the next lambda, and then perform any
        post-processing on the response.
        '''
        res = type(self)(self.account_name, self.account_key, self.protocol,
                         self.host_base, self.dev_host,
                         self._httpclient.timeout)
        old_filter = self._filter

        def new_filter(request):
            return filter(request, old_filter)

        res._filter = new_filter
        return res

    def set_proxy(self, host, port, user=None, password=None):
        '''
        Sets the proxy server host and port for the HTTP CONNECT Tunnelling.

        host:
            Address of the proxy. Ex: '192.168.0.100'
        port:
            Port of the proxy. Ex: 6000
        user:
            User for proxy authorization.
        password:
            Password for proxy authorization.
        '''
        self._httpclient.set_proxy(host, port, user, password)

    @property
    def timeout(self):
        return self._httpclient.timeout

    @timeout.setter
    def timeout(self, value):
        self._httpclient.timeout = value

    def _get_host(self):
        if self.use_local_storage:
            return self.dev_host
        else:
            return self.account_name + self.host_base

    def _perform_request_worker(self, request):
        return self._httpclient.perform_request(request)

    def _perform_request(self, request, text_encoding='utf-8'):
        '''
        Sends the request and return response. Catches HTTPError and hand it
        to error handler
        '''
        try:
            if self._batchclient is not None:
                return self._batchclient.insert_request_to_batch(request)
            else:
                resp = self._filter(request)

            if sys.version_info >= (3,) and isinstance(resp, bytes) and \
                text_encoding:
                resp = resp.decode(text_encoding)

        except HTTPError as ex:
            _storage_error_handler(ex)

        return resp
