# coding: utf-8

# -------------------------------------------------------------------------
# Copyright (c) Microsoft.  All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this blob except in compliance with the License.
# You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# --------------------------------------------------------------------------
import requests

from azure.storage.blob import BlockBlobService
from azure.storage.common.models import LocationMode
from azure.storage.common.retry import (
    ExponentialRetry,
    LinearRetry,
    no_retry,
)


class ClientSamples():
    def __init__(self):
        pass

    def run_all_samples(self):
        self.retries()
        self.read_from_secondary()
        self.custom_endpoint()
        self.custom_domain()
        self.protocol()
        self.request_session()
        self.proxy()
        self.callbacks()

    def retries(self):
        # By default, retries are performed with an exponential backoff.
        # Any custom retry logic may be used by simply defining a retry function, 
        # but several easy pre-written options are available with modifiable settings.
        client = BlockBlobService(account_name='<account_name>', account_key='<account_key>')

        # Use an exponential retry, but modify the backoff settings
        # Here, we increase the initial back off, increase the number of retry attempts
        # and decrease the base of the exponential backoff.
        client.retry = ExponentialRetry(initial_backoff=30, increment_power=2, max_attempts=5).retry

        # Use a default linear retry policy instead
        client.retry = LinearRetry().retry

        # Turn off retries
        client.retry = no_retry

    def read_from_secondary(self):
        # If you are using RA-GRS accounts, you may want to enable reading from the 
        # secondary endpoint. Note that your application will have to handle this 
        # data potentially being out of date as the secondary may be behind the 
        # primary. 
        client = BlockBlobService(account_name='<account_name>', account_key='<account_key>')

        # The location mode is set to primary by default meaning that all requests 
        # are sent to the primary endpoint. If you'd like to instead read from the 
        # secondary endpoint by default, set location mode to secondary. Note that 
        # writes will continue to go to primary as they are not allowed on secondary.
        client.location_mode = LocationMode.SECONDARY

        # You may also decide you want to retry to secondary. This is useful if 
        # you'd like to automatically handle the primary being temporarily down. 
        # Again, your application will have to handle data being potentially out 
        # of date. Retry to secondary logic may be built into a custom retry policy, 
        # but our retry policies have a flag to enable it. Here we use the same 
        # exponential retry as by default, but allow it to retry to secondary if 
        # the initial request to primary fails.
        client.location_mode = LocationMode.PRIMARY  # Reset the location_mode to start with primary
        client.retry = ExponentialRetry(retry_to_secondary=True).retry

    def custom_endpoint(self):
        # Custom endpoints are necessary for certain regions.
        # The most common usage is to connect to the China cloud.
        client = BlockBlobService(account_name='<account_name>', account_key='<account_key>',
                                  endpoint_suffix='core.chinacloudapi.cn')

    def custom_domain(self):
        # This applies to the blob services only
        # The custom domain must be set on the account through the Portal or Powershell
        client = BlockBlobService(account_name='<account_name>', account_key='<account_key>',
                                  custom_domain='www.mydomain.com')

    def protocol(self):
        # https is the default protocol and is strongly recommended for security 
        # However, http may be used if desired
        client = BlockBlobService(account_name='<account_name>', account_key='<account_key>',
                                  protocol='http')

        # Set later
        client = BlockBlobService(account_name='<account_name>', account_key='<account_key>')
        client.protocol = 'http'

    def request_session(self):
        # A custom request session may be used to set special network options
        session = requests.Session()
        client = BlockBlobService(account_name='<account_name>', account_key='<account_key>',
                                  request_session=session)

        # Set later
        client = BlockBlobService(account_name='<account_name>', account_key='<account_key>')
        client.request_session = session

    def proxy(self):
        # Unauthenticated
        client = BlockBlobService(account_name='<account_name>', account_key='<account_key>')
        client.set_proxy('127.0.0.1', '8888')

        # Authenticated
        client = BlockBlobService(account_name='<account_name>', account_key='<account_key>')
        proxy_user = '1'
        proxy_password = '1'
        client.set_proxy('127.0.0.1', '8888', user=proxy_user, password=proxy_password)

    def callbacks(self):
        # Callbacks may be used read or modify the request and response.
        # The request_callback is called when the request is complete except for
        # adding the authentication and date headers.
        # The response_callback is called when the HTTP response is received before 
        # any parsing is done.

        # Custom client request id
        client = BlockBlobService(account_name='<account_name>', account_key='<account_key>')

        def request_callback(request):
            request.headers['x-ms-client-request-id'] = '<my custom id>'

        client.request_callback = request_callback

        # View data from the response
        def response_callback(response):
            status = response.status
            headers = response.headers

        # Force an exists call to succeed by resetting the status
        client.response_callback = response_callback
