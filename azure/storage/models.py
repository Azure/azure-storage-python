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
    AzureException,
    AzureHttpError,
)
from ._common_models import (
    WindowsAzureData,
    _list_of,
)
from ._common_error import (
    _validate_not_none,
)


class _list(list):
    '''Used so that a continuation token can be set on the return object'''
    pass


class AzureBatchValidationError(AzureException):

    '''Indicates that a batch operation cannot proceed due to invalid input'''


class AzureBatchOperationError(AzureHttpError):

    '''Indicates that a batch operation failed'''

    def __init__(self, message, status_code, batch_code):
        super(AzureBatchOperationError, self).__init__(message, status_code)
        self.code = batch_code


class EnumResultsBase(object):

    ''' base class for EnumResults. '''

    def __init__(self):
        self.prefix = u''
        self.marker = u''
        self.max_results = 0
        self.next_marker = u''


class RetentionPolicy(object):

    '''
    RetentionPolicy class to be used with ServiceProperties.
    
    :param bool enabled: 
        Indicates whether a retention policy is enabled for the 
        storage service. If disabled, logging and metrics data will be retained 
        infinitely by the service unless explicitly deleted.
    :param int days: 
        Required if enabled is true. Indicates the number of 
        days that metrics or logging data should be retained. All data older 
        than this value will be deleted. The minimum value you can specify is 1; 
        the largest value is 365 (one year).
    '''

    def __init__(self, enabled=False, days=None):
        _validate_not_none("enabled", enabled)
        if enabled:
            _validate_not_none("days", days)

        self.enabled = enabled
        self.days = days


class Logging(object):

    '''
    Logging class to be used with ServiceProperties.

    :param bool delete: 
        Indicates whether all delete requests should be logged.
    :param bool read: 
        Indicates whether all read requests should be logged.
    :param bool write: 
        Indicates whether all write requests should be logged.
    :param RetentionPolicy retention_policy: 
        The retention policy for the metrics.
    '''

    def __init__(self, delete=False, read=False, write=False,
                 retention_policy=None):
        _validate_not_none("read", read)
        _validate_not_none("write", write)
        _validate_not_none("delete", delete)

        self.version = u'1.0'
        self.delete = delete
        self.read = read
        self.write = write
        self.retention_policy = retention_policy if retention_policy else RetentionPolicy()


class Metrics(object):

    ''' 
    Metrics class to be used with ServiceProperties.

    :param bool enabled: 
        Indicates whether metrics are enabled for 
        the service.
    :param bool include_apis: 
        Required if enabled is True. Indicates whether metrics 
        should generate summary statistics for called API operations.
    :param RetentionPolicy retention_policy: 
        The retention policy for the metrics.
    '''

    def __init__(self, enabled=False, include_apis=None,
                 retention_policy=None):
        _validate_not_none("enabled", enabled)
        if enabled:
            _validate_not_none("include_apis", include_apis)

        self.version = u'1.0'
        self.enabled = enabled
        self.include_apis = include_apis
        self.retention_policy = retention_policy if retention_policy else RetentionPolicy()


class CorsRule(object):

    '''
    Cors Rule class to be used with ServiceProperties.
    
    :param allowed_origins: 
        A list of origin domains that will be allowed via CORS, or "*" to allow 
        all domains. The list of must contain at least one entry. Limited to 64 
        origin domains. Each allowed origin can have up to 256 characters.
    :type allowed_origins: list of str
    :param allowed_methods:
        A list of HTTP methods that are allowed to be executed by the origin. 
        The list of must contain at least one entry. For Azure Storage, 
        permitted methods are DELETE, GET, HEAD, MERGE, POST, OPTIONS or PUT.
    :type allowed_methods: list of str
    :param int max_age_in_seconds:
        The number of seconds that the client/browser should cache a 
        preflight response.
    :param exposed_headers:
        Defaults to an empty list. A list of response headers to expose to CORS 
        clients. Limited to 64 defined headers and two prefixed headers. Each 
        header can be up to 256 characters.
    :type exposed_headers: list of str
    :param allowed_headers:
        Defaults to an empty list. A list of headers allowed to be part of 
        the cross-origin request. Limited to 64 defined headers and 2 prefixed 
        headers. Each header can be up to 256 characters.
    :type allowed_headers: list of str
    '''

    def __init__(self, allowed_origins, allowed_methods, max_age_in_seconds=0,
                 exposed_headers=None, allowed_headers=None):
        _validate_not_none("allowed_origins", allowed_origins)
        _validate_not_none("allowed_methods", allowed_methods)
        _validate_not_none("max_age_in_seconds", max_age_in_seconds)

        self.allowed_origins = allowed_origins if allowed_origins else list()
        self.allowed_methods = allowed_methods if allowed_methods else list()
        self.max_age_in_seconds = max_age_in_seconds
        self.exposed_headers = exposed_headers if exposed_headers else list()
        self.allowed_headers = allowed_headers if allowed_headers else list()


class ServiceProperties(object):
    ''' Only for IntelliSense and telling user the return type. '''

    pass


class AccessPolicy(WindowsAzureData):

    ''' Access Policy class in service properties. '''

    def __init__(self, start=u'', expiry=u'', permission=u'',
                 start_pk=u'', start_rk=u'', end_pk=u'', end_rk=u''):
        self.start = start
        self.expiry = expiry
        self.permission = permission
        self.start_pk = start_pk
        self.start_rk = start_rk
        self.end_pk = end_pk
        self.end_rk = end_rk


class SignedIdentifier(WindowsAzureData):

    ''' Signed Identifier class for service properties. '''

    def __init__(self):
        self.id = u''
        self.access_policy = AccessPolicy()


class SignedIdentifiers(WindowsAzureData):

    ''' SignedIdentifier list. '''

    def __init__(self):
        self.signed_identifiers = _list_of(SignedIdentifier)

    def __iter__(self):
        return iter(self.signed_identifiers)

    def __len__(self):
        return len(self.signed_identifiers)

    def __getitem__(self, index):
        return self.signed_identifiers[index]
