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
import sys
from datetime import date
if sys.version_info >= (3,):
    from io import BytesIO
else:
    from cStringIO import StringIO as BytesIO

try:
    from xml.etree import cElementTree as ETree
except ImportError:
    from xml.etree import ElementTree as ETree

from ._common_error import (
    _general_error_handler,
)
from .constants import (
    X_MS_VERSION,
)
from ._common_serialization import (
    _to_utc_datetime,
)

def _storage_error_handler(http_error):
    ''' Simple error handler for storage service. '''
    return _general_error_handler(http_error)


def _convert_signed_identifiers_to_xml(signed_identifiers):
    if signed_identifiers is None:
        return ''

    sis = ETree.Element('SignedIdentifiers');
    for id, access_policy in signed_identifiers.items():
        # Root signed identifers element
        si = ETree.SubElement(sis, 'SignedIdentifier')

        # Id element
        ETree.SubElement(si, 'Id').text = id

        # Access policy element
        policy = ETree.SubElement(si, 'AccessPolicy')

        if access_policy.start:
            start = access_policy.start
            if isinstance(access_policy.start, date):
                start = _to_utc_datetime(start)
            ETree.SubElement(policy, 'Start').text = start

        if access_policy.expiry:
            expiry = access_policy.expiry
            if isinstance(access_policy.expiry, date):
                expiry = _to_utc_datetime(expiry)
            ETree.SubElement(policy, 'Expiry').text = expiry
        
        if access_policy.permission:
            ETree.SubElement(policy, 'Permission').text = access_policy.permission

    # Add xml declaration and serialize
    with BytesIO() as stream:
        ETree.ElementTree(sis).write(stream, xml_declaration=True, encoding='utf-8', method='xml')
        output = stream.getvalue()
    
    return output

def _convert_service_properties_to_xml(logging, hour_metrics, minute_metrics, cors, target_version=None):
    '''
    <?xml version="1.0" encoding="utf-8"?>
    <StorageServiceProperties>
        <Logging>
            <Version>version-number</Version>
            <Delete>true|false</Delete>
            <Read>true|false</Read>
            <Write>true|false</Write>
            <RetentionPolicy>
                <Enabled>true|false</Enabled>
                <Days>number-of-days</Days>
            </RetentionPolicy>
        </Logging>
        <HourMetrics>
            <Version>version-number</Version>
            <Enabled>true|false</Enabled>
            <IncludeAPIs>true|false</IncludeAPIs>
            <RetentionPolicy>
                <Enabled>true|false</Enabled>
                <Days>number-of-days</Days>
            </RetentionPolicy>
        </HourMetrics>
        <MinuteMetrics>
            <Version>version-number</Version>
            <Enabled>true|false</Enabled>
            <IncludeAPIs>true|false</IncludeAPIs>
            <RetentionPolicy>
                <Enabled>true|false</Enabled>
                <Days>number-of-days</Days>
            </RetentionPolicy>
        </MinuteMetrics>
        <Cors>
            <CorsRule>
                <AllowedOrigins>comma-separated-list-of-allowed-origins</AllowedOrigins>
                <AllowedMethods>comma-separated-list-of-HTTP-verb</AllowedMethods>
                <MaxAgeInSeconds>max-caching-age-in-seconds</MaxAgeInSeconds>
                <ExposedHeaders>comma-seperated-list-of-response-headers</ExposedHeaders>
                <AllowedHeaders>comma-seperated-list-of-request-headers</AllowedHeaders>
            </CorsRule>
        </Cors>
    </StorageServiceProperties>
    '''
    service_properties_element = ETree.Element('StorageServiceProperties');

    # Logging
    if logging:
        logging_element = ETree.SubElement(service_properties_element, 'Logging')
        ETree.SubElement(logging_element, 'Version').text = logging.version
        ETree.SubElement(logging_element, 'Delete').text = str(logging.delete)
        ETree.SubElement(logging_element, 'Read').text = str(logging.read)
        ETree.SubElement(logging_element, 'Write').text = str(logging.write)

        retention_element = ETree.SubElement(logging_element, 'RetentionPolicy')
        _convert_retention_policy_to_xml(logging.retention_policy, retention_element)

    # HourMetrics
    if hour_metrics:
        hour_metrics_element = ETree.SubElement(service_properties_element, 'HourMetrics')
        _convert_metrics_to_xml(hour_metrics, hour_metrics_element)

    # MinuteMetrics
    if minute_metrics:
        minute_metrics_element = ETree.SubElement(service_properties_element, 'MinuteMetrics')
        _convert_metrics_to_xml(minute_metrics, minute_metrics_element)

    # CORS
    # Make sure to still serialize empty list
    if cors is not None:
        cors_element = ETree.SubElement(service_properties_element, 'Cors')
        for rule in cors:
            cors_rule = ETree.SubElement(cors_element, 'CorsRule')
            ETree.SubElement(cors_rule, 'AllowedOrigins').text = ",".join(rule.allowed_origins)
            ETree.SubElement(cors_rule, 'AllowedMethods').text = ",".join(rule.allowed_methods)
            ETree.SubElement(cors_rule, 'MaxAgeInSeconds').text = str(rule.max_age_in_seconds)
            ETree.SubElement(cors_rule, 'ExposedHeaders').text = ",".join(rule.exposed_headers)
            ETree.SubElement(cors_rule, 'AllowedHeaders').text = ",".join(rule.allowed_headers)

    # Target version
    if target_version:
        ETree.SubElement(service_properties_element, 'DefaultServiceVersion').text = target_version


    # Add xml declaration and serialize
    with BytesIO() as stream:
        ETree.ElementTree(service_properties_element).write(stream, xml_declaration=True, encoding='utf-8', method='xml')
        output = stream.getvalue()
    
    return output

def _convert_metrics_to_xml(metrics, root):
    '''
    <Version>version-number</Version>
    <Enabled>true|false</Enabled>
    <IncludeAPIs>true|false</IncludeAPIs>
    <RetentionPolicy>
        <Enabled>true|false</Enabled>
        <Days>number-of-days</Days>
    </RetentionPolicy>
    '''
    # Version
    ETree.SubElement(root, 'Version').text = metrics.version

    # Enabled
    ETree.SubElement(root, 'Enabled').text = str(metrics.enabled)

    # IncludeAPIs
    if metrics.include_apis:
        ETree.SubElement(root, 'IncludeAPIs').text = str(metrics.include_apis)

    # RetentionPolicy
    retention_element = ETree.SubElement(root, 'RetentionPolicy')
    _convert_retention_policy_to_xml(metrics.retention_policy, retention_element)

def _convert_retention_policy_to_xml(retention_policy, root):
    '''
    <Enabled>true|false</Enabled>
    <Days>number-of-days</Days>
    '''
    # Enabled
    ETree.SubElement(root, 'Enabled').text = str(retention_policy.enabled)

    # Days
    if retention_policy.enabled and retention_policy.days:
        ETree.SubElement(root, 'Days').text = str(retention_policy.days)


def _update_storage_header(request):
    ''' add additional headers for storage request. '''
    if request.body:
        assert isinstance(request.body, bytes)

    # if it is PUT, POST, MERGE, DELETE, need to add content-length to header.
    if request.method in ['PUT', 'POST', 'MERGE', 'DELETE']:
        request.headers.append(('Content-Length', str(len(request.body))))

    # append addtional headers based on the service
    request.headers.append(('x-ms-version', X_MS_VERSION))
    request.headers.append(('Accept-Charset', 'UTF-8'))
    request.headers.append(('Accept-Encoding', 'identity'))

    # append x-ms-meta name, values to header
    for name, value in request.headers:
        if 'x-ms-meta-name-values' in name and value:
            for meta_name, meta_value in value.items():
                request.headers.append(('x-ms-meta-' + meta_name, meta_value))
            request.headers.remove((name, value))
            break
    return request
