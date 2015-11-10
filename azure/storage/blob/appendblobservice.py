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
import requests
from azure.common import (
    AzureHttpError,
)
from .._common_error import (
    _validate_not_none,
    _validate_type_bytes,
    _ERROR_VALUE_NEGATIVE,
)
from .._common_conversion import (
    _str,
    _str_or_none,
)
from .._common_serialization import (
    _get_request_body_bytes_only,
    _update_request_uri_query_local_storage,
    _parse_response_for_dict,
)
from .._http import HTTPRequest
from ._chunking import (
    _AppendBlobChunkUploader,
    _upload_blob_chunks,
)
from .models import _BlobTypes
from ..constants import (
    BLOB_SERVICE_HOST_BASE,
    DEFAULT_HTTP_TIMEOUT,
    DEV_BLOB_HOST,
    X_MS_VERSION,
)
from azure.common import AzureMissingResourceHttpError
from ._serialization import _update_storage_blob_header
from ._baseblobservice import _BaseBlobService
from os import path
import sys
if sys.version_info >= (3,):
    from io import BytesIO
else:
    from cStringIO import StringIO as BytesIO


class AppendBlobService(_BaseBlobService):

    def __init__(self, account_name=None, account_key=None, protocol='https',
                 host_base=BLOB_SERVICE_HOST_BASE, dev_host=DEV_BLOB_HOST,
                 timeout=DEFAULT_HTTP_TIMEOUT, sas_token=None, connection_string=None,
                 request_session=None):
        '''
        account_name:
            your storage account name, required for all operations.
        account_key:
            your storage account key, required for all operations.
        protocol:
            Protocol. Defaults to https.
        host_base:
            Live host base url. Defaults to Azure url. Override this
            for on-premise.
        dev_host:
            Dev host url. Defaults to localhost.
        timeout:
            Timeout for the http request, in seconds.
        sas_token:
            Token to use to authenticate with shared access signature.
        connection_string:
            If specified, the first four parameters (account_name,
            account_key, protocol, host_base) may be overridden
            by values specified in the connection_string. The next three parameters
            (dev_host, timeout, sas_token) cannot be specified with a
            connection_string. See 
            http://azure.microsoft.com/en-us/documentation/articles/storage-configure-connection-string/
            for the connection string format.
        request_session:
            Session object to use for http requests.
        '''
        self.blob_type = _BlobTypes.AppendBlob
        super(AppendBlobService, self).__init__(
            account_name, account_key, protocol, host_base, dev_host,
            timeout, sas_token, connection_string, request_session)

    def create_blob(self, container_name, blob_name, settings=None,
                 metadata=None, lease_id=None,
                 if_modified_since=None, if_unmodified_since=None,
                 if_match=None, if_none_match=None):
        '''
        Creates a blob or overrides an existing blob. Use if_match=* to
        prevent overriding an existing blob. 

        See create_blob_from_* for high level
        functions that handle the creation and upload of large blobs with
        automatic chunking and progress notifications.

        container_name:
            Name of existing container.
        blob_name:
            Name of blob to create or update.
        settings:
            Settings object used to set blob properties.
        metadata:
            A dict containing name, value for metadata.
        lease_id:
            Required if the blob has an active lease.
        if_modified_since:
            Datetime string.
        if_unmodified_since:
            DateTime string.
        if_match:
            An ETag value.
        if_none_match:
            An ETag value.
        '''
        _validate_not_none('container_name', container_name)
        _validate_not_none('blob_name', blob_name)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = '/' + _str(container_name) + '/' + _str(blob_name)
        request.headers = [
            ('x-ms-blob-type', _str_or_none(self.blob_type)),
            ('x-ms-meta-name-values', metadata),
            ('x-ms-lease-id', _str_or_none(lease_id)),
            ('If-Modified-Since', _str_or_none(if_modified_since)),
            ('If-Unmodified-Since', _str_or_none(if_unmodified_since)),
            ('If-Match', _str_or_none(if_match)),
            ('If-None-Match', _str_or_none(if_none_match))
        ]
        if settings is not None:
            request.headers += settings.to_headers()
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_blob_header(
            request, self.authentication)
        self._perform_request(request)

    def append_block(self, container_name, blob_name, block,
                     content_md5=None, maxsize_condition=None,
                     appendpos_condition=None,
                     lease_id=None, if_modified_since=None,
                     if_unmodified_since=None, if_match=None,
                     if_none_match=None):
        '''
        The Append Block operation commits a new block of data
        to the end of an existing append blob.
        
        container_name:
            Name of existing container.
        blob_name:
            Name of existing blob.
        block:
            Content of the block in bytes.
        content_md5:
            An MD5 hash of the block content. This hash is used to
            verify the integrity of the blob during transport. When this
            header is specified, the storage service checks the hash that has
            arrived with the one that was sent.
        maxsize_condition:
            Optional conditional header. The max length in bytes permitted for
            the append blob. If the Append Block operation would cause the blob
            to exceed that limit or if the blob size is already greater than the
            value specified in this header, the request will fail with
            MaxBlobSizeConditionNotMet error (HTTP status code 412 – Precondition Failed).
        appendpos_condition:
            Optional conditional header, used only for the Append Block operation.
            A number indicating the byte offset to compare. Append Block will
            succeed only if the append position is equal to this number. If it
            is not, the request will fail with the
            AppendPositionConditionNotMet error
            (HTTP status code 412 – Precondition Failed).
        lease_id:
            Required if the blob has an active lease.
        if_modified_since:
            Datetime string.
        if_unmodified_since:
            DateTime string.
        if_match:
            An ETag value.
        if_none_match:
            An ETag value.
        '''
        _validate_not_none('container_name', container_name)
        _validate_not_none('blob_name', blob_name)
        _validate_not_none('block', block)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = '/' + \
            _str(container_name) + '/' + _str(blob_name) + '?comp=appendblock'
        request.headers = [
            ('Content-MD5', _str_or_none(content_md5)),
            ('x-ms-blob-condition-maxsize', _str_or_none(maxsize_condition)),
            ('x-ms-blob-condition-appendpos', _str_or_none(appendpos_condition)),
            ('x-ms-lease-id', _str_or_none(lease_id)),
            ('If-Modified-Since', _str_or_none(if_modified_since)),
            ('If-Unmodified-Since', _str_or_none(if_unmodified_since)),
            ('If-Match', _str_or_none(if_match)),
            ('If-None-Match', _str_or_none(if_none_match))
        ]
        request.body = _get_request_body_bytes_only('block', block)
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_blob_header(
            request, self.authentication)
        response = self._perform_request(request)

        return _parse_response_for_dict(response)

    #----Convenience APIs----------------------------------------------

    def append_blob_from_path(
        self, container_name, blob_name, file_path,
        settings=None, metadata=None, maxsize_condition=None,
        progress_callback=None, max_retries=5, retry_wait=1.0,
        lease_id=None, if_modified_since=None, if_unmodified_since=None,
        if_match=None, if_none_match=None, create_if_not_exist=True):
        '''
        Appends to the content of an existing blob from a file path, with automatic
        chunking and progress notifications. If blob doesn't exist, a new blob will
        be created if create_if_not_exist, otherwise operation will fail.

        container_name:
            Name of existing container.
        blob_name:
            Name of blob to create or update.
        file_path:
            Path of the file to upload as the blob content.
        settings:
            Settings object used to set blob properties.
        metadata:
            A dict containing name, value pairs for setting blob metadata, if blob
            doesn't exist and create_if_not_exists is true.
        maxsize_condition:
            Optional conditional header. The max length in bytes permitted for
            the append blob. If the Append Block operation would cause the blob
            to exceed that limit or if the blob size is already greater than the
            value specified in this header, the request will fail with
            MaxBlobSizeConditionNotMet error (HTTP status code 412 – Precondition Failed).
        progress_callback:
            Callback for progress with signature function(current, total) where
            current is the number of bytes transfered so far, and total is the
            size of the blob, or None if the total size is unknown.
        max_retries:
            Number of times to retry upload of blob chunk if an error occurs.
        retry_wait:
            Sleep time in secs between retries.
        lease_id:
            Required if the blob has an active lease.
        if_modified_since:
            Datetime string, if blob doesn't exist and
            create_if_not_exists is true.
        if_unmodified_since:
            DateTime string, if blob doesn't exist and
            create_if_not_exists is true.
        if_match:
            An ETag value, if blob doesn't exist and
            create_if_not_exists is true.
        if_none_match:
            An ETag value, if blob doesn't exist and
            create_if_not_exists is true.
        create_if_not_exist:
            Indicates if a blob should be created for the append block operation if
            the blob doesn't already exist.
        '''
        _validate_not_none('container_name', container_name)
        _validate_not_none('blob_name', blob_name)
        _validate_not_none('file_path', file_path)

        count = path.getsize(file_path)
        with open(file_path, 'rb') as stream:
            self.append_blob_from_stream(
                container_name,
                blob_name,
                stream,
                count=count,
                settings=settings,
                metadata=metadata,
                maxsize_condition=maxsize_condition,
                progress_callback=progress_callback,
                max_retries=max_retries,
                retry_wait=retry_wait,
                lease_id=lease_id,
                if_modified_since=if_modified_since,
                if_unmodified_since=if_unmodified_since,
                if_match=if_match,
                if_none_match=if_none_match,
                create_if_not_exist=create_if_not_exist)

    def append_blob_from_bytes(
        self, container_name, blob_name, blob, index=0, count=None,
        settings=None, metadata=None, maxsize_condition=None, 
        progress_callback=None, max_retries=5, retry_wait=1.0,
        lease_id=None, if_modified_since=None, if_unmodified_since=None,
        if_match=None, if_none_match=None, create_if_not_exist=True):
        '''
        Appends to the content of an existing blob from an array of bytes, with
        automatic chunking and progress notifications. If blob doesn't exist, a new
        blob will be created if create_if_not_exist, otherwise operation will fail.

        container_name:
            Name of existing container.
        blob_name:
            Name of blob to create or update.
        blob:
            Content of blob as an array of bytes.
        index:
            Start index in the array of bytes.
        count:
            Number of bytes to upload. Set to None or negative value to upload
            all bytes starting from index.
        settings:
            Setting object used to set blob properties.
        metadata:
            A dict containing name, value pairs for setting blob metadata, if blob
            doesn't exist and create_if_not_exists is true.
        maxsize_condition:
            Optional conditional header. The max length in bytes permitted for
            the append blob. If the Append Block operation would cause the blob
            to exceed that limit or if the blob size is already greater than the
            value specified in this header, the request will fail with
            MaxBlobSizeConditionNotMet error (HTTP status code 412 – Precondition Failed).
        progress_callback:
            Callback for progress with signature function(current, total) where
            current is the number of bytes transfered so far, and total is the
            size of the blob, or None if the total size is unknown.
        max_retries:
            Number of times to retry upload of blob chunk if an error occurs.
        retry_wait:
            Sleep time in secs between retries.
        lease_id:
            Required if the blob has an active lease.
        if_modified_since:
            Datetime string, if blob doesn't exist and
            create_if_not_exists is true.
        if_unmodified_since:
            DateTime string, if blob doesn't exist and
            create_if_not_exists is true.
        if_match:
            An ETag value, if blob doesn't exist and
            create_if_not_exists is true.
        if_none_match:
            An ETag value, if blob doesn't exist and
            create_if_not_exists is true.
        create_if_not_exist:
            Indicates if a blob should be created for the append block operation if
            the blob doesn't already exist.
        '''
        _validate_not_none('container_name', container_name)
        _validate_not_none('blob_name', blob_name)
        _validate_not_none('blob', blob)
        _validate_not_none('index', index)
        _validate_type_bytes('blob', blob)

        if index < 0:
            raise IndexError(_ERROR_VALUE_NEGATIVE.format('index'))

        if count is None or count < 0:
            count = len(blob) - index

        stream = BytesIO(blob)
        stream.seek(index)

        self.append_blob_from_stream(
            container_name,
            blob_name,
            stream,
            count=count,
            settings=settings,
            metadata=metadata,
            maxsize_condition=maxsize_condition,
            lease_id=lease_id,
            progress_callback=progress_callback,
            max_retries=max_retries,
            retry_wait=retry_wait,
            if_modified_since=if_modified_since,
            if_unmodified_since=if_unmodified_since,
            if_match=if_match,
            if_none_match=if_none_match,
            create_if_not_exist=create_if_not_exist)

    def append_blob_from_text(
        self, container_name, blob_name, text, encoding='utf-8',
        settings=None, metadata=None, maxsize_condition=None, 
        progress_callback=None, max_retries=5, retry_wait=1.0,
        lease_id=None, if_modified_since=None, if_unmodified_since=None,
        if_match=None, if_none_match=None, create_if_not_exist=True):
        '''
        Appends to the content of an existing blob from str/unicode, with
        automatic chunking and progress notifications. If blob doesn't exist, a new
        blob will be created if create_if_not_exist, otherwise operation will fail.

        container_name:
            Name of existing container.
        blob_name:
            Name of blob to create or update.
        text:
            Text to upload to the blob.
        encoding:
            Python encoding to use to convert the text to bytes.
        settings:
            Settings object used to set blob properties.
        metadata:
            A dict containing name, value pairs for setting blob metadata, if blob
            doesn't exist and create_if_not_exists is true.
        maxsize_condition:
            Optional conditional header. The max length in bytes permitted for
            the append blob. If the Append Block operation would cause the blob
            to exceed that limit or if the blob size is already greater than the
            value specified in this header, the request will fail with
            MaxBlobSizeConditionNotMet error (HTTP status code 412 – Precondition Failed).
        progress_callback:
            Callback for progress with signature function(current, total) where
            current is the number of bytes transfered so far, and total is the
            size of the blob, or None if the total size is unknown.
        max_retries:
            Number of times to retry upload of blob chunk if an error occurs.
        retry_wait:
            Sleep time in secs between retries.
        lease_id:
            Required if the blob has an active lease.
        if_modified_since:
            Datetime string, if blob doesn't exist and
            create_if_not_exists is true.
        if_unmodified_since:
            DateTime string, if blob doesn't exist and
            create_if_not_exists is true.
        if_match:
            An ETag value, if blob doesn't exist and
            create_if_not_exists is true.
        if_none_match:
            An ETag value, if blob doesn't exist and
            create_if_not_exists is true.
        create_if_not_exist:
            Indicates if a blob should be created for the append block operation if
            the blob doesn't already exist.
        '''
        _validate_not_none('container_name', container_name)
        _validate_not_none('blob_name', blob_name)
        _validate_not_none('text', text)

        if not isinstance(text, bytes):
            _validate_not_none('encoding', encoding)
            text = text.encode(encoding)

        self.append_blob_from_bytes(
            container_name,
            blob_name,
            text,
            index=0,
            count=len(text),
            settings=settings,
            metadata=metadata,
            maxsize_condition=maxsize_condition,
            lease_id=lease_id,
            progress_callback=progress_callback,
            max_retries=max_retries,
            retry_wait=retry_wait,
            if_modified_since=if_modified_since,
            if_unmodified_since=if_unmodified_since,
            if_match=if_match,
            if_none_match=if_none_match,
            create_if_not_exist=create_if_not_exist)

    def append_blob_from_stream(
        self, container_name, blob_name, stream, count=None,
        settings=None, metadata=None, maxsize_condition=None, 
        progress_callback=None,  max_retries=5, retry_wait=1.0,
        lease_id=None, if_modified_since=None, if_unmodified_since=None,
        if_match=None, if_none_match=None, create_if_not_exist=True):
        '''
        Appends to the content of an existing blob from a file/stream, with
        automatic chunking and progress notifications. If blob doesn't exist, a new
        blob will be created if create_if_not_exist, otherwise operation will fail.

        container_name:
            Name of existing container.
        blob_name:
            Name of blob to create or update.
        stream:
            Opened file/stream to upload as the blob content.
        count:
            Number of bytes to read from the stream. This is optional, but
            should be supplied for optimal performance.
        settings:
            Settings object used to set blob properties.
        metadata:
            A dict containing name, value pairs for setting blob metadata,
            if blob doesn't exist and create_if_not_exists is true.
        maxsize_condition:
            Conditional header. The max length in bytes permitted for
            the append blob. If the Append Block operation would cause the blob
            to exceed that limit or if the blob size is already greater than the
            value specified in this header, the request will fail with
            MaxBlobSizeConditionNotMet error (HTTP status code 412 – Precondition Failed).
        progress_callback:
            Callback for progress with signature function(current, total) where
            current is the number of bytes transfered so far, and total is the
            size of the blob, or None if the total size is unknown.
        max_retries:
            Number of times to retry upload of blob chunk if an error occurs.
        retry_wait:
            Sleep time in secs between retries.
        lease_id:
            Required if the blob has an active lease.
        if_modified_since:
            Datetime string. Used if blob doesn't exist and
            create_if_not_exists is true.
        if_unmodified_since:
            DateTime string. Used if blob doesn't exist and
            create_if_not_exists is true.
        if_match:
            An ETag value. Used if blob doesn't exist and
            create_if_not_exists is true.
        if_none_match:
            An ETag value. Used if blob doesn't exist and
            create_if_not_exists is true.
        create_if_not_exist:
            Indicates if a blob should be created for the append block operation if
            the blob doesn't already exist.
        '''
        _validate_not_none('container_name', container_name)
        _validate_not_none('blob_name', blob_name)
        _validate_not_none('stream', stream)
        _validate_not_none('create_if_not_exist', create_if_not_exist)
        
        if create_if_not_exist:
            try:
                self.get_blob_properties(container_name, blob_name)
            except AzureMissingResourceHttpError:
                self.create_blob(
                    container_name=container_name,
                    blob_name=blob_name,
                    settings=settings,
                    metadata=metadata,
                    lease_id=lease_id,
                    if_modified_since=if_modified_since,
                    if_unmodified_since=if_unmodified_since,
                    if_match=if_match,
                    if_none_match=if_none_match,
                )

        _upload_blob_chunks(
            blob_service=self,
            container_name=container_name,
            blob_name=blob_name,
            blob_size=count,
            block_size=self._BLOB_MAX_CHUNK_DATA_SIZE,
            stream=stream,
            max_connections=1, # upload not easily parallelizable
            max_retries=max_retries,
            retry_wait=retry_wait,
            progress_callback=progress_callback,
            lease_id=lease_id,
            uploader_class=_AppendBlobChunkUploader,
            maxsize_condition=maxsize_condition
        )