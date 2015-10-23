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
from azure.common import AzureHttpError
from .._common_error import (
    _validate_not_none,
    _validate_type_bytes,
    _ERROR_VALUE_NEGATIVE,
    _ERROR_PAGE_BLOB_SIZE_ALIGNMENT,
)
from .._common_conversion import (
    _int_or_none,
    _str,
    _str_or_none,
)
from .._common_serialization import (
    _get_request_body_bytes_only,
    _update_request_uri_query_local_storage,
    _ETreeXmlToObject,
)
from .._http import HTTPRequest
from ._chunking import (
    _PageBlobChunkUploader,
    _upload_blob_chunks,
)
from .models import (
    PageList,
    PageRange,
    _BlobTypes,
)
from ..constants import (
    BLOB_SERVICE_HOST_BASE,
    DEFAULT_HTTP_TIMEOUT,
    DEV_BLOB_HOST,
    X_MS_VERSION,
)
from ._serialization import _update_storage_blob_header
from ._baseblobservice import _BaseBlobService
from os import path
import sys
if sys.version_info >= (3,):
    from io import BytesIO
else:
    from cStringIO import StringIO as BytesIO

# Keep this value sync with _ERROR_PAGE_BLOB_SIZE_ALIGNMENT
_PAGE_SIZE = 512


class PageBlobService(_BaseBlobService):
    
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
        self.blob_type = _BlobTypes.PageBlob
        super(PageBlobService, self).__init__(
            account_name, account_key, protocol, host_base, dev_host,
            timeout, sas_token, connection_string, request_session)

    def create_blob(
        self, container_name, blob_name, content_length, content_encoding=None,
        content_language=None, cache_control=None, content_type=None,
        content_md5=None, metadata=None, lease_id=None,
        sequence_number=None, if_modified_since=None, if_unmodified_since=None,
        if_match=None, if_none_match=None):
        '''
        Creates a new page blob.

        See create_blob_from_* for high level functions that handle the
        creation and upload of large blobs with automatic chunking and
        progress notifications.

        container_name:
            Name of existing container.
        blob_name:
            Name of blob to create or update.
        content_length:
            Required. This header specifies the maximum size
            for the page blob, up to 1 TB. The page blob size must be aligned
            to a 512-byte boundary.
        content_encoding:
            Specifies which content encodings have been applied to
            the blob. This value is returned to the client when the Get Blob
            (REST API) operation is performed on the blob resource. The client
            can use this value when returned to decode the blob content.
        content_language:
            Specifies the natural languages used by this resource.
        cache_control:
            The Blob service stores this value but does not use or
            modify it.
        content_type:
            Set the blob's content type.
        content_md5:
            Set the blob's MD5 hash.
        metadata:
            A dict containing name, value for metadata.
        lease_id:
            Required if the blob has an active lease.
        sequence_number:
            The sequence number is a user-controlled value that you
            can use to track requests. The value of the sequence number must
            be between 0 and 2^63 - 1. The default value is 0.
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
            ('x-ms-blob-content-type', _str_or_none(content_type)),
            ('x-ms-blob-content-encoding',
                _str_or_none(content_encoding)),
            ('x-ms-blob-content-language',
                _str_or_none(content_language)),
            ('x-ms-blob-content-md5', _str_or_none(content_md5)),
            ('x-ms-blob-cache-control', _str_or_none(cache_control)),
            ('x-ms-meta-name-values', metadata),
            ('x-ms-lease-id', _str_or_none(lease_id)),
            ('x-ms-blob-content-length',
                _str_or_none(content_length)),
            ('x-ms-blob-sequence-number',
                _str_or_none(sequence_number)),
            ('If-Modified-Since', _str_or_none(if_modified_since)),
            ('If-Unmodified-Since', _str_or_none(if_unmodified_since)),
            ('If-Match', _str_or_none(if_match)),
            ('If-None-Match', _str_or_none(if_none_match))
        ]
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_blob_header(
            request, self.authentication)
        self._perform_request(request)

    def put_page(
        self, container_name, blob_name, page, range,
        page_write, timeout=None, content_md5=None,
        lease_id=None, if_sequence_number_lte=None,
        if_sequence_number_lt=None, if_sequence_number_eq=None,
        if_modified_since=None, if_unmodified_since=None,
        if_match=None, if_none_match=None):
        '''
        Writes a range of pages to a page blob.

        container_name:
            Name of existing container.
        blob_name:
            Name of existing blob.
        page:
            Content of the page.
        range:
            Required. Specifies the range of bytes to be written as a page.
            Both the start and end of the range must be specified. Must be in
            format:
                bytes=startByte-endByte. Given that pages must be aligned
            with 512-byte boundaries, the start offset must be a modulus of
            512 and the end offset must be a modulus of 512-1. Examples of
            valid byte ranges are 0-511, 512-1023, etc.
        page_write:
            Required. You may specify one of the following options:
                update (lower case):
                    Writes the bytes specified by the request body into the
                    specified range. The Range and Content-Length headers must
                    match to perform the update.
                clear (lower case):
                    Clears the specified range and releases the space used in
                    storage for that range. To clear a range, set the
                    Content-Length header to zero, and the Range header to a
                    value that indicates the range to clear, up to maximum
                    blob size.
        timeout:
            the timeout parameter is expressed in seconds.
        content_md5:
            An MD5 hash of the page content. This hash is used to
            verify the integrity of the page during transport. When this header
            is specified, the storage service compares the hash of the content
            that has arrived with the header value that was sent. If the two
            hashes do not match, the operation will fail with error code 400
            (Bad Request).
        lease_id:
            Required if the blob has an active lease.
        if_sequence_number_lte:
            If the blob's sequence number is less than or equal to
            the specified value, the request proceeds; otherwise it fails.
        if_sequence_number_lt:
            If the blob's sequence number is less than the specified
            value, the request proceeds; otherwise it fails.
        if_sequence_number_eq:
            If the blob's sequence number is equal to the specified
            value, the request proceeds; otherwise it fails.
        if_modified_since:
            A DateTime value. Specify this conditional header to
            write the page only if the blob has been modified since the
            specified date/time. If the blob has not been modified, the Blob
            service fails.
        if_unmodified_since:
            A DateTime value. Specify this conditional header to
            write the page only if the blob has not been modified since the
            specified date/time. If the blob has been modified, the Blob
            service fails.
        if_match:
            An ETag value. Specify an ETag value for this conditional
            header to write the page only if the blob's ETag value matches the
            value specified. If the values do not match, the Blob service fails.
        if_none_match:
            An ETag value. Specify an ETag value for this conditional
            header to write the page only if the blob's ETag value does not
            match the value specified. If the values are identical, the Blob
            service fails.
        '''
        _validate_not_none('container_name', container_name)
        _validate_not_none('blob_name', blob_name)
        _validate_not_none('page', page)
        _validate_not_none('range', range)
        _validate_not_none('page_write', page_write)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = '/' + \
            _str(container_name) + '/' + _str(blob_name) + '?comp=page'
        request.headers = [
            ('x-ms-range', _str_or_none(range)),
            ('Content-MD5', _str_or_none(content_md5)),
            ('x-ms-page-write', _str_or_none(page_write)),
            ('x-ms-lease-id', _str_or_none(lease_id)),
            ('x-ms-if-sequence-number-le',
             _str_or_none(if_sequence_number_lte)),
            ('x-ms-if-sequence-number-lt',
             _str_or_none(if_sequence_number_lt)),
            ('x-ms-if-sequence-number-eq',
             _str_or_none(if_sequence_number_eq)),
            ('If-Modified-Since', _str_or_none(if_modified_since)),
            ('If-Unmodified-Since', _str_or_none(if_unmodified_since)),
            ('If-Match', _str_or_none(if_match)),
            ('If-None-Match', _str_or_none(if_none_match))
        ]
        request.query = [('timeout', _int_or_none(timeout))]
        request.body = _get_request_body_bytes_only('page', page)
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_blob_header(
            request, self.authentication)
        self._perform_request(request)

    def get_page_ranges(
        self, container_name, blob_name, snapshot=None, range=None,
        lease_id=None, if_modified_since=None, if_unmodified_since=None,
        if_match=None, if_none_match=None):
        '''
        Retrieves the page ranges for a blob.

        container_name:
            Name of existing container.
        blob_name:
            Name of existing blob.
        snapshot:
            The snapshot parameter is an opaque DateTime value that,
            when present, specifies the blob snapshot to retrieve information
            from.
        range:
            Specifies the range of bytes to be written as a page.
            Both the start and end of the range must be specified. Must be in
            format:
                bytes=startByte-endByte. Given that pages must be aligned
            with 512-byte boundaries, the start offset must be a modulus of
            512 and the end offset must be a modulus of 512-1. Examples of
            valid byte ranges are 0-511, 512-1023, etc.
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
        request.method = 'GET'
        request.host = self._get_host()
        request.path = '/' + \
            _str(container_name) + '/' + _str(blob_name) + '?comp=pagelist'
        request.headers = [
            ('x-ms-range', _str_or_none(range)),
            ('x-ms-lease-id', _str_or_none(lease_id)),
            ('If-Modified-Since', _str_or_none(if_modified_since)),
            ('If-Unmodified-Since', _str_or_none(if_unmodified_since)),
            ('If-Match', _str_or_none(if_match)),
            ('If-None-Match', _str_or_none(if_none_match)),
        ]
        request.query = [('snapshot', _str_or_none(snapshot))]
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_blob_header(
            request, self.authentication)
        response = self._perform_request(request)

        return _ETreeXmlToObject.parse_simple_list(response, PageList, PageRange, "page_ranges")

    #----Convenience APIs-----------------------------------------------------

    def create_blob_from_path(
        self, container_name, blob_name, file_path, content_encoding=None,
        content_language=None, cache_control=None, content_type=None,
        content_md5=None, metadata=None, lease_id=None,
        sequence_number=None, progress_callback=None, max_connections=1,
        max_retries=5, retry_wait=1.0, if_modified_since=None,
        if_unmodified_since=None, if_match=None, if_none_match=None):
        '''
        Creates a new blob from a file path, or updates the content of an
        existing blob, with automatic chunking and progress notifications.

        container_name:
            Name of existing container.
        blob_name:
            Name of blob to create or update.
        file_path:
            Path of the file to upload as the blob content.
        content_encoding:
            Specifies which content encodings have been applied to
            the blob. This value is returned to the client when the Get Blob
            (REST API) operation is performed on the blob resource. The client
            can use this value when returned to decode the blob content.
        content_language:
            Specifies the natural languages used by this resource.
        cache_control:
            The Blob service stores this value but does not use or
            modify it.
        content_type:
            Set the blob's content type.
        content_md5:
            Set the blob's MD5 hash.
        metadata:
            A dict containing name, value for metadata.
        lease_id:
            Required if the blob has an active lease.
        sequence_number:
            Set for page blobs only. The sequence number is a
            user-controlled value that you can use to track requests. The
            value of the sequence number must be between 0 and 2^63 - 1. The
            default value is 0.
        progress_callback:
            Callback for progress with signature function(current, total) where
            current is the number of bytes transfered so far, and total is the
            size of the blob, or None if the total size is unknown.
        max_connections:
            Maximum number of parallel connections to use when the blob size
            exceeds 64MB.
            Set to 1 to upload the blob chunks sequentially.
            Set to 2 or more to upload the blob chunks in parallel. This uses
            more system resources but will upload faster.
        max_retries:
            Number of times to retry upload of blob chunk if an error occurs.
        retry_wait:
            Sleep time in secs between retries.
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
        _validate_not_none('file_path', file_path)

        count = path.getsize(file_path)
        with open(file_path, 'rb') as stream:
            self.create_blob_from_stream(
                container_name=container_name,
                blob_name=blob_name,
                stream=stream,
                count=count,
                content_encoding=content_encoding,
                content_language=content_language,
                cache_control=cache_control,
                content_type=content_type,
                content_md5=content_md5,
                metadata=metadata,
                lease_id=lease_id,
                sequence_number=sequence_number,
                progress_callback=progress_callback,
                max_connections=max_connections,
                max_retries=max_retries,
                retry_wait=retry_wait,
                if_modified_since=if_modified_since,
                if_unmodified_since=if_unmodified_since,
                if_match=if_match,
                if_none_match=if_none_match)

    def create_blob_from_stream(
        self, container_name, blob_name, stream, count, content_encoding=None,
        content_language=None, cache_control=None, content_type=None,
        content_md5=None, metadata=None, lease_id=None,
        sequence_number=None, progress_callback=None, max_connections=1,
        max_retries=5, retry_wait=1.0, if_modified_since=None,
        if_unmodified_since=None, if_match=None, if_none_match=None):
        '''
        Creates a new blob from a file/stream, or updates the content of an
        existing blob, with automatic chunking and progress notifications.

        container_name:
            Name of existing container.
        blob_name:
            Name of blob to create or update.
        stream:
            Opened file/stream to upload as the blob content.
        count:
            Number of bytes to read from the stream. This is required, a page
            blob cannot be created if the count is unknown.
        content_encoding:
            Specifies which content encodings have been applied to
            the blob. This value is returned to the client when the Get Blob
            (REST API) operation is performed on the blob resource. The client
            can use this value when returned to decode the blob content.
        content_language:
            Specifies the natural languages used by this resource.
        cache_control:
            The Blob service stores this value but does not use or
            modify it.
        content_type:
            Set the blob's content type.
        content_md5:
            Set the blob's MD5 hash.
        metadata:
            A dict containing name, value for metadata.
        lease_id:
            Required if the blob has an active lease.
        sequence_number:
            Set for page blobs only. The sequence number is a
            user-controlled value that you can use to track requests. The
            value of the sequence number must be between 0 and 2^63 - 1. The
            default value is 0.
        progress_callback:
            Callback for progress with signature function(current, total) where
            current is the number of bytes transfered so far, and total is the
            size of the blob, or None if the total size is unknown.
        max_connections:
            Maximum number of parallel connections to use when the blob size
            exceeds 64MB.
            Set to 1 to upload the blob chunks sequentially.
            Set to 2 or more to upload the blob chunks in parallel. This uses
            more system resources but will upload faster.
            Note that parallel upload requires the stream to be seekable.
        max_retries:
            Number of times to retry upload of blob chunk if an error occurs.
        retry_wait:
            Sleep time in secs between retries.
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
        _validate_not_none('stream', stream)
        _validate_not_none('count', count)

        if count < 0:
            raise ValueError(_ERROR_VALUE_NEGATIVE.format('count'))

        if count % _PAGE_SIZE != 0:
            raise ValueError(_ERROR_PAGE_BLOB_SIZE_ALIGNMENT.format(count))

        self.create_blob(
            container_name=container_name,
            blob_name=blob_name,
            content_length=count,
            content_encoding=content_encoding,
            content_language=content_language,
            cache_control=cache_control,
            content_type=content_type,
            content_md5=content_md5,
            metadata=metadata,
            lease_id=lease_id,
            sequence_number=sequence_number,
            if_modified_since=if_modified_since,
            if_unmodified_since=if_unmodified_since,
            if_match=if_match,
            if_none_match=if_none_match
        )

        _upload_blob_chunks(
            blob_service=self,
            container_name=container_name,
            blob_name=blob_name,
            blob_size=count,
            block_size=self._BLOB_MAX_CHUNK_DATA_SIZE,
            stream=stream,
            max_connections=max_connections,
            max_retries=max_retries,
            retry_wait=retry_wait,
            progress_callback=progress_callback,
            lease_id=lease_id,
            uploader_class=_PageBlobChunkUploader,
        )

    def create_blob_from_bytes(
        self, container_name, blob_name, blob, index=0, count=None,
        content_encoding=None, content_language=None, cache_control=None,
        content_type=None, content_md5=None, metadata=None,
        lease_id=None, sequence_number=None, progress_callback=None,
        max_connections=1, max_retries=5, retry_wait=1.0,
        if_modified_since=None, if_unmodified_since=None, if_match=None,
        if_none_match=None):
        '''
        Creates a new blob from an array of bytes, or updates the content
        of an existing blob, with automatic chunking and progress
        notifications.

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
        content_encoding:
            Specifies which content encodings have been applied to
            the blob. This value is returned to the client when the Get Blob
            (REST API) operation is performed on the blob resource. The client
            can use this value when returned to decode the blob content.
        content_language:
            Specifies the natural languages used by this resource.
        cache_control:
            The Blob service stores this value but does not use or
            modify it.
        content_type:
            Set the blob's content type.
        content_md5:
            Set the blob's MD5 hash.
        metadata:
            A dict containing name, value for metadata.
        lease_id:
            Required if the blob has an active lease.
        blob_sequence_number:
            Set for page blobs only. The sequence number is a
            user-controlled value that you can use to track requests. The
            value of the sequence number must be between 0 and 2^63 - 1. The
            default value is 0.
        progress_callback:
            Callback for progress with signature function(current, total) where
            current is the number of bytes transfered so far, and total is the
            size of the blob, or None if the total size is unknown.
        max_connections:
            Maximum number of parallel connections to use when the blob size
            exceeds 64MB.
            Set to 1 to upload the blob chunks sequentially.
            Set to 2 or more to upload the blob chunks in parallel. This uses
            more system resources but will upload faster.
        max_retries:
            Number of times to retry upload of blob chunk if an error occurs.
        retry_wait:
            Sleep time in secs between retries.
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
        _validate_not_none('blob', blob)
        _validate_type_bytes('blob', blob)

        if index < 0:
            raise IndexError(_ERROR_VALUE_NEGATIVE.format('index'))

        if count is None or count < 0:
            count = len(blob) - index

        stream = BytesIO(blob)
        stream.seek(index)

        self.create_blob_from_stream(
            container_name=container_name,
            blob_name=blob_name,
            stream=stream,
            count=count,
            content_encoding=content_encoding,
            content_language=content_language,
            cache_control=cache_control,
            content_type=content_type,
            content_md5=content_md5,
            metadata=metadata,
            lease_id=lease_id,
            sequence_number=sequence_number,
            progress_callback=progress_callback,
            max_connections=max_connections,
            max_retries=max_retries,
            retry_wait=retry_wait,
            if_modified_since=if_modified_since,
            if_unmodified_since=if_unmodified_since,
            if_match=if_match,
            if_none_match=if_none_match)