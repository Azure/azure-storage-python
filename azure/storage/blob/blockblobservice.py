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
)
from .._common_conversion import (
    _encode_base64,
    _str,
    _str_or_none,
)
from .._common_serialization import (
    _get_request_body,
    _get_request_body_bytes_only,
    _update_request_uri_query_local_storage,
)
from .._http import HTTPRequest
from ._chunking import (
    _BlockBlobChunkUploader,
    _upload_blob_chunks,
)
from .models import _BlobTypes
from ..constants import (
    BLOB_SERVICE_HOST_BASE,
    DEFAULT_HTTP_TIMEOUT,
    DEV_BLOB_HOST,
    X_MS_VERSION,
)
from ._serialization import (
    _convert_block_list_to_xml,
    _convert_response_to_block_list,
    _update_storage_blob_header,
)
from ._baseblobservice import _BaseBlobService
from os import path
import sys
if sys.version_info >= (3,):
    from io import BytesIO
else:
    from cStringIO import StringIO as BytesIO


class BlockBlobService(_BaseBlobService):

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
        self.blob_type = _BlobTypes.BlockBlob
        super(BlockBlobService, self).__init__(
            account_name, account_key, protocol, host_base, dev_host,
            timeout, sas_token, connection_string, request_session)

    def _put_blob(self, container_name, blob_name, blob, content_encoding=None,
                  content_language=None, content_md5=None, cache_control=None,
                  content_type=None, metadata=None, lease_id=None,
                  if_modified_since=None, if_unmodified_since=None,
                  if_match=None, if_none_match=None):
        '''
        Creates a blob or updates an existing blob.

        See create_blob_from_* for high level
        functions that handle the creation and upload of large blobs with
        automatic chunking and progress notifications.

        container_name:
            Name of existing container.
        blob_name:
            Name of blob to create or update.
        blob:
            Content of blob as bytes (size < 64MB). For larger size, you
            must call put_block and put_block_list to set content of blob.
        content_encoding:
            Specifies which content encodings have been applied to
            the blob. This value is returned to the client when the Get Blob
            (REST API) operation is performed on the blob resource. The client
            can use this value when returned to decode the blob content.
        content_language:
            Specifies the natural languages used by this resource.
        content_md5:
            An MD5 hash of the blob content. This hash is used to
            verify the integrity of the blob during transport. When this header
            is specified, the storage service checks the hash that has arrived
            with the one that was sent. If the two hashes do not match, the
            operation will fail with error code 400 (Bad Request).
        cache_control:
            The Blob service stores this value but does not use or
            modify it.
        content_type:
            Set the blob's content type.
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
            ('Content-MD5', _str_or_none(content_md5)),
            ('x-ms-blob-content-type', _str_or_none(content_type)),
            ('x-ms-blob-content-encoding',
                _str_or_none(content_encoding)),
            ('x-ms-blob-content-language',
                _str_or_none(content_language)),
            ('x-ms-blob-cache-control', _str_or_none(cache_control)),
            ('x-ms-meta-name-values', metadata),
            ('x-ms-lease-id', _str_or_none(lease_id)),
            ('If-Modified-Since', _str_or_none(if_modified_since)),
            ('If-Unmodified-Since', _str_or_none(if_unmodified_since)),
            ('If-Match', _str_or_none(if_match)),
            ('If-None-Match', _str_or_none(if_none_match))
        ]
        request.body = _get_request_body_bytes_only('blob', blob)
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_blob_header(
            request, self.authentication)
        self._perform_request(request)

    def put_block(self, container_name, blob_name, block, blockid,
                  content_md5=None, lease_id=None):
        '''
        Creates a new block to be committed as part of a blob.

        container_name:
            Name of existing container.
        blob_name:
            Name of existing blob.
        block:
            Content of the block.
        blockid:
            Required. A value that identifies the block. The string must be
            less than or equal to 64 bytes in size.
        content_md5:
            An MD5 hash of the block content. This hash is used to
            verify the integrity of the blob during transport. When this
            header is specified, the storage service checks the hash that has
            arrived with the one that was sent.
        lease_id:
            Required if the blob has an active lease.
        '''
        _validate_not_none('container_name', container_name)
        _validate_not_none('blob_name', blob_name)
        _validate_not_none('block', block)
        _validate_not_none('blockid', blockid)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = '/' + \
            _str(container_name) + '/' + _str(blob_name) + '?comp=block'
        request.headers = [
            ('Content-MD5', _str_or_none(content_md5)),
            ('x-ms-lease-id', _str_or_none(lease_id))
        ]
        request.query = [('blockid', _encode_base64(_str_or_none(blockid)))]
        request.body = _get_request_body_bytes_only('block', block)
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_blob_header(
            request, self.authentication)
        self._perform_request(request)

    def put_block_list(
        self, container_name, blob_name, block_list,
        transactional_content_md5=None, cache_control=None,
        content_type=None, content_encoding=None,
        content_language=None, content_md5=None,
        metadata=None, lease_id=None, if_modified_since=None,
        if_unmodified_since=None, if_match=None, if_none_match=None):
        '''
        Writes a blob by specifying the list of block IDs that make up the
        blob. In order to be written as part of a blob, a block must have been
        successfully written to the server in a prior Put Block (REST API)
        operation.

        container_name:
            Name of existing container.
        blob_name:
            Name of existing blob.
        block_list:
            A str list containing the block ids.
        transactional_content_md5:
            An MD5 hash of the block content. This hash is used to
            verify the integrity of the blob during transport. When this header
            is specified, the storage service checks the hash that has arrived
            with the one that was sent.
        cache_control:
            Sets the blob's cache control. If specified, this
            property is stored with the blob and returned with a read request.
        content_type:
            Sets the blob's content type. If specified, this property
            is stored with the blob and returned with a read request.
        content_encoding:
            Sets the blob's content encoding. If specified, this
            property is stored with the blob and returned with a read request.
        content_language:
            Set the blob's content language. If specified, this
            property is stored with the blob and returned with a read request.
        content_md5:
            An MD5 hash of the blob content. Note that this hash is
            not validated, as the hashes for the individual blocks were
            validated when each was uploaded.
        metadata:
            Dict containing name and value pairs.
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
        _validate_not_none('block_list', block_list)
        request = HTTPRequest()
        request.method = 'PUT'
        request.host = self._get_host()
        request.path = '/' + \
            _str(container_name) + '/' + _str(blob_name) + '?comp=blocklist'
        request.headers = [
            ('Content-MD5', _str_or_none(transactional_content_md5)),
            ('x-ms-blob-cache-control', _str_or_none(cache_control)),
            ('x-ms-blob-content-type', _str_or_none(content_type)),
            ('x-ms-blob-content-encoding',
             _str_or_none(content_encoding)),
            ('x-ms-blob-content-language',
             _str_or_none(content_language)),
            ('x-ms-blob-content-md5', _str_or_none(content_md5)),
            ('x-ms-meta-name-values', metadata),
            ('If-Modified-Since', _str_or_none(if_modified_since)),
            ('If-Unmodified-Since', _str_or_none(if_unmodified_since)),
            ('If-Match', _str_or_none(if_match)),
            ('If-None-Match', _str_or_none(if_none_match)),
            ('x-ms-lease-id', _str_or_none(lease_id))
        ]
        request.body = _get_request_body(
            _convert_block_list_to_xml(block_list))
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_blob_header(
            request, self.authentication)
        self._perform_request(request)

    def get_block_list(self, container_name, blob_name, snapshot=None,
                       block_list_type=None, lease_id=None):
        '''
        Retrieves the list of blocks that have been uploaded as part of a
        block blob.

        container_name:
            Name of existing container.
        blob_name:
            Name of existing blob.
        snapshot:
            Datetime to determine the time to retrieve the blocks.
        block_list_type:
            Specifies whether to return the list of committed blocks, the list
            of uncommitted blocks, or both lists together. Valid values are:
            committed, uncommitted, or all.
        lease_id:
            Required if the blob has an active lease.
        '''
        _validate_not_none('container_name', container_name)
        _validate_not_none('blob_name', blob_name)
        request = HTTPRequest()
        request.method = 'GET'
        request.host = self._get_host()
        request.path = '/' + \
            _str(container_name) + '/' + _str(blob_name) + '?comp=blocklist'
        request.headers = [('x-ms-lease-id', _str_or_none(lease_id))]
        request.query = [
            ('snapshot', _str_or_none(snapshot)),
            ('blocklisttype', _str_or_none(block_list_type))
        ]
        request.path, request.query = _update_request_uri_query_local_storage(
            request, self.use_local_storage)
        request.headers = _update_storage_blob_header(
            request, self.authentication)
        response = self._perform_request(request)

        return _convert_response_to_block_list(response)

    #----Convenience APIs-----------------------------------------------------

    def create_blob_from_path(
        self, container_name, blob_name, file_path,
        content_encoding=None, content_language=None, content_md5=None,
        cache_control=None, content_type=None,
        metadata=None, lease_id=None, progress_callback=None,
        max_connections=1, max_retries=5, retry_wait=1.0,
        if_modified_since=None, if_unmodified_since=None,
        if_match=None, if_none_match=None):
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
        content_md5:
            An MD5 hash of the blob content. This hash is used to
            verify the integrity of the blob during transport. When this header
            is specified, the storage service checks the hash that has arrived
            with the one that was sent. If the two hashes do not match, the
            operation will fail with error code 400 (Bad Request).
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
                content_md5=content_md5,
                cache_control=cache_control,
                content_type=content_type,
                metadata=metadata,
                lease_id=lease_id,
                progress_callback=progress_callback,
                max_connections=max_connections,
                max_retries=max_retries,
                retry_wait=retry_wait,
                if_modified_since=if_modified_since,
                if_unmodified_since=if_unmodified_since,
                if_match=if_match,
                if_none_match=if_none_match)

    def create_blob_from_stream(
        self, container_name, blob_name, stream, count=None,
        content_encoding=None, content_language=None, content_md5=None,
        cache_control=None, content_type=None,
        metadata=None, lease_id=None, progress_callback=None,
        max_connections=1, max_retries=5, retry_wait=1.0,
        if_modified_since=None, if_unmodified_since=None,
        if_match=None, if_none_match=None):
        '''
        Creates a new blob from a file/stream, or updates the content of
        an existing blob, with automatic chunking and progress
        notifications.

        container_name:
            Name of existing container.
        blob_name:
            Name of blob to create or update.
        stream:
            Opened file/stream to upload as the blob content.
        count:
            Number of bytes to read from the stream. This is optional, but
            should be supplied for optimal performance.
        content_encoding:
            Specifies which content encodings have been applied to
            the blob. This value is returned to the client when the Get Blob
            (REST API) operation is performed on the blob resource. The client
            can use this value when returned to decode the blob content.
        content_language:
            Specifies the natural languages used by this resource.
        content_md5:
            An MD5 hash of the blob content. This hash is used to
            verify the integrity of the blob during transport. When this header
            is specified, the storage service checks the hash that has arrived
            with the one that was sent. If the two hashes do not match, the
            operation will fail with error code 400 (Bad Request).
        cache_control:
            The Blob service stores this value but does not use or
            modify it.
        content_type:
            Set the blob's content type.
        metadata:
            A dict containing name, value for metadata.
        lease_id:
            Required if the blob has an active lease.
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

        if count and count < self._BLOB_MAX_DATA_SIZE:
            if progress_callback:
                progress_callback(0, count)

            data = stream.read(count)
            self._put_blob(
                container_name=container_name,
                blob_name=blob_name,
                blob=data,
                content_encoding=content_encoding,
                content_language=content_language,
                content_md5=content_md5,
                cache_control=cache_control,
                content_type=content_type,
                metadata=metadata,
                lease_id=lease_id,
                if_modified_since=if_modified_since,
                if_unmodified_since=if_unmodified_since,
                if_match=if_match,
                if_none_match=if_none_match)

            if progress_callback:
                progress_callback(count, count)
        else:
            self._put_blob(
                container_name=container_name,
                blob_name=blob_name,
                blob=None,
                content_encoding=content_encoding,
                content_language=content_language,
                content_md5=content_md5,
                cache_control=cache_control,
                content_type=content_type,
                metadata=metadata,
                lease_id=lease_id,
                if_modified_since=if_modified_since,
                if_unmodified_since=if_unmodified_since,
                if_match=if_match,
                if_none_match=if_none_match,
            )

            block_ids = _upload_blob_chunks(
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
                uploader_class=_BlockBlobChunkUploader,
            )

            self.put_block_list(
                container_name=container_name,
                blob_name=blob_name,
                block_list=block_ids,
                content_md5=content_md5,
                content_type=content_type,
                content_encoding=content_encoding,
                content_language=content_language,
                cache_control=cache_control,
                metadata=metadata,
                lease_id=lease_id,
                if_modified_since=if_modified_since,
                if_unmodified_since=if_unmodified_since,
                if_match=if_match,
                if_none_match=if_none_match,
            )

    def create_blob_from_bytes(
        self, container_name, blob_name, blob, index=0, count=None,
        content_encoding=None, content_language=None, content_md5=None,
        cache_control=None,content_type=None,
        metadata=None, lease_id=None, progress_callback=None,
        max_connections=1, max_retries=5, retry_wait=1.0,
        if_modified_since=None, if_unmodified_since=None,
        if_match=None, if_none_match=None):
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
        content_md5:
            An MD5 hash of the blob content. This hash is used to
            verify the integrity of the blob during transport. When this header
            is specified, the storage service checks the hash that has arrived
            with the one that was sent. If the two hashes do not match, the
            operation will fail with error code 400 (Bad Request).
        cache_control:
            The Blob service stores this value but does not use or
            modify it.
        content_type:
            Set the blob's content type.
        metadata:
            A dict containing name, value for metadata.
        lease_id:
            Required if the blob has an active lease.
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
        _validate_not_none('index', index)
        _validate_type_bytes('blob', blob)

        if index < 0:
            raise IndexError(_ERROR_VALUE_NEGATIVE.format('index'))

        if count is None or count < 0:
            count = len(blob) - index

        if count < self._BLOB_MAX_DATA_SIZE:
            if progress_callback:
                progress_callback(0, count)

            data = blob[index: index + count]
            self._put_blob(
                container_name=container_name,
                blob_name=blob_name,
                blob=data,
                content_encoding=content_encoding,
                content_language=content_language,
                content_md5=content_md5,
                cache_control=cache_control,
                content_type=content_type,
                metadata=metadata,
                lease_id=lease_id,
                if_modified_since=if_modified_since,
                if_unmodified_since=if_unmodified_since,
                if_match=if_match,
                if_none_match=if_none_match)

            if progress_callback:
                progress_callback(count, count)
        else:
            stream = BytesIO(blob)
            stream.seek(index)

            self.create_blob_from_stream(
                container_name=container_name,
                blob_name=blob_name,
                stream=stream,
                count=count,
                content_encoding=content_encoding,
                content_language=content_language,
                content_md5=content_md5,
                cache_control=cache_control,
                content_type=content_type,
                metadata=metadata,
                lease_id=lease_id,
                progress_callback=progress_callback,
                max_connections=max_connections,
                max_retries=max_retries,
                retry_wait=retry_wait,
                if_modified_since=if_modified_since,
                if_unmodified_since=if_unmodified_since,
                if_match=if_match,
                if_none_match=if_none_match)

    def create_blob_from_text(
        self, container_name, blob_name, text, encoding='utf-8',
        content_encoding=None, content_language=None,
        content_md5=None, cache_control=None, content_type=None,
        metadata=None, lease_id=None, progress_callback=None,
        max_connections=1, max_retries=5, retry_wait=1.0,
        if_modified_since=None, if_unmodified_since=None,
        if_match=None, if_none_match=None):
        '''
        Creates a new blob from str/unicode, or updates the content of an
        existing blob, with automatic chunking and progress notifications.

        container_name:
            Name of existing container.
        blob_name:
            Name of blob to create or update.
        text:
            Text to upload to the blob.
        encoding:
            Python encoding to use to convert the text to bytes.
        content_encoding:
            Specifies which content encodings have been applied to
            the blob. This value is returned to the client when the Get Blob
            (REST API) operation is performed on the blob resource. The client
            can use this value when returned to decode the blob content.
        content_language:
            Specifies the natural languages used by this resource.
        content_md5:
            An MD5 hash of the blob content. This hash is used to
            verify the integrity of the blob during transport. When this header
            is specified, the storage service checks the hash that has arrived
            with the one that was sent. If the two hashes do not match, the
            operation will fail with error code 400 (Bad Request).
        cache_control:
            The Blob service stores this value but does not use or
            modify it.
        content_type:
            Set the blob's content type.
        metadata:
            A dict containing name, value for metadata.
        lease_id:
            Required if the blob has an active lease.
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
        _validate_not_none('text', text)

        if not isinstance(text, bytes):
            _validate_not_none('encoding', encoding)
            text = text.encode(encoding)

        self.create_blob_from_bytes(
            container_name=container_name,
            blob_name=blob_name,
            blob=text,
            index=0,
            count=len(text),
            content_encoding=content_encoding,
            content_language=content_language,
            content_md5=content_md5,
            cache_control=cache_control,
            content_type=content_type,
            metadata=metadata,
            lease_id=lease_id,
            progress_callback=progress_callback,
            max_connections=max_connections,
            max_retries=max_retries,
            retry_wait=retry_wait,
            if_modified_since=if_modified_since,
            if_unmodified_since=if_unmodified_since,
            if_match=if_match,
            if_none_match=if_none_match)