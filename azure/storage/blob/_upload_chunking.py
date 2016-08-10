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
import threading
import sys
from time import sleep
from cryptography.hazmat.primitives.padding import PKCS7
from .._common_conversion import _encode_base64
from .._serialization import(
    url_quote,
    _get_data_bytes_only,
)
from ._encryption import(
    _get_blob_encryptor_and_padder,
)
from azure.common import (
    AzureHttpError,
)
from .models import BlobBlock
if sys.version_info >= (3,):
    from io import BytesIO
else:
    from cStringIO import StringIO as BytesIO

def _upload_blob_chunks(blob_service, container_name, blob_name,
                        blob_size, block_size, stream, max_connections,
                        progress_callback, validate_content, lease_id, uploader_class, 
                        maxsize_condition=None, if_match=None, timeout=None,
                        content_encryption_key=None, initialization_vector=None):

    encryptor, padder = _get_blob_encryptor_and_padder(content_encryption_key, initialization_vector,
                                                       uploader_class is not _PageBlobChunkUploader)

    uploader = uploader_class(
        blob_service,
        container_name,
        blob_name,
        blob_size,
        block_size,
        stream,
        max_connections > 1,
        progress_callback,
        validate_content,
        lease_id,
        timeout,
        encryptor,
        padder
    )

    uploader.maxsize_condition = maxsize_condition

    # ETag matching does not work with parallelism as a ranged upload may start 
    # before the previous finishes and provides an etag
    uploader.if_match = if_match if not max_connections > 1 else None

    if progress_callback is not None:
        progress_callback(0, blob_size)

    if max_connections > 1:
        import concurrent.futures
        executor = concurrent.futures.ThreadPoolExecutor(max_connections)
        range_ids = list(executor.map(uploader.process_chunk, uploader.get_chunk_streams()))
    else:
        range_ids = [uploader.process_chunk(result) for result in uploader.get_chunk_streams()]

    return range_ids

class _BlobChunkUploader(object):
    def __init__(self, blob_service, container_name, blob_name, blob_size,
                 chunk_size, stream, parallel, progress_callback, 
                 validate_content, lease_id, timeout, encryptor, padder):
        self.blob_service = blob_service
        self.container_name = container_name
        self.blob_name = blob_name
        self.blob_size = blob_size
        self.chunk_size = chunk_size
        self.stream = stream
        self.parallel = parallel
        self.stream_start = stream.tell() if parallel else None
        self.stream_lock = threading.Lock() if parallel else None
        self.progress_callback = progress_callback
        self.progress_total = 0
        self.progress_lock = threading.Lock() if parallel else None
        self.validate_content = validate_content
        self.lease_id = lease_id
        self.timeout = timeout
        self.encryptor = encryptor
        self.padder = padder

    def get_chunk_streams(self):
        index = 0
        while True:
            data = b''
            read_size = self.chunk_size

            # Buffer until we either reach the end of the stream or get a whole chunk.
            while True:
                if self.blob_size:
                    read_size = min(self.chunk_size-len(data), self.blob_size - (index + len(data)))
                temp = self.stream.read(read_size)
                temp = _get_data_bytes_only('temp', temp)
                data += temp

                # We have read an empty string and so are at the end
                # of the buffer or we have read a full chunk.
                if temp == b'' or len(data) == self.chunk_size:
                    break

            if len(data) == self.chunk_size:
                if self.padder:
                    data = self.padder.update(data)
                if self.encryptor:
                    data = self.encryptor.update(data)
                yield index, BytesIO(data)
            else:
                if self.padder:
                    data = self.padder.update(data) + self.padder.finalize()
                if self.encryptor:
                    data = self.encryptor.update(data) + self.encryptor.finalize()
                if len(data) > 0:
                    yield index, BytesIO(data)
                break
            
            index += len(data)

    def process_chunk(self, chunk_data):
        chunk_bytes = chunk_data[1].read()
        chunk_offset = chunk_data[0]
        return self._upload_chunk_with_progress(chunk_offset, chunk_bytes)

    def _update_progress(self, length):
        if self.progress_callback is not None:
            if self.progress_lock is not None:
                with self.progress_lock:
                    self.progress_total += length
                    total = self.progress_total
            else:
                self.progress_total += length
                total = self.progress_total
            self.progress_callback(total, self.blob_size)

    def _upload_chunk_with_progress(self, chunk_offset, chunk_data):
        range_id = self._upload_chunk(chunk_offset, chunk_data) 
        self._update_progress(len(chunk_data))
        return range_id


class _BlockBlobChunkUploader(_BlobChunkUploader):
    def _upload_chunk(self, chunk_offset, chunk_data):
        block_id=url_quote(_encode_base64('{0:032d}'.format(chunk_offset)))
        self.blob_service._put_block(
            self.container_name,
            self.blob_name,
            chunk_data,
            block_id,
            validate_content=self.validate_content,
            lease_id=self.lease_id,
            timeout=self.timeout,
        )
        return BlobBlock(block_id)


class _PageBlobChunkUploader(_BlobChunkUploader):
    def _upload_chunk(self, chunk_start, chunk_data):
        chunk_end = chunk_start + len(chunk_data) - 1
        resp = self.blob_service._update_page(
            self.container_name,
            self.blob_name,
            chunk_data,
            chunk_start,
            chunk_end,
            validate_content=self.validate_content,
            lease_id=self.lease_id,
            if_match=self.if_match,
            timeout=self.timeout,
        )

        if not self.parallel:
            self.if_match = resp.etag

class _AppendBlobChunkUploader(_BlobChunkUploader):
    def _upload_chunk(self, chunk_offset, chunk_data):
        if not hasattr(self, 'current_length'):
            resp = self.blob_service.append_block(
                self.container_name,
                self.blob_name,
                chunk_data,
                validate_content=self.validate_content,
                lease_id=self.lease_id,
                maxsize_condition=self.maxsize_condition,
                timeout=self.timeout,
            )

            self.current_length = resp.append_offset
        else:
            resp = self.blob_service.append_block(
                self.container_name,
                self.blob_name,
                chunk_data,
                validate_content=self.validate_content,
                lease_id=self.lease_id,
                maxsize_condition=self.maxsize_condition,
                appendpos_condition=self.current_length + chunk_offset,
                timeout=self.timeout,
            )