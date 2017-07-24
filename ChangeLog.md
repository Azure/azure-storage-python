# Change Log

> See [BreakingChanges](BreakingChanges.md) for a detailed list of API breaks.

**Note: This changelog is deprecated starting with version 0.36.0, please refer to the ChangeLog.md in each package for future change logs.** 

## Version 0.36.0:
### All:
- The library has been split into 4 different packages:
    - azure-storage-blob
    - azure-storage-file
    - azure-storage-queue
    - azure-storage-table
- The package `azure-storage` is now deprecated.
- The classes that were directly under azure.storage, not under azure.storage.*(blob, file, queue, table), are now under azure.storage.common.
    - Example: azure.storage.retry becomes azure.storage.common.retry

## Version 0.35.1:

### Blob:
- Fixed bug where calling create_from_* and and append_blob_from_* methods with no data fails.

## Version 0.35.0:

### All:
- Support for 2017-04-17 REST version. Please see our REST API documentation and blogs for information about the related added features. If you are using the Storage Emulator, please update to Emulator version 5.2.
- Fixed a bug where deserialization of service stats throws a TypeError when the service is unavailable.

### Blob:
- For Premium Accounts only, added support for getting and setting the tier on a page blob. The tier can also be set when creating or copying from an existing page blob.
- create_from_* and and append_blob_from_* methods will return response_properties which contains the etag and last modified time.

### Table:
- Fixed syntax error in _convert_json_response_to_entities.
- Fixed a bug where the urls are not correctly formed when making commit_batch to the emulator.

### File:
- The `server_encrypted` file property will now be populated when calling 'get_directory_properties', 'get_file', and 'get_file_properties'. This value is set to True if the file data (for files) and application metadata are completely encrypted.

## Version 0.34.3:
- All: Made the socket timeout configurable. Increased the default socket timeout to 20 seconds.
- All: Fixed a bug where SAS tokens were being duplicated on retries

## Version 0.34.2:

### All:
- Updated the azure namespace packaging system.

## Version 0.34.1:

### Blob:
- Fixed a bug where downloading the snapshot of a blob will fail in some cases.

## Version 0.34.0:

### All:
- All: Support for 2016-05-31 REST version. Please see our REST API documentation and blogs for information about the related added features. If you are using the Storage Emulator, please update to Emulator version 4.6
- All: Several error messages have been clarified or made more specific.

### Blob:
- Added support for server-side encryption headers.
- Properly return connections to pool when checking for non-existent blobs.
- Fixed a bug with parallel uploads for PageBlobs and BlockBlobs where chunks were being buffered and queued faster than can be processed, potentially causing out-of-memory issues.
- Added large block blob upload support. Blocks can now support sizes up to 100 MB and thus the maximum size of a BlockBlob is now 5,000,000 MB (~4.75 TB).
- Added streaming upload support for the put_block method and a new memory optimized upload algorithm for create_blob_from_stream and create_blob_from_file APIs. (BlockBlobService)
- The new upload strategy will no longer fully buffer seekable streams unless Encryption is enabled. See 'use_byte_buffer' parameter documentation on the 'create_blob_from_stream' method for more details.
- Fixed a deserialization bug with get_block_list() where calling it with anything but the 'all' block_list_type would cause an error.
- Using If-None-Match: * will now fail when reading a blob. Previously this header was ignored for blob reads.
- Populate public access when listing blob containers.
- The public access setting on a blob container is now a container property returned from downloadProperties.
- Populate content MD5 for range gets on blobs.
- Added support for incremental copy on page blobs. The source must be a snapshot of a page blob and include a SAS token.

### File:
- Prefix support for listing files and directories.
- Populate content MD5 for range gets on files.

### Queue:
-  put_message now returns a QueueMessage with the PopReceipt, Id, NextVisibleTime, InsertionTime, and ExpirationTime properties populated along with the content.

## Version 0.33.0:

### All:
- Remove with_filter from service client in favor of the newer callback functions.
- Fixed a bug where empty signed identifiers could not be parsed.
- Improved the error message returned when too many signed identifers are provided.
- Added support for automatic retries. A retry function taking a RetryContext object and returning a retry wait time (or None for no retry) may be set on the service client. The default retry has an exponential back-off and is defined in the retry class.
- Added support for reading from secondary. Note that this only applies for RA-GRS accounts. If the client location_mode is set to LocationMode.SECONDARY, read requests which may be sent to secondary will be.

### Blob:
- Client-side encryption. Allows a user to encrypt entire blobs (not individual blocks) before uploading them by providing an encryption policy. See ~samples.blob.encryption_usage.py for samples.

### Table:
- Fixed a bug with Table Entity where EDM bound checks would not allow for full resolution of 32/64-bit values.
- Client-side encryption. Allows a user to encrypt specified properties on an entity before uploading them by providing an encryption policy. See ~samples.table.encryption_usage.py for samples.

### Queue:
- Client-side encryption. Allows a user to encrypt queue messages before uploading them by specifying fields on the queueservice. See ~samples.queue.encryption_usuage.py for samples.

## Version 0.32.0:

### All:
- request_callback and response_callback functions may be set on the service clients. These callbacks will be run before the request is executed and after the response is received, respectively. They maybe used to add custom headers to the request and for logging, among other purposes.
- A client request id is added to requests by default.

### Blob:
- Get requests taking the start_range parameter incorrectly sent an x-ms-range header when start_range was not specified.
- get_blob_to_* will do an initial get request of size 32 MB. If it then finds the blob is larger than this size, it will parallelize by default.
- Block blob and page blob create_blob_from_* methods will parallelize by default.
- The validate_content option on get_blob_to_* and on methods which put blob data will compute and validate an md5 hash of the content if set to True. This is primarily valuable for detecting bitflips on the wire if using http instead of https as https (the default) will already validate.
- Fixed a bug where lease_id was not specified if given by the user for each chunk on parallel get requests.

### File:
- Get requests taking the start_range parameter incorrectly sent an x-ms-range header when start_range was not specified.
- get_file_to_* will do an initial get request of size 32 MB. If it then finds the file is larger than this size, it will parallelize by default.
- create_file_from_* methods will parallelize by default.
- The validate_content option on get_file_to_* and create_file_from_* will compute and validate an md5 hash of the content if set to True. This is primarily valuable for detecting bitflips on the wire if using http instead of https as https (the default) will already validate.

## Version 0.31.0:

### All:
- Support for 2015-07-08 REST version. Please see our REST API documentation and blogs for information about the related added features.
- ListGenerator extends Iterable
- Added get_*_service_stats APIs to retrieve statistics related to replication for read-access geo-redundant storage accounts.
- Fixed a bug where custom endpoints with a trailing slash were not handled correctly.

### Blob:
- Diffing support has been added to the get_page_range API which facilitates finding different page ranges between a previous snapshot and newer snapshot (or current Page Blob).

### Table:
- Fixed a bug in table SAS generation where table names with capital letters were not signed correctly.
- Fixed a bug where list_tables did not parse continuation tokens correctly.

### Queue:
- QueueMessage dequeue_count was documented as and intended to be an int but was instead returned as a string. Changed it to be an int.

## Version 0.30.0:

### All:
- Support for 2015-04-05 REST version. Please see our REST API documentation and blogs for information about the related added features.
- UserAgent string has changed to conform to the Azure Storage standard.
- Added optional timeout parameter to all APIs.
- Empty headers are signed.
- Exceptions produced after request construction and before request parsing (ie, connection or HTTP exceptions) are always wrapped as type AzureException.

### Shared Access Signatures (SAS) and ACL
- Added support for Account SAS. See CloudStorageAccount.generateSharedAccessSignature and the generate_account_shared_access_signature methods on each service.
- Added support for protocol (HTTP/HTTPS) and IP restrictions on the SAS token.
- Created instantiable objects for the shared access Permissions classes to simplify specifying more than one permission.
- set and get acl methods take and return dictionaries mapping an id to an AccessPolicy object rather than a SignedIdentifiers object.
- generate_shared_access_signature methods take permission, expiry, start and id directly rather than as part of a SharedAccessPolicy object.
- generate_signed_query_string on SharedAccessSignature takes permission, expiry, start and id directly rather than as part of a SharedAccessPolicy object.
- expiry and start, whether as part of AccessPolicy or params in generateSharedAccessSignature, may be given as UTC date objects or as strings.

### Table:
- Simplified tableservice *_entity functions by removing partition_key, row_key, and content_type parameters where possible.
- tableservice *_entity functions that returned dictionaries instead return the etag.
- tableservice insert_entity and create_table operations no longer echo content from the service, improving performance.
- tableservice uses json instead of atompub, improving performance. 
- Accept type can be specified for the tableservice get_entity and query_entities functions. Minimal metadata is the default. No metadata can be used to reduce the payload size but will not return the Edm type for the entity properties. For inferable property types like string, boolean, int32 and double the return values will be the same. For binary, guid, int64 and datetime values simple strings will be returned. A property resolver delegate can be specified if you would like to specify the Edm type manually for these entity properties. The library will use the Edm type returned by the delegate to cast the entity property value appropriately before adding it to the returned entity dictionary.
- All table entity integer values are stored on the service with type Edm.Int64 unless the type is explicitly overridden as Edm.Int32.
- Table Entity class extends dict but also allows property access as if it were an object to allow more flexible usage.
- Table batches are constructed using the Batch class rather than turning batching on and off via the TableService. The TableService can then execute these batches using commit_batch(table_name, batch).
- Table sas generation requires start/end pk/rk to be specified as direct parameters to the method rather than as part of an AccessPolicy.
- Added exists method to check table existence.

### Blob:
- Added snapshot support for the get_blob_properties API.
- Separated lease_container and lease_blob into unique methods for each lease action.
- Added access condition support for all applicable APIs.
- Refactored the blob service into a block blob and page blob service.
- Added Append Blob support.
- Renamed some APIs and parameters for better readablity and less redundancy.
- Changed models for better usability.
- ContentSettings objects have replaced all content_* and cache_control params for applicable APIs. Create a ContentSettings object passing with those params and pass it to APIs instead.
- list_blobs no longer exposes prefix, marker, max_results, or delimiter.
- resize and set_sequence_number APIs have been added for Page Blob. It is not possible to make these changes with set_blob_properties.
- Single-threaded blob download APIs will now download the blob without chunking to improve perf.
- Allow '?' as part of blob names.
- get_blob_to_* progress_callback may receive None for its total parameter when parallelism is off to allow a perf optimization.
- Added exists method to check container or blob existence.
- Client-side validation added for ranges used in APIs.
- Metadata returned for blobs and containers will be returned without the 'x-ms-meta' prefix on the keys. Namely, metadata will be returned as it is received.
- get_container_properties and get_blob_properties return parsed Container and Blob objects, respectively, instead of string header dictionaries.
- copy_blob returns a parsed CopyProperties object instead of a string header dictionary.
- acquire and renew lease calls return the lease id, break lease returns the remaining lease time, and change and release lease return nothing instead of string header dictionaries.
- snapshot_blob returns a Blob object with the name, snapshot, etag and LMT properties populated instead of a string header dictionary.
- PageBlob put_page API is split into update_page and clear_page instead of being parsed a flag to indicate the behavior.
- An error is thrown immediately if parallel operations are attempted with a non-seekable stream rather than being thrown later.
- get_container_acl returns a public_access property attached to the returned ACL dictionary.
- Blob uploads which fail no longer commit an empty blob.

### Queue:
- The list_queues operation returns a list of Queue objects. The list returned has a single attribute, next_marker. Queue objects contain a name and metadata element. The metadata is returned as a dictionary rather than an object.
- The peek_messages and get_messages operations return a list of QueueMessage objects. QueueMessage objects contain the same fields as previously, but insertion_time, expiration_time, and time_next_visible are returned as UTC dates rather than strings.
- update_message takes message_text as an optional parameter. This changes the parameter ordering.
- create_queue and set_queue_metadata apis take metadata rather than x_ms_meta_name_values.
- Added encode_function and decode_function properties to the queue service to allow users to specify custom encoding and decoding of queue messages.
- Encoding and decoding functions default to xml encoding and decoding. Previously messages were only xml encoded but not decoded.
- Added exists method to check queue existence.
- Metadata returned for queues will be returned without the 'x-ms-meta' prefix on the keys. Namely, metadata will be returned as it is received.
- get_queue_metadata returns a metadata dict with an approximate_message_count property as an int.
- update_message returns a QueueMessage object with pop receipt and time next visible (parsed as a date) populated rather than a header dictionary.

### File:
- Renamed some APIs and parameters for better readablity and less redundancy.
- Added new file features including support for SAS and ACL, share usage stats, directory metadata, async server side file copy, and share quota.
- ContentSettings objects have replaced all content_* and cache_control params for applicable APIs. Create a ContentSettings object passing with those params and pass it to APIs instead.
- Single-threaded file download APIs will now download the file without chunking to improve perf.
- Combined models for File & FileResult for better usability. get_file_properties double returns FileProperties object and a metadata dict.
- list_directories_and_files no longer exposes marker or max_results.
- get_file_to_* progress_callback may receive None for its total parameter when parallelism is off to allow a perf optimization.
- Added exists method to check share, directory, or file existence.
- Client-side validation added for ranges used in APIs.
- Metadata returned for shares, directories, and files will be returned without the 'x-ms-meta' prefix on the keys. Namely, metadata will be returned as it is received.
- get_share_properties, get_directory_properties, and get_file_properties return parsed Share, Directory, and File objects, respectively, instead of string header dictionaries.
- copy_file returns a parsed CopyProperties object instead of a string header dictionary.
