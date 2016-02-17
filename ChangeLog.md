# Change Log

> See [BreakingChanges](BreakingChanges.md) for a detailed list of API breaks.

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