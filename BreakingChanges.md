# Breaking Changes

> See the [Change Log](ChangeLog.md) for a summary of storage library changes.

## Version 0.30.0

### All:
- UserAgent string has changed to conform to the Azure Storage standard.

### Shared Access Signatures (SAS) and ACL
- set and get acl methods take and return dictionaries mapping an id to an AccessPolicy object rather than a SignedIdentifiers object.
- generate_shared_access_signature methods were renamed to indicate the object for which they were creating the sas. For example, generate_shared_access_signature for containers became generate_container_shared_access_signature.
- generate_*_shared_access_signature methods take permission, expiry, start and id directly rather than as part of a SharedAccessPolicy object.
- *SharedAccessPermissions objects were renamed to *Permissions. For example, TableSharedAccessPermissions became TablePermissions.
- SharedAccessSignature was completely redone and contains methods for individual services rather than one method for all. For example, generate_file to create a file shared access token.

### Table:
- Entity insert, update, merge, delete, insert_or_replace and insert_or_merge operations do not take a content_type parameter.
- Entity update, merge, insert_or_replace and insert_or_merge operations do not take partition_key or row_key parameters. These are still required to be part of the entity parameter as before.
- insert_entity returns the entity's etag rather than returning the entire entity.
- Entity update, merge, insert_or_replace and insert_or_merge operations return the etag directly as a string rather than returning a dictionary containing the etag.
- Operations which return entities (get_entity, query_entities) will return Edm.Int64 properties as plain Python ints and Edm.Int32 properties as as EntityProperty objects.
- All table entity integer values are stored on the service with type Edm.Int64 unless the type is explicitly overridden as Edm.Int32.
- Table batches are constructed using the Batch class rather than turning batching on and off via the TableService. The TableService can then execute these batches using commit_batch(table_name, batch). TableService no longer contains begin_batch or cancel_batch methods, and commit_batch works differently and takes different parameters.
- Table sas generation requires start/end pk/rk to be specified as direct parameters to the method rather than as part of an AccessPolicy.

### Blob:
- Separated lease_container and lease_blob into unique methods for each lease action.
- Refactored the blob service into a block blob and page blob service.
- Renamed APIs and params: All x_ms(_blob) prefixes and duplicates headers removed. x_ms_range => byte_range for applicable APIs. maxresults => max_results for applicable APIs. For append blobs and page blobs: put_blob => create_blob. For block blobs put_blob => _put_blob. x_ms_blob_condition_maxsize => maxsize_condition for append blob APIs. x_ms_blob_condition_appendpos => appendpos_condition for append blob APIs. text_encoding => encoding for applicable APIs. put_blob_from* => create_blob_from* for page and block blobs. x_ms_blob_content_md5 => transactional_content_md5 for put_block_list. blocklisttype => block_list_type for get_block_list. blockid => block_id for put_block. get_blob_to_file => get_blob_to_stream. get_blob => _get_blob (use get_blob_to_bytes instead). All get_blob* APIs use boolean value for range_get_content_md5 parameter. All get_blob* APIs take in a start_range and end_range param instead of byte_range.
- Client-side validation added for ranges used in APIs.
- Changed models for better usability. Blob & BlobResult classes have been joined. ContainerEnumResults => list of Container objects. Properties => ContainerProperties. BlobEnumResults => list of Blob objects. BlobBlock objects are used for specifying information for blocks passed to put_block_list. PageList => list of PageRange objects. get_blob_properties returns a Blob object with the metadata and properties variables filled in. All get_blob_to_* APIs now return a Blob object.
- ContentSettings objects have replaced all content_* and cache_control params for applicable APIs. Create a ContentSettings object passing with those params and pass it to APIs instead.
- list_blobs no longer exposes prefix, marker, max_results, or delimiter.
- Single-threaded blob download APIs will now download the blob without chunking to improve perf.
- get_blob_to_* progress_callback may receive None for its total parameter when parallelism is off to allow a perf optimization.
- Metadata returned for blobs and containers will be returned without the 'x-ms-meta' prefix on the keys. Namely, metadata will be returned as it is received.
- get_container_properties and get_blob_properties return parsed Container and Blob objects, respectively, instead of string header dictionaries.
- copy_blob returns a parsed CopyProperties object instead of a string header dictionary.
- acquire and renew lease calls return the lease id, break lease returns the remaining lease time, and change and release lease return nothing instead of string header dictionaries.
- snapshot_blob returns a Blob object with the name, snapshot, etag and LMT properties populated instead of a string header dictionary.
- PageBlob put_page API is split into update_page and clear_page instead of being parsed a flag to indicate the behavior.
- An error is thrown immediately if parallel operations are attempted with a non-seekable stream rather than being thrown later.

### Queue:
- The list_queues operation returns a sequence of Queue objects. The sequence returned has a single attribute, next_marker. Queue objects contain a name and metadata element. The metadata is returned as a dictionary rather than an object.
- The peek_messages and get_messages operations return a list of QueueMessage objects. QueueMessage objects contain the same fields as previously, but insertion_time, expiration_time, and time_next_visible are returned as UTC dates rather than strings.
- Renamed params: maxresults => max_results for list_queues.
- update_message takes message_text as an optional parameter. This changes the parameter ordering.
- Encoding and decoding functions default to xml encoding and decoding. Previously messages were only xml encoded but not decoded.
- Metadata returned for queues will be returned without the 'x-ms-meta' prefix on the keys. Namely, metadata will be returned as it is received.
- get_queue_metadata returns a metadata dict with an approximate_message_count property as an int.
- update_message returns a QueueMessage object with pop receipt and time next visible (parsed as a date) populated rather than a header dictionary.

### File:
- Renamed APIs and params: All x_ms prefixes have been removed. x_ms_range => byte_range for applicable APIs. get_file => _get_file (use get_file_to_bytes instead). maxresults => max_results for applicable APIs. x_ms_meta_name_values => metadata for applicable APIs. text_encoding => encoding for applicable APIs. get_file is now internal (_get_file) since the get_file_to_* APIs make get_file somewhat redundant. All get_file* APIs use boolean value for range_get_content_md5 parameter. All APIs that took byte_range now take start_range and end_range params.
- Client-side validation added for ranges used in APIs.
- Added sas_token parameter to FileService constructor before the connection_string param. Added quota parameter to create_share before the fail_on_exist param.
- Changed list_ranges to return the list of file ranges directly rather than nested within a RangeList object.
- ContentSettings objects have replaced all content_* and cache_control params for applicable APIs. Create a ContentSettings object passing with those params and pass it to APIs instead.
- Single-threaded file download APIs will now download the file without chunking to improve perf.
- Combined models for File & FileResult for better usability. get_file_properties returns a File object with the metadata and properties variables filled in. All get_file_to_* APIs now return a File object.
- list_directories_and_files no longer exposes marker or max_results.
- get_file_to_* progress_callback may receive None for its total parameter when parallelism is off to allow a perf optimization.
- Metadata returned for shares, directories, and files will be returned without the 'x-ms-meta' prefix on the keys. Namely, metadata will be returned as it is received.
- get_share_properties, get_directory_properties, and get_file_properties return parsed Share, Directory, and File objects, respectively, instead of string header dictionaries.
- copy_file returns a parsed CopyProperties object instead of a string header dictionary.