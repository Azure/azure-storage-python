# Change Log

> See [BreakingChanges](https://github.com/Azure/azure-sdk-for-python/tree/dev/azure-storage/BreakingChanges.md) for a detailed list of API breaks.

## Version X.X.X:

### All:
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

### Blob:
- Added snapshot support for the get_blob_properties API.
- Separated lease_container and lease_blob into unique methods for each lease action.
- Added access condition support for all applicable APIs.
- Refactored the blob service into a block blob and page blob service.
- Added Append Blob support.

### Queue:
- The list_queues operation returns a list of Queue objects. The list returned has a single attribute, next_marker. Queue objects contain a name and metadata element. The metadata is returned as a dictionary rather than an object.
- The peek_messages and get_messages operations return a list of QueueMessage objects. QueueMessage objects contain the same fields as previously, but insertion_time, expiration_time, and time_next_visible are returned as UTC dates rather than strings.
