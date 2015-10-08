# Breaking Changes

> See the [Change Log](https://github.com/Azure/azure-sdk-for-python/tree/dev/azure-storage/ChangeLog.md) for a summary of storage library changes.

## Version X.X.X

### All:
- set and get acl methods take and return dictionaries mapping an id to an AccessPolicy object rather than a SignedIdentifiers object.
- generate_shared_access_signature methods take permissions, expiry, start and id directly rather than as part of a SharedAccessPolicy object.
- generate_signed_query_string on SharedAccessSignature takes permissions, expiry, start and id directly rather than as part of a SharedAccessPolicy object.

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

### Queue:
- The list_queues operation returns a sequence of Queue objects. The sequence returned has a single attribute, next_marker. Queue objects contain a name and metadata element. The metadata is returned as a dictionary rather than an object.
- The peek_messages and get_messages operations return a list of QueueMessage objects. QueueMessage objects contain the same fields as previously, but insertion_time, expiration_time, and time_next_visible are returned as UTC dates rather than strings.
