# Breaking Changes

> See the [Change Log](https://github.com/Azure/azure-sdk-for-python/tree/dev/azure-storage/ChangeLog.md) for a summary of storage library changes.

## Version X.X.X

### Table:
- Entity insert, update, merge, delete, insert_or_replace and insert_or_merge operations do not take a content_type parameter.
- Entity update, merge, insert_or_replace and insert_or_merge operations do not take partition_key or row_key parameters. These are still required to be part of the entity parameter as before.
- insert_entity returns the entity's etag rather than returning the entire entity.
- Entity update, merge, insert_or_replace and insert_or_merge operations return the etag directly as a string rather than returning a dictionary containing the etag.
- Operations which return entities (get_entity, query_entities) will return Edm.Int64 properties as plain Python ints and Edm.Int32 properties as as EntityProperty objects.
- All table entity integer values are stored on the service with type Edm.Int64 unless the type is explicitly overridden as Edm.Int32.
- Table batches are constructed using the Batch class rather than turning batching on and off via the TableService. The TableService can then execute these batches using commit_batch(table_name, batch). TableService no longer contains begin_batch or cancel_batch methods, and commit_batch works differently and takes different parameters.

### Blob:
- Separated lease_container and lease_blob into unique methods for each lease action.