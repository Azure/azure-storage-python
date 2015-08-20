# Breaking Changes

> See the [Change Log](https://github.com/Azure/azure-sdk-for-python/tree/dev/azure-storage/ChangeLog.md) for a summary of storage library changes.

## Version X.X.X

### Table:
- Entity insert, update, merge, delete, insert_or_replace and insert_or_merge operations do not take a content_type parameter.
- Entity update, merge, insert_or_replace and insert_or_merge operations do not take partition_key or row_key parameters. These are still required to be part of the entity parameter as before.
- insert_entity returns the entity's etag rather than returning the entire entity.
- Entity update, merge, insert_or_replace and insert_or_merge operations return the etag directly as a string rather than returning a dictionary containing the etag.
- Operations which return entities (get_entity, query_entities) will return Edm.Int64 properties as EntityProperty objects rather than as ints.
