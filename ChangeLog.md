# Change Log

> See [BreakingChanges](https://github.com/Azure/azure-sdk-for-python/tree/dev/azure-storage/BreakingChanges.md) for a detailed list of API breaks.

## Version X.X.X:

- Simplified tableservice *_entity functions by removing partition_key, row_key, and content_type parameters where possible.
- tableservice *_entity functions that returned dictionaries instead return the etag.
- tableservice insert_entity and create_table operations no longer echo content from the service, improving performance.
- tableservice uses json instead of atompub, improving performance. 
- Accept type can be specified for the tableservice get_entity and query_entities functions. Minimal metadata is the default. No metadata can be used to reduce the payload size but will not return the Edm type for the entity properties. For inferable property types like string, boolean, int32 and double the return values will be the same. For binary, guid, int64 and datetime values simple strings will be returned. A property resolver delegate can be specified if you would like to specify the Edm type manually for these entity properties. The library will use the Edm type returned by the delegate to cast the entity property value appropriately before adding it to the returned entity dictionary.
- All table entity integer values are stored on the service with type Edm.Int64 unless the type is explicitly overridden as Edm.Int32.
- Table Entity class extends dict but also allows property access as if it were an object to allow more flexible usage.