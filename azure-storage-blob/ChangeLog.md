# Change Log azure-storage-blob

> See [BreakingChanges](BreakingChanges.md) for a detailed list of API breaks.

## Version XX.XX.XX:

- Optimized page blob upload for create_blob_from_* methods, by skipping the empty chunks.
- Added convenient method to generate container url (make_container_url).

## Version 0.37.1:

- Enabling MD5 validation no longer uses the memory-efficient algorithm for large block blobs, since computing the MD5 hash requires reading the entire block into memory.
- Fixed a bug in the _SubStream class which was at risk of causing data corruption when using the memory-efficient algorithm for large block blobs.
- Support for AccessTierChangeTime to get the last time a tier was modified on an individual blob.