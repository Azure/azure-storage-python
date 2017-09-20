# Change Log azure-storage-blob

> See [BreakingChanges](BreakingChanges.md) for a detailed list of API breaks.

## Version XX.XX.XX:
- Added support for soft delete feature. If a delete retention policy is enabled through the set service properties API, then blobs or snapshots could be deleted softly and retained for a specified number of days, before being permanently removed by garbage collection.

## Version 1.0.0:

- The package has switched from Apache 2.0 to the MIT license.
- Fixed bug where get_blob_to_* cannot get a single byte when start_range and end_range are both equal to 0. 
- Optimized page blob upload for create_blob_from_* methods, by skipping the empty chunks.
- Added convenient method to generate container url (make_container_url).
- Metadata keys are now case-preserving when fetched from the service. Previously they were made lower-case by the library.

## Version 0.37.1:

- Enabling MD5 validation no longer uses the memory-efficient algorithm for large block blobs, since computing the MD5 hash requires reading the entire block into memory.
- Fixed a bug in the _SubStream class which was at risk of causing data corruption when using the memory-efficient algorithm for large block blobs.
- Support for AccessTierChangeTime to get the last time a tier was modified on an individual blob.
