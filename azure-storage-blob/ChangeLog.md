# Change Log azure-storage-blob

> See [BreakingChanges](BreakingChanges.md) for a detailed list of API breaks.

## Version 0.37.1:

- Enabling MD5 validation no longer uses the memory-efficient algorithm for large block blobs, since computing the MD5 hash requires reading the entire block into memory.
- Fixed a bug in the _SubStream class which was at risk of causing data corruption when using the memory-efficient algorithm for large block blobs.
- Support for AccessTierChangeTime to get the last time a tier was modified on an individual blob.