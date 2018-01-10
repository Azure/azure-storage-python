# Change Log azure-storage-file

> See [BreakingChanges](BreakingChanges.md) for a detailed list of API breaks.

## Version XX.XX.XX:

- The package has switched from Apache 2.0 to the MIT license.
- Fixed bug where get_file_to_* cannot get a single byte when start_range and end_range are both equal to 0.
- Metadata keys are now case-preserving when fetched from the service. Previously they were made lower-case by the library.
