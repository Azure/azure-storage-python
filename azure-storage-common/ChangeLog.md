# Change Log azure-storage-common

> See [BreakingChanges](BreakingChanges.md) for a detailed list of API breaks.

## Version XX.XX.XX:
- Fixed the return type of __add__ and __or__ methods on the AccountPermissions class
- Added the captured exception to retry_context, in case the user wants more info in retry_callback or implement their own retry class.