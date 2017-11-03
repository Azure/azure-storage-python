# Change Log azure-storage-common

> See [BreakingChanges](BreakingChanges.md) for a detailed list of API breaks.

## Version 0.37.1:
- Fixed the return type of __add__ and __or__ methods on the AccountPermissions class
- Added the captured exception to retry_context, in case the user wants more info in retry_callback or implement their own retry class.
- Added random jitters to retry intervals, in order to avoid multiple retries to happen at the exact same time

