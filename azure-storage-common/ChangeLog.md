# Change Log azure-storage-common

> See [BreakingChanges](BreakingChanges.md) for a detailed list of API breaks.

## Version XX.XX.XX:

- When unable to sign request, avoid wasting time on retries by failing faster.

## Version 1.3.0:

- Support for 2018-03-28 REST version. Please see our REST API documentation and blog for information about the related added features.

## Version 1.2.0rc1:

- Increased default socket timeout to a more reasonable number for Python 3.5+.
- Fixed bug where seekable streams (request body) were not being reset for retries.

## Version 1.1.0:

- Support for 2017-07-29 REST version. Please see our REST API documentation and blogs for information about the related added features.
- Error message now contains the ErrorCode from the x-ms-error-code header value.

## Version 1.0.0:

- The package has switched from Apache 2.0 to the MIT license.
- Added back the ability to generate account SAS for table service.
- Fixed bug where a question mark prefix on SAS tokens causes failures.
- Fixed the handling of path style host for the Storage Emulator, specifically the location lock and retry to secondary location.
- Renamed the confusing argument name increment_power to increment_base on ExponentialRetry.

## Version 0.37.1:

- Fixed the return type of __add__ and __or__ methods on the AccountPermissions class
- Added the captured exception to retry_context, in case the user wants more info in retry_callback or implement their own retry class.
- Added random jitters to retry intervals, in order to avoid multiple retries to happen at the exact same time

