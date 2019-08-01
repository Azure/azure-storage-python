# Change Log azure-storage-queue

> See [BreakingChanges](BreakingChanges.md) for a detailed list of API breaks.

## Version 2.1.0:

- Support for 2019-02-02 REST version. No new features for Queue.

## Version 2.0.1:
- Updated dependency on azure-storage-common.

## Version 2.0.0:
- Support for 2018-11-09 REST version.

## Version 1.4.0:

- azure-storage-nspkg is not installed anymore on Python 3 (PEP420-based namespace package)

## Version 1.3.0:

- Support for 2018-03-28 REST version. Please see our REST API documentation and blog for information about the related added features.

## Version 1.2.0rc1:

- Support for 2017-11-09 REST version. Please see our REST API documentation and blog for information about the related added features.
- Added support for OAuth authentication for HTTPS requests(Please note that this feature is available in preview).

## Version 1.1.0:

- Support for 2017-07-29 REST version. Please see our REST API documentation and blogs for information about the related added features.
- Queue messages can now have an arbitrarily large or infinite time to live.
- Error message now contains the ErrorCode from the x-ms-error-code header value.

## Version 1.0.0:

- The package has switched from Apache 2.0 to the MIT license.
