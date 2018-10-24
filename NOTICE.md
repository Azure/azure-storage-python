## NOTICE

Please note that while the Storage Python SDKs (`azure-storage-blob`, `azure-storage-queue`, `azure-storage-file`, and `azure-storage-common`)
are licensed under the MIT license, the SDKs have dependencies that use other types of licenses.

Since Microsoft does not modify nor distribute these dependencies, it is the sole responsibility of the user to determine the correct/compliant usage of these dependencies. Please refer to the 
[setup.py](./azure-storage-common/setup.py#L73) for a list of the **direct** dependencies.

Please also note that the SDKs depend on the `requests` package, which has a dependency `chardet` that uses LGPL license.
 