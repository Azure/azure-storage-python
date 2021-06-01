// swift-tools-version:5.1

import PackageDescription

let package = Package(
    name: "azure-storage-python",
    products: [
        .library(
            name: "azure-storage-python",
            targets: ["azure-storage-python"]
        )
    ],
    targets: [
        .target(
            name: "azure-storage-python",
            path: "azure-storage-python"
        ),
        .testTarget(
            name: "azure-storage-python-Tests",
            dependencies: ["azure-storage-python"]
        ),
    ]
)
