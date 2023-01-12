// swift-tools-version: 5.7
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "Cryo",
    platforms: [
        .iOS(.v15),
        .macOS(.v12),
    ],
    products: [
        .library(
            name: "Cryo",
            targets: ["Cryo"]),
    ],
    dependencies: [
    ],
    targets: [
        .target(
            name: "Cryo",
            dependencies: []),
        .testTarget(
            name: "CryoTests",
            dependencies: ["Cryo"]),
    ]
)
