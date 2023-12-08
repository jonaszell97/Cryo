// swift-tools-version: 5.9
// The swift-tools-version declares the minimum version of Swift required to build this package.

import CompilerPluginSupport
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
        .package(url: "https://github.com/apple/swift-syntax", from: "509.0.0")
    ],
    targets: [
        .macro(
            name: "CryoMacros",
            dependencies: [
                .product(name: "SwiftSyntaxMacros", package: "swift-syntax"),
                .product(name: "SwiftCompilerPlugin", package: "swift-syntax")
            ]),
        .target(
            name: "Cryo",
            dependencies: ["CryoMacros"]),
        .testTarget(
            name: "CryoTests",
            dependencies: [
                "Cryo", "CryoMacros",
                .product(name: "SwiftSyntaxMacrosTestSupport", package: "swift-syntax"),
            ]),
    ]
)
