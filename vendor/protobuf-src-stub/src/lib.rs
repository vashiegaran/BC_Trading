//! No-op stub replacing the real `protobuf-src` crate.
//!
//! The real crate compiles libprotobuf from C source via autotools, which
//! is hostile on Windows. We rely on a locally-installed `protoc` (via
//! winget/apt) pointed at by the `PROTOC` env var, and never need the
//! bundled build.
//!
//! The API surface below mirrors what `yellowstone-grpc-proto`'s build.rs
//! invokes. It just returns paths pointing at `PROTOC`.

use std::path::PathBuf;

pub fn protoc() -> PathBuf {
    std::env::var_os("PROTOC")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("protoc"))
}

pub fn include() -> PathBuf {
    // Callers that include the bundled protobuf .proto files can set
    // PROTOC_INCLUDE to their own include dir. Falls back to a bogus
    // path — the build fails loudly instead of silently shipping broken
    // protos.
    std::env::var_os("PROTOC_INCLUDE")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(""))
}
