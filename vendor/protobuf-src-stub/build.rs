// No-op build script. The real protobuf-src compiles libprotobuf; we
// rely on a system-installed protoc instead.
fn main() {
    println!("cargo:rerun-if-env-changed=PROTOC");
    println!("cargo:rerun-if-env-changed=PROTOC_INCLUDE");
}
