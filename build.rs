//! Generates the Spark Connect gRPC stubs from the Spark v3.5.1 protobuf files in proto/.
//! protox parses the files in Rust, so the build needs no protoc on the host.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto");
    let files = protox::compile(["spark/connect/base.proto"], ["proto"])?;
    tonic_prost_build::configure()
        // The tests run a fake server that implements only ExecutePlan. Every other
        // method of the generated server trait answers Unimplemented.
        .generate_default_stubs(true)
        .compile_fds(files)?;
    Ok(())
}
