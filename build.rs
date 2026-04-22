fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Only compile protobuf definitions when the `cloud` feature is enabled.
    // The check is done at build time via CARGO_FEATURE_CLOUD env var.
    if std::env::var("CARGO_FEATURE_CLOUD").is_ok() {
        tonic_build::configure()
            .build_server(true)
            .build_client(true)
            .compile_protos(&["proto/sync.proto"], &["proto"])?;
    }
    Ok(())
}
