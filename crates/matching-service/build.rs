fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("proto");
    tonic_build::configure()
        .build_server(true)
        .compile(
            &[proto_root.join("matching/admin.proto")],
            &[proto_root],
        )?;
    Ok(())
}
