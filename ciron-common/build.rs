fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all("src/generated")?;

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir("src/generated")
        .compile_protos(
            &["../proto/ciron.proto", "../proto/errors.proto"],
            &["../proto"],
        )?;

    Ok(())
}
