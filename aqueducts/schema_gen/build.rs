use std::env;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

/// Build script to generate the YAML schema for documentation purposes
fn main() {
    let schema = schemars::schema_for!(aqueducts_core::Aqueduct);

    let schema_json = serde_json::to_string_pretty(&schema).expect("Failed to serialize schema");

    let out_dir = env::var("OUT_DIR").expect("OUT_DIR not set");
    let dest_path = PathBuf::from(out_dir).join("aqueducts.schema.json");
    let mut file = File::create(dest_path).expect("Failed to create schema file");
    file.write_all(schema_json.as_bytes())
        .expect("Failed to write schema file");

    println!("cargo:rerun-if-changed=src/lib.rs");
}
